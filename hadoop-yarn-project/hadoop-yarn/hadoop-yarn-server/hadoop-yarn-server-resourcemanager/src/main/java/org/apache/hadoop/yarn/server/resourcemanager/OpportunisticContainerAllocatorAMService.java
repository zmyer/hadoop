/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB;
import org.apache.hadoop.yarn.api.impl.pb.service.ApplicationMasterProtocolPBServiceImpl;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.ApplicationMasterProtocol.ApplicationMasterProtocolService;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.DistributedSchedulingAMProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterDistributedSchedulingAMResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed.NodeQueueLoadMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed.QueueLimitCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerAllocator;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerContext;
import org.apache.hadoop.yarn.server.utils.YarnServerSecurityUtils;

/**
 * The OpportunisticContainerAllocatorAMService is started instead of the
 * ApplicationMasterService if opportunistic scheduling is enabled for the YARN
 * cluster (either centralized or distributed opportunistic scheduling).
 *
 * It extends the functionality of the ApplicationMasterService by servicing
 * clients (AMs and AMRMProxy request interceptors) that understand the
 * DistributedSchedulingProtocol.
 */
// TODO: 17/3/23 by zmyer
public class OpportunisticContainerAllocatorAMService extends ApplicationMasterService
    implements DistributedSchedulingAMProtocol, EventHandler<SchedulerEvent> {

    private static final Log LOG = LogFactory.getLog(OpportunisticContainerAllocatorAMService.class);
    //节点队列负载监视器
    private final NodeQueueLoadMonitor nodeMonitor;
    //容器分配对象
    private final OpportunisticContainerAllocator oppContainerAllocator;
    private final int k;
    //缓存刷新时间间隔
    private final long cacheRefreshInterval;
    //缓存节点列表
    private volatile List<RemoteNode> cachedNodes;
    //最后一次缓存变更时间
    private volatile long lastCacheUpdateTime;

    // TODO: 17/4/2 by zmyer
    public OpportunisticContainerAllocatorAMService(RMContext rmContext, YarnScheduler scheduler) {
        super(OpportunisticContainerAllocatorAMService.class.getName(), rmContext, scheduler);
        //创建容器分配对象
        this.oppContainerAllocator = new OpportunisticContainerAllocator(
            rmContext.getContainerTokenSecretManager());
        this.k = rmContext.getYarnConfiguration().getInt(
            YarnConfiguration.OPP_CONTAINER_ALLOCATION_NODES_NUMBER_USED,
            YarnConfiguration.DEFAULT_OPP_CONTAINER_ALLOCATION_NODES_NUMBER_USED);
        //节点负载计算时间间隔
        long nodeSortInterval = rmContext.getYarnConfiguration().getLong(
            YarnConfiguration.NM_CONTAINER_QUEUING_SORTING_NODES_INTERVAL_MS,
            YarnConfiguration.DEFAULT_NM_CONTAINER_QUEUING_SORTING_NODES_INTERVAL_MS);
        //缓存刷新时间间隔
        this.cacheRefreshInterval = nodeSortInterval;
        this.lastCacheUpdateTime = System.currentTimeMillis();
        //创建节点队列负载比较对象
        NodeQueueLoadMonitor.LoadComparator comparator =
            NodeQueueLoadMonitor.LoadComparator.valueOf(
                rmContext.getYarnConfiguration().get(
                    YarnConfiguration.NM_CONTAINER_QUEUING_LOAD_COMPARATOR,
                    YarnConfiguration.DEFAULT_NM_CONTAINER_QUEUING_LOAD_COMPARATOR));

        //节点队列负载监视器
        NodeQueueLoadMonitor topKSelector = new NodeQueueLoadMonitor(nodeSortInterval, comparator);
        float sigma = rmContext.getYarnConfiguration()
            .getFloat(YarnConfiguration.NM_CONTAINER_QUEUING_LIMIT_STDEV,
                YarnConfiguration.DEFAULT_NM_CONTAINER_QUEUING_LIMIT_STDEV);

        int limitMin, limitMax;
        if (comparator == NodeQueueLoadMonitor.LoadComparator.QUEUE_LENGTH) {
            limitMin = rmContext.getYarnConfiguration()
                .getInt(YarnConfiguration.NM_CONTAINER_QUEUING_MIN_QUEUE_LENGTH,
                    YarnConfiguration.
                        DEFAULT_NM_CONTAINER_QUEUING_MIN_QUEUE_LENGTH);
            limitMax = rmContext.getYarnConfiguration()
                .getInt(YarnConfiguration.NM_CONTAINER_QUEUING_MAX_QUEUE_LENGTH,
                    YarnConfiguration.
                        DEFAULT_NM_CONTAINER_QUEUING_MAX_QUEUE_LENGTH);
        } else {
            limitMin = rmContext.getYarnConfiguration()
                .getInt(
                    YarnConfiguration.NM_CONTAINER_QUEUING_MIN_QUEUE_WAIT_TIME_MS,
                    YarnConfiguration.
                        DEFAULT_NM_CONTAINER_QUEUING_MIN_QUEUE_WAIT_TIME_MS);
            limitMax = rmContext.getYarnConfiguration()
                .getInt(
                    YarnConfiguration.NM_CONTAINER_QUEUING_MAX_QUEUE_WAIT_TIME_MS,
                    YarnConfiguration.
                        DEFAULT_NM_CONTAINER_QUEUING_MAX_QUEUE_WAIT_TIME_MS);
        }

        topKSelector.initThresholdCalculator(sigma, limitMin, limitMax);
        this.nodeMonitor = topKSelector;
    }

    // TODO: 17/4/2 by zmyer
    @Override
    public Server getServer(YarnRPC rpc, Configuration serverConf,
        InetSocketAddress addr, AMRMTokenSecretManager secretManager) {
        if (YarnConfiguration.isDistSchedulingEnabled(serverConf)) {
            //创建指定协议对应的rpc服务器对象
            Server server = rpc.getServer(DistributedSchedulingAMProtocol.class, this,
                addr, serverConf, secretManager,
                serverConf.getInt(YarnConfiguration.RM_SCHEDULER_CLIENT_THREAD_COUNT,
                    YarnConfiguration.DEFAULT_RM_SCHEDULER_CLIENT_THREAD_COUNT));
            // To support application running on NMs that DO NOT support
            // Dist Scheduling... The server multiplexes both the
            // ApplicationMasterProtocol as well as the DistributedSchedulingProtocol
            //开始在rpc服务器上注册相关协议实现
            ((RPC.Server) server).addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
                ApplicationMasterProtocolPB.class,
                ApplicationMasterProtocolService.newReflectiveBlockingService(
                    new ApplicationMasterProtocolPBServiceImpl(this)));
            //返回rpc服务器
            return server;
        }
        return super.getServer(rpc, serverConf, addr, secretManager);
    }

    // TODO: 17/4/2 by zmyer
    @Override
    public RegisterApplicationMasterResponse registerApplicationMaster
    (RegisterApplicationMasterRequest request) throws YarnException, IOException {
        //读取本次执行的应用执行id
        final ApplicationAttemptId appAttemptId = getAppAttemptId();
        //读取本次执行对象
        SchedulerApplicationAttempt appAttempt = ((AbstractYarnScheduler)
            rmContext.getScheduler()).getApplicationAttempt(appAttemptId);
        if (appAttempt.getOpportunisticContainerContext() == null) {
            //创建容器上下文对象
            OpportunisticContainerContext opCtx = new OpportunisticContainerContext();
            //设置生成容器对象的id生成器
            opCtx.setContainerIdGenerator(new OpportunisticContainerAllocator
                .ContainerIdGenerator() {
                @Override
                public long generateContainerId() {
                    return appAttempt.getAppSchedulingInfo().getNewContainerId();
                }
            });
            //获取token过期的时间间隔
            int tokenExpiryInterval = getConfig()
                .getInt(YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
                    YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS);
            //更新容器分配器对象的分配参数信息
            opCtx.updateAllocationParams(rmContext.getScheduler().getMinimumResourceCapability(),
                rmContext.getScheduler().getMaximumResourceCapability(),
                rmContext.getScheduler().getMinimumResourceCapability(),
                tokenExpiryInterval);
            //将该容器对象注册到本次执行对象中
            appAttempt.setOpportunisticContainerContext(opCtx);
        }
        return super.registerApplicationMaster(request);
    }

    // TODO: 17/4/2 by zmyer
    @Override
    public FinishApplicationMasterResponse finishApplicationMaster
    (FinishApplicationMasterRequest request) throws YarnException, IOException {
        return super.finishApplicationMaster(request);
    }

    // TODO: 17/4/2 by zmyer
    @Override
    protected void allocateInternal(ApplicationAttemptId appAttemptId,
        AllocateRequest request, AllocateResponse allocateResponse)
        throws YarnException {
        // Partition requests to GUARANTEED and OPPORTUNISTIC.
        OpportunisticContainerAllocator.PartitionedResourceRequests
            partitionedAsks = oppContainerAllocator.partitionAskList(request.getAskList());

        // Allocate OPPORTUNISTIC containers.
        SchedulerApplicationAttempt appAttempt = ((AbstractYarnScheduler) rmContext.getScheduler())
            .getApplicationAttempt(appAttemptId);

        OpportunisticContainerContext oppCtx = appAttempt.getOpportunisticContainerContext();
        oppCtx.updateNodeList(getLeastLoadedNodes());

        List<Container> oppContainers =
            oppContainerAllocator.allocateContainers(
                request.getResourceBlacklistRequest(),
                partitionedAsks.getOpportunistic(), appAttemptId, oppCtx,
                ResourceManager.getClusterTimeStamp(), appAttempt.getUser());

        // Create RMContainers and update the NMTokens.
        if (!oppContainers.isEmpty()) {
            handleNewContainers(oppContainers, false);
            appAttempt.updateNMTokens(oppContainers);
            addToAllocatedContainers(allocateResponse, oppContainers);
        }

        // Allocate GUARANTEED containers.
        request.setAskList(partitionedAsks.getGuaranteed());
        super.allocateInternal(appAttemptId, request, allocateResponse);
    }

    @Override
    public RegisterDistributedSchedulingAMResponse
    registerApplicationMasterForDistributedScheduling(RegisterApplicationMasterRequest request)
        throws YarnException, IOException {
        RegisterApplicationMasterResponse response =
            registerApplicationMaster(request);
        RegisterDistributedSchedulingAMResponse dsResp = recordFactory
            .newRecordInstance(RegisterDistributedSchedulingAMResponse.class);
        dsResp.setRegisterResponse(response);
        dsResp.setMinContainerResource(
            rmContext.getScheduler().getMinimumResourceCapability());
        dsResp.setMaxContainerResource(
            rmContext.getScheduler().getMaximumResourceCapability());
        dsResp.setContainerTokenExpiryInterval(
            getConfig().getInt(
                YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
                YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS));
        dsResp.setContainerIdStart(
            this.rmContext.getEpoch() << ResourceManager.EPOCH_BIT_SHIFT);

        // Set nodes to be used for scheduling
        dsResp.setNodesForScheduling(getLeastLoadedNodes());
        return dsResp;
    }

    @Override
    public DistributedSchedulingAllocateResponse allocateForDistributedScheduling(
        DistributedSchedulingAllocateRequest request)
        throws YarnException, IOException {
        List<Container> distAllocContainers = request.getAllocatedContainers();
        handleNewContainers(distAllocContainers, true);
        AllocateResponse response = allocate(request.getAllocateRequest());
        DistributedSchedulingAllocateResponse dsResp = recordFactory
            .newRecordInstance(DistributedSchedulingAllocateResponse.class);
        dsResp.setAllocateResponse(response);
        dsResp.setNodesForScheduling(getLeastLoadedNodes());
        return dsResp;
    }

    private void handleNewContainers(List<Container> allocContainers,
        boolean isRemotelyAllocated) {
        for (Container container : allocContainers) {
            // Create RMContainer
            SchedulerApplicationAttempt appAttempt = ((AbstractYarnScheduler) rmContext.getScheduler())
                .getCurrentAttemptForContainer(container.getId());
            RMContainer rmContainer = new RMContainerImpl(container,
                appAttempt.getApplicationAttemptId(), container.getNodeId(),
                appAttempt.getUser(), rmContext, isRemotelyAllocated);
            appAttempt.addRMContainer(container.getId(), rmContainer);
            ((AbstractYarnScheduler) rmContext.getScheduler()).getNode(
                container.getNodeId()).allocateContainer(rmContainer);
            rmContainer.handle(
                new RMContainerEvent(container.getId(),
                    RMContainerEventType.ACQUIRED));
        }
    }

    @Override
    public void handle(SchedulerEvent event) {
        switch (event.getType()) {
            case NODE_ADDED:
                if (!(event instanceof NodeAddedSchedulerEvent)) {
                    throw new RuntimeException("Unexpected event type: " + event);
                }
                NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent) event;
                nodeMonitor.addNode(nodeAddedEvent.getContainerReports(),
                    nodeAddedEvent.getAddedRMNode());
                break;
            case NODE_REMOVED:
                if (!(event instanceof NodeRemovedSchedulerEvent)) {
                    throw new RuntimeException("Unexpected event type: " + event);
                }
                NodeRemovedSchedulerEvent nodeRemovedEvent =
                    (NodeRemovedSchedulerEvent) event;
                nodeMonitor.removeNode(nodeRemovedEvent.getRemovedRMNode());
                break;
            case NODE_UPDATE:
                if (!(event instanceof NodeUpdateSchedulerEvent)) {
                    throw new RuntimeException("Unexpected event type: " + event);
                }
                NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)
                    event;
                nodeMonitor.updateNode(nodeUpdatedEvent.getRMNode());
                break;
            case NODE_RESOURCE_UPDATE:
                if (!(event instanceof NodeResourceUpdateSchedulerEvent)) {
                    throw new RuntimeException("Unexpected event type: " + event);
                }
                NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent =
                    (NodeResourceUpdateSchedulerEvent) event;
                nodeMonitor.updateNodeResource(nodeResourceUpdatedEvent.getRMNode(),
                    nodeResourceUpdatedEvent.getResourceOption());
                break;

            // <-- IGNORED EVENTS : START -->
            case APP_ADDED:
                break;
            case APP_REMOVED:
                break;
            case APP_ATTEMPT_ADDED:
                break;
            case APP_ATTEMPT_REMOVED:
                break;
            case CONTAINER_EXPIRED:
                break;
            case NODE_LABELS_UPDATE:
                break;
            // <-- IGNORED EVENTS : END -->
            default:
                LOG.error("Unknown event arrived at" +
                    "OpportunisticContainerAllocatorAMService: " + event.toString());
        }

    }

    // TODO: 17/3/23 by zmyer
    public QueueLimitCalculator getNodeManagerQueueLimitCalculator() {
        return nodeMonitor.getThresholdCalculator();
    }

    private synchronized List<RemoteNode> getLeastLoadedNodes() {
        long currTime = System.currentTimeMillis();
        if ((currTime - lastCacheUpdateTime > cacheRefreshInterval)
            || (cachedNodes == null)) {
            cachedNodes = convertToRemoteNodes(
                this.nodeMonitor.selectLeastLoadedNodes(this.k));
            if (cachedNodes.size() > 0) {
                lastCacheUpdateTime = currTime;
            }
        }
        return cachedNodes;
    }

    private List<RemoteNode> convertToRemoteNodes(List<NodeId> nodeIds) {
        ArrayList<RemoteNode> retNodes = new ArrayList<>();
        for (NodeId nId : nodeIds) {
            RemoteNode remoteNode = convertToRemoteNode(nId);
            if (null != remoteNode) {
                retNodes.add(remoteNode);
            }
        }
        return retNodes;
    }

    private RemoteNode convertToRemoteNode(NodeId nodeId) {
        SchedulerNode node =
            ((AbstractYarnScheduler) rmContext.getScheduler()).getNode(nodeId);
        return node != null ? RemoteNode.newInstance(nodeId, node.getHttpAddress())
            : null;
    }

    private static ApplicationAttemptId getAppAttemptId() throws YarnException {
        AMRMTokenIdentifier amrmTokenIdentifier =
            YarnServerSecurityUtils.authorizeRequest();
        ApplicationAttemptId applicationAttemptId =
            amrmTokenIdentifier.getApplicationAttemptId();
        return applicationAttemptId;
    }
}
