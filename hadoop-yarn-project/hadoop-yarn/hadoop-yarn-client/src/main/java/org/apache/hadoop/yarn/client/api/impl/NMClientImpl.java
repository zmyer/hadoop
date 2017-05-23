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

package org.apache.hadoop.yarn.client.api.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.IncreaseContainersResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.impl.ContainerManagementProtocolProxy.ContainerManagementProtocolProxyData;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;

/**
 * <p>
 * This class implements {@link NMClient}. All the APIs are blocking.
 * </p>
 *
 * <p>
 * By default, this client stops all the running containers that are started by
 * it when it stops. It can be disabled via
 * {@link #cleanupRunningContainersOnStop}, in which case containers will
 * continue to run even after this client is stopped and till the application
 * runs at which point ResourceManager will forcefully kill them.
 * </p>
 *
 * <p>
 * Note that the blocking APIs ensure the RPC calls to <code>NodeManager</code>
 * are executed immediately, and the responses are received before these APIs
 * return. However, when {@link #startContainer} or {@link #stopContainer}
 * returns, <code>NodeManager</code> may still need some time to either start
 * or stop the container because of its asynchronous implementation. Therefore,
 * {@link #getContainerStatus} is likely to return a transit container status
 * if it is executed immediately after {@link #startContainer} or
 * {@link #stopContainer}.
 * </p>
 */
@Private
@Unstable
// TODO: 17/3/26 by zmyer
public class NMClientImpl extends NMClient {

    private static final Log LOG = LogFactory.getLog(NMClientImpl.class);

    // The logically coherent operations on startedContainers is synchronized to
    // ensure they are atomic
    //已经启动的容器映射表
    protected ConcurrentMap<ContainerId, StartedContainer> startedContainers =
        new ConcurrentHashMap<ContainerId, StartedContainer>();

    //enabled by default
    //是否清理正在运行的容器
    private final AtomicBoolean cleanupRunningContainers = new AtomicBoolean(true);
    //容器管理器协议代理对象
    private ContainerManagementProtocolProxy cmProxy;

    // TODO: 17/3/26 by zmyer
    public NMClientImpl() {
        super(NMClientImpl.class.getName());
    }

    // TODO: 17/3/26 by zmyer
    public NMClientImpl(String name) {
        super(name);
    }

    // TODO: 17/3/26 by zmyer
    @Override
    protected void serviceStop() throws Exception {
        // Usually, started-containers are stopped when this client stops. Unless
        // the flag cleanupRunningContainers is set to false.
        if (getCleanupRunningContainers().get()) {
            //启动运行的容器
            cleanupRunningContainers();
        }
        //关闭容器管理协议代理
        cmProxy.stopAllProxies();
        super.serviceStop();
    }

    // TODO: 17/3/26 by zmyer
    synchronized void cleanupRunningContainers() {
        for (StartedContainer startedContainer : startedContainers.values()) {
            try {
                //关闭容器
                stopContainer(startedContainer.getContainerId(), startedContainer.getNodeId());
            } catch (YarnException | IOException e) {
                LOG.error("Failed to stop Container " +
                    startedContainer.getContainerId() +
                    "when stopping NMClientImpl");
            }
        }
    }

    // TODO: 17/3/26 by zmyer
    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        if (getNMTokenCache() == null) {
            throw new IllegalStateException("NMTokenCache has not been set");
        }
        //创建容器管理协议代理对象
        cmProxy = new ContainerManagementProtocolProxy(conf, getNMTokenCache());
    }

    // TODO: 17/3/26 by zmyer
    @Override
    public void cleanupRunningContainersOnStop(boolean enabled) {
        getCleanupRunningContainers().set(enabled);
    }

    // TODO: 17/3/26 by zmyer
    protected static class StartedContainer {
        //容器id
        private ContainerId containerId;
        //节点id
        private NodeId nodeId;
        //容器状态
        private ContainerState state;

        // TODO: 17/3/26 by zmyer
        StartedContainer(ContainerId containerId, NodeId nodeId) {
            this.containerId = containerId;
            this.nodeId = nodeId;
            state = ContainerState.NEW;
        }

        // TODO: 17/3/26 by zmyer
        public ContainerId getContainerId() {
            return containerId;
        }

        // TODO: 17/3/26 by zmyer
        public NodeId getNodeId() {
            return nodeId;
        }
    }

    // TODO: 17/3/26 by zmyer
    private void addStartingContainer(StartedContainer startedContainer)
        throws YarnException {
        if (startedContainers.putIfAbsent(startedContainer.containerId, startedContainer) != null) {
            throw RPCUtil.getRemoteException("Container "
                + startedContainer.containerId.toString() + " is already started");
        }
    }

    // TODO: 17/3/26 by zmyer
    @Override
    public Map<String, ByteBuffer> startContainer(Container container,
        ContainerLaunchContext containerLaunchContext) throws YarnException, IOException {
        // Do synchronization on StartedContainer to prevent race condition
        // between startContainer and stopContainer only when startContainer is
        // in progress for a given container.
        StartedContainer startingContainer =
            new StartedContainer(container.getId(), container.getNodeId());
        synchronized (startingContainer) {
            //将容器记录插入到列表中
            addStartingContainer(startingContainer);

            Map<String, ByteBuffer> allServiceResponse;
            ContainerManagementProtocolProxyData proxy = null;
            try {
                //读取指定容器的代理对象
                proxy = cmProxy.getProxy(container.getNodeId().toString(), container.getId());
                //创建启动容器请求对象
                StartContainerRequest scRequest =
                    StartContainerRequest.newInstance(containerLaunchContext,
                        container.getContainerToken());
                //启动容器请求列表
                List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
                //将该请求插入到列表中
                list.add(scRequest);
                //拷贝请求列表
                StartContainersRequest allRequests = StartContainersRequest.newInstance(list);
                //启动指定的容器
                StartContainersResponse response =
                    proxy.getContainerManagementProtocol().startContainers(allRequests);
                if (response.getFailedRequests() != null
                    && response.getFailedRequests().containsKey(container.getId())) {
                    Throwable t =
                        response.getFailedRequests().get(container.getId()).deSerialize();
                    parseAndThrowException(t);
                }
                //读取应答结果元数据
                allServiceResponse = response.getAllServicesMetaData();
                //设置容器运行状态
                startingContainer.state = ContainerState.RUNNING;
            } catch (YarnException | IOException e) {
                startingContainer.state = ContainerState.COMPLETE;
                // Remove the started container if it failed to start
                startedContainers.remove(startingContainer.containerId);
                throw e;
            } catch (Throwable t) {
                startingContainer.state = ContainerState.COMPLETE;
                startedContainers.remove(startingContainer.containerId);
                throw RPCUtil.getRemoteException(t);
            } finally {
                if (proxy != null) {
                    cmProxy.mayBeCloseProxy(proxy);
                }
            }
            return allServiceResponse;
        }
    }

    // TODO: 17/3/27 by zmyer
    @Override
    public void increaseContainerResource(Container container)
        throws YarnException, IOException {
        ContainerManagementProtocolProxyData proxy = null;
        try {
            //获取指定的远程代理对象
            proxy = cmProxy.getProxy(container.getNodeId().toString(), container.getId());
            List<Token> increaseTokens = new ArrayList<>();
            //设置容器的访问token对象
            increaseTokens.add(container.getContainerToken());
            //创建增加容器资源请求
            IncreaseContainersResourceRequest increaseRequest =
                IncreaseContainersResourceRequest.newInstance(increaseTokens);
            //向远程发送增加容器资源请求
            IncreaseContainersResourceResponse response =
                proxy.getContainerManagementProtocol().increaseContainersResource(increaseRequest);
            if (response.getFailedRequests() != null
                && response.getFailedRequests().containsKey(container.getId())) {
                Throwable t = response.getFailedRequests().get(container.getId()).deSerialize();
                parseAndThrowException(t);
            }
        } finally {
            if (proxy != null) {
                cmProxy.mayBeCloseProxy(proxy);
            }
        }
    }

    // TODO: 17/3/27 by zmyer
    @Override
    public void stopContainer(ContainerId containerId, NodeId nodeId)
        throws YarnException, IOException {
        //获取已启动的容器对象
        StartedContainer startedContainer = startedContainers.get(containerId);

        // Only allow one request of stopping the container to move forward
        // When entering the block, check whether the precursor has already stopped
        // the container
        if (startedContainer != null) {
            synchronized (startedContainer) {
                //如果当前的容器对象的状态不为运行态,则直接退出
                if (startedContainer.state != ContainerState.RUNNING) {
                    return;
                }
                //关闭容器
                stopContainerInternal(containerId, nodeId);
                // Only after successful
                //设置容器状态为完成态
                startedContainer.state = ContainerState.COMPLETE;
                //从启动的容器列表中删除该容器
                startedContainers.remove(startedContainer.containerId);
            }
        } else {
            //停止容器
            stopContainerInternal(containerId, nodeId);
        }

    }

    // TODO: 17/3/27 by zmyer
    @Override
    public ContainerStatus getContainerStatus(ContainerId containerId, NodeId nodeId)
        throws YarnException, IOException {

        //容器管理协议代理对象
        ContainerManagementProtocolProxyData proxy = null;
        List<ContainerId> containerIds = new ArrayList<ContainerId>();
        //将容器id插入到待查询的容器列表中
        containerIds.add(containerId);
        try {
            //读取指定容器的代理对象
            proxy = cmProxy.getProxy(nodeId.toString(), containerId);
            //获取指定容器的运行状态
            GetContainerStatusesResponse response =
                proxy.getContainerManagementProtocol().getContainerStatuses(
                    GetContainerStatusesRequest.newInstance(containerIds));
            if (response.getFailedRequests() != null
                && response.getFailedRequests().containsKey(containerId)) {
                Throwable t = response.getFailedRequests().get(containerId).deSerialize();
                parseAndThrowException(t);
            }
            //读取容器运行状态
            ContainerStatus containerStatus = response.getContainerStatuses().get(0);
            return containerStatus;
        } finally {
            if (proxy != null) {
                cmProxy.mayBeCloseProxy(proxy);
            }
        }
    }

    // TODO: 17/3/27 by zmyer
    private void stopContainerInternal(ContainerId containerId, NodeId nodeId)
        throws IOException, YarnException {
        ContainerManagementProtocolProxyData proxy = null;
        List<ContainerId> containerIds = new ArrayList<ContainerId>();
        containerIds.add(containerId);
        try {
            //读取容器的代理对象
            proxy = cmProxy.getProxy(nodeId.toString(), containerId);
            //发送关闭容器消息
            StopContainersResponse response =
                proxy.getContainerManagementProtocol().stopContainers(
                    StopContainersRequest.newInstance(containerIds));
            if (response.getFailedRequests() != null
                && response.getFailedRequests().containsKey(containerId)) {
                Throwable t = response.getFailedRequests().get(containerId)
                    .deSerialize();
                parseAndThrowException(t);
            }
        } finally {
            if (proxy != null) {
                cmProxy.mayBeCloseProxy(proxy);
            }
        }
    }

    // TODO: 17/3/27 by zmyer
    public AtomicBoolean getCleanupRunningContainers() {
        return cleanupRunningContainers;
    }

    // TODO: 17/3/27 by zmyer
    private void parseAndThrowException(Throwable t) throws YarnException, IOException {
        if (t instanceof YarnException) {
            throw (YarnException) t;
        } else if (t instanceof InvalidToken) {
            throw (InvalidToken) t;
        } else {
            throw (IOException) t;
        }
    }
}
