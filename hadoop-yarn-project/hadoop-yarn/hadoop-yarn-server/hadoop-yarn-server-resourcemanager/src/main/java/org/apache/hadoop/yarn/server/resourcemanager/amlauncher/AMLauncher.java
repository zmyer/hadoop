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

package org.apache.hadoop.yarn.server.resourcemanager.amlauncher;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.NMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

/**
 * The launch of the AM itself.
 */
// TODO: 17/3/24 by zmyer
public class AMLauncher implements Runnable {
    private static final Log LOG = LogFactory.getLog(AMLauncher.class);
    //容器管理器协议对象
    private ContainerManagementProtocol containerMgrProxy;
    //应用尝试执行对象
    private final RMAppAttempt application;
    //配置对象
    private final Configuration conf;
    //am启动事件类类型
    private final AMLauncherEventType eventType;
    //rm执行上下文对象
    private final RMContext rmContext;
    //容器对象
    private final Container masterContainer;

    @SuppressWarnings("rawtypes")
    //事件处理器对象
    private final EventHandler handler;

    // TODO: 17/4/3 by zmyer
    public AMLauncher(RMContext rmContext, RMAppAttempt application,
        AMLauncherEventType eventType, Configuration conf) {
        this.application = application;
        this.conf = conf;
        this.eventType = eventType;
        this.rmContext = rmContext;
        this.handler = rmContext.getDispatcher().getEventHandler();
        this.masterContainer = application.getMasterContainer();
    }

    // TODO: 17/4/3 by zmyer
    private void connect() throws IOException {
        //首先读取容器id
        ContainerId masterContainerID = masterContainer.getId();
        //读取指定容器的代理对象
        containerMgrProxy = getContainerMgrProxy(masterContainerID);
    }

    // TODO: 17/4/3 by zmyer
    private void launch() throws IOException, YarnException {
        //开始连接容器对象
        connect();
        //读取容器id
        ContainerId masterContainerID = masterContainer.getId();
        //读取应用提交上下文对象
        ApplicationSubmissionContext applicationContext = application.getSubmissionContext();
        LOG.info("Setting up container " + masterContainer + " for AM " + application.getAppAttemptId());
        //容器启动上下文对象
        ContainerLaunchContext launchContext = createAMContainerLaunchContext(applicationContext, masterContainerID);

        //创建启动容器对象请求
        StartContainerRequest scRequest = StartContainerRequest.newInstance(launchContext,
            masterContainer.getContainerToken());
        List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
        list.add(scRequest);
        StartContainersRequest allRequests = StartContainersRequest.newInstance(list);

        StartContainersResponse response = containerMgrProxy.startContainers(allRequests);
        if (response.getFailedRequests() != null && response.getFailedRequests().containsKey(masterContainerID)) {
            Throwable t = response.getFailedRequests().get(masterContainerID).deSerialize();
            parseAndThrowException(t);
        } else {
            LOG.info("Done launching container " + masterContainer + " for AM "
                + application.getAppAttemptId());
        }
    }

    // TODO: 17/4/4 by zmyer
    private void cleanup() throws IOException, YarnException {
        connect();
        ContainerId containerId = masterContainer.getId();
        List<ContainerId> containerIds = new ArrayList<ContainerId>();
        containerIds.add(containerId);
        StopContainersRequest stopRequest = StopContainersRequest.newInstance(containerIds);
        StopContainersResponse response = containerMgrProxy.stopContainers(stopRequest);
        if (response.getFailedRequests() != null && response.getFailedRequests().containsKey(containerId)) {
            Throwable t = response.getFailedRequests().get(containerId).deSerialize();
            parseAndThrowException(t);
        }
    }

    // Protected. For tests.
    // TODO: 17/4/4 by zmyer
    protected ContainerManagementProtocol getContainerMgrProxy(final ContainerId containerId) {
        final NodeId node = masterContainer.getNodeId();
        final InetSocketAddress containerManagerConnectAddress =
            NetUtils.createSocketAddrForHost(node.getHost(), node.getPort());

        final YarnRPC rpc = getYarnRPC();
        UserGroupInformation currentUser =
            UserGroupInformation.createRemoteUser(containerId.getApplicationAttemptId().toString());

        String user = rmContext.getRMApps().get(containerId.getApplicationAttemptId().getApplicationId())
            .getUser();
        org.apache.hadoop.yarn.api.records.Token token = rmContext.getNMTokenSecretManager().createNMToken(
            containerId.getApplicationAttemptId(), node, user);
        currentUser.addToken(ConverterUtils.convertFromYarn(token, containerManagerConnectAddress));

        return NMProxy.createNMProxy(conf, ContainerManagementProtocol.class,
            currentUser, rpc, containerManagerConnectAddress);
    }

    @VisibleForTesting
    protected YarnRPC getYarnRPC() {
        return YarnRPC.create(conf);  // TODO: Don't create again and again.
    }

    // TODO: 17/4/3 by zmyer
    private ContainerLaunchContext createAMContainerLaunchContext(
        ApplicationSubmissionContext applicationMasterContext,
        ContainerId containerID) throws IOException {

        // Construct the actual Container
        //创建容器启动上下文对象
        ContainerLaunchContext container = applicationMasterContext.getAMContainerSpec();

        // Populate the current queue name in the environment variable.
        //设置当前应用的队列名称
        setupQueueNameEnv(container, applicationMasterContext);

        // Finalize the container
        //设置容器token
        setupTokens(container, containerID);
        // set the flow context optionally for timeline service v.2
        //设置流的上下文对象
        setFlowContext(container);
        //返回容器
        return container;
    }

    // TODO: 17/4/3 by zmyer
    private void setupQueueNameEnv(ContainerLaunchContext container,
        ApplicationSubmissionContext applicationMasterContext) {
        String queueName = applicationMasterContext.getQueue();
        if (queueName == null) {
            queueName = YarnConfiguration.DEFAULT_QUEUE_NAME;
        }
        container.getEnvironment().put(ApplicationConstants.Environment
            .YARN_RESOURCEMANAGER_APPLICATION_QUEUE.key(), queueName);
    }

    // TODO: 17/4/3 by zmyer
    @Private
    @VisibleForTesting
    protected void setupTokens(ContainerLaunchContext container, ContainerId containerID)
        throws IOException {
        Map<String, String> environment = container.getEnvironment();
        environment.put(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV,
            application.getWebProxyBase());
        // Set AppSubmitTime to be consumable by the AM.
        ApplicationId applicationId = application.getAppAttemptId().getApplicationId();
        environment.put(ApplicationConstants.APP_SUBMIT_TIME_ENV,
            String.valueOf(rmContext.getRMApps().get(applicationId).getSubmitTime()));

        Credentials credentials = new Credentials();
        DataInputByteBuffer dibb = new DataInputByteBuffer();
        ByteBuffer tokens = container.getTokens();
        if (tokens != null) {
            // TODO: Don't do this kind of checks everywhere.
            dibb.reset(tokens);
            credentials.readTokenStorageStream(dibb);
            tokens.rewind();
        }

        // Add AMRMToken
        Token<AMRMTokenIdentifier> amrmToken = createAndSetAMRMToken();
        if (amrmToken != null) {
            credentials.addToken(amrmToken.getService(), amrmToken);
        }
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        container.setTokens(ByteBuffer.wrap(dob.getData(), 0, dob.getLength()));
    }

    // TODO: 17/4/3 by zmyer
    private void setFlowContext(ContainerLaunchContext container) {
        if (YarnConfiguration.timelineServiceV2Enabled(conf)) {
            Map<String, String> environment = container.getEnvironment();
            ApplicationId applicationId = application.getAppAttemptId().getApplicationId();
            RMApp app = rmContext.getRMApps().get(applicationId);

            // initialize the flow in the environment with default values for those
            // that do not specify the flow tags
            // flow name: app name (or app id if app name is missing),
            // flow version: "1", flow run id: start time
            setFlowTags(environment, TimelineUtils.FLOW_NAME_TAG_PREFIX,
                TimelineUtils.generateDefaultFlowName(app.getName(), applicationId));
            setFlowTags(environment, TimelineUtils.FLOW_VERSION_TAG_PREFIX,
                TimelineUtils.DEFAULT_FLOW_VERSION);
            setFlowTags(environment, TimelineUtils.FLOW_RUN_ID_TAG_PREFIX,
                String.valueOf(app.getStartTime()));

            // Set flow context info: the flow context is received via the application
            // tags
            for (String tag : app.getApplicationTags()) {
                String[] parts = tag.split(":", 2);
                if (parts.length != 2 || parts[1].isEmpty()) {
                    continue;
                }
                switch (parts[0].toUpperCase()) {
                    case TimelineUtils.FLOW_NAME_TAG_PREFIX:
                        setFlowTags(environment, TimelineUtils.FLOW_NAME_TAG_PREFIX,
                            parts[1]);
                        break;
                    case TimelineUtils.FLOW_VERSION_TAG_PREFIX:
                        setFlowTags(environment, TimelineUtils.FLOW_VERSION_TAG_PREFIX,
                            parts[1]);
                        break;
                    case TimelineUtils.FLOW_RUN_ID_TAG_PREFIX:
                        setFlowTags(environment, TimelineUtils.FLOW_RUN_ID_TAG_PREFIX,
                            parts[1]);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    // TODO: 17/4/3 by zmyer
    private static void setFlowTags(
        Map<String, String> environment, String tagPrefix, String value) {
        if (!value.isEmpty()) {
            environment.put(tagPrefix, value);
        }
    }

    // TODO: 17/4/3 by zmyer
    @VisibleForTesting
    protected Token<AMRMTokenIdentifier> createAndSetAMRMToken() {
        Token<AMRMTokenIdentifier> amrmToken =
            this.rmContext.getAMRMTokenSecretManager().createAndGetAMRMToken(
                application.getAppAttemptId());
        ((RMAppAttemptImpl) application).setAMRMToken(amrmToken);
        return amrmToken;
    }

    // TODO: 17/4/3 by zmyer
    @SuppressWarnings("unchecked")
    public void run() {
        switch (eventType) {
            case LAUNCH:
                try {
                    LOG.info("Launching master" + application.getAppAttemptId());
                    //启动容器
                    launch();
                    //处理容器启动事件对象
                    handler.handle(new RMAppAttemptEvent(application.getAppAttemptId(),
                        RMAppAttemptEventType.LAUNCHED));
                } catch (Exception ie) {
                    String message = "Error launching " + application.getAppAttemptId()
                        + ". Got exception: " + StringUtils.stringifyException(ie);
                    LOG.info(message);
                    handler.handle(new RMAppAttemptEvent(application
                        .getAppAttemptId(), RMAppAttemptEventType.LAUNCH_FAILED, message));
                }
                break;
            case CLEANUP:
                try {
                    LOG.info("Cleaning master " + application.getAppAttemptId());
                    //清理容器对象
                    cleanup();
                } catch (IOException ie) {
                    LOG.info("Error cleaning master ", ie);
                } catch (YarnException e) {
                    StringBuilder sb = new StringBuilder("Container ");
                    sb.append(masterContainer.getId().toString());
                    sb.append(" is not handled by this NodeManager");
                    if (!e.getMessage().contains(sb.toString())) {
                        // Ignoring if container is already killed by Node Manager.
                        LOG.info("Error cleaning master ", e);
                    }
                }
                break;
            default:
                LOG.warn("Received unknown event-type " + eventType + ". Ignoring.");
                break;
        }
    }

    // TODO: 17/4/3 by zmyer
    private void parseAndThrowException(Throwable t) throws YarnException,
        IOException {
        if (t instanceof YarnException) {
            throw (YarnException) t;
        } else if (t instanceof InvalidToken) {
            throw (InvalidToken) t;
        } else {
            throw (IOException) t;
        }
    }
}
