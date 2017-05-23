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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FailApplicationAttemptRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationAttemptsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetLabelsToNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewReservationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNodesToLabelsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.MoveApplicationAcrossQueuesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationDeleteResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationListResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationUpdateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationPriorityRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UpdateApplicationTimeoutsResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.SignalContainerCommand;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.AHSClient;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationIdNotProvidedException;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.ContainerNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;

// TODO: 17/3/22 by zmyer
@Private
@Unstable
public class YarnClientImpl extends YarnClient {

    private static final Log LOG = LogFactory.getLog(YarnClientImpl.class);
    //应用客户端协议对象
    protected ApplicationClientProtocol rmClient;
    //应用提交间隔时间
    protected long submitPollIntervalMillis;
    //异步poll间隔时间
    private long asyncApiPollIntervalMillis;
    //异步poll超时时间
    private long asyncApiPollTimeoutMillis;
    //
    protected AHSClient historyClient;
    //
    private boolean historyServiceEnabled;
    //时间线客户端对象
    protected TimelineClient timelineClient;
    @VisibleForTesting
    Text timelineService;
    @VisibleForTesting
    String timelineDTRenewer;
    //是否开启时间线服务
    protected boolean timelineServiceEnabled;
    //
    protected boolean timelineServiceBestEffort;

    private static final String ROOT = "root";

    // TODO: 17/3/25 by zmyer
    public YarnClientImpl() {
        super(YarnClientImpl.class.getName());
    }

    @SuppressWarnings("deprecation")
    @Override
    // TODO: 17/3/25 by zmyer
    protected void serviceInit(Configuration conf) throws Exception {
        //异步poll间隔时间
        asyncApiPollIntervalMillis =
            conf.getLong(YarnConfiguration.YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS,
                YarnConfiguration.DEFAULT_YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS);
        //异步poll超时时间
        asyncApiPollTimeoutMillis =
            conf.getLong(YarnConfiguration.YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_TIMEOUT_MS,
                YarnConfiguration.DEFAULT_YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_TIMEOUT_MS);
        //设置提交poll间隔时间
        submitPollIntervalMillis = asyncApiPollIntervalMillis;
        if (conf.get(YarnConfiguration.YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS) != null) {
            submitPollIntervalMillis = conf.getLong(
                YarnConfiguration.YARN_CLIENT_APP_SUBMISSION_POLL_INTERVAL_MS,
                YarnConfiguration.DEFAULT_YARN_CLIENT_APPLICATION_CLIENT_PROTOCOL_POLL_INTERVAL_MS);
        }

        if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
            try {
                //设置时间线服务
                timelineServiceEnabled = true;
                //创建时间线客户端对象
                timelineClient = createTimelineClient();
                //初始化时间线客户端对象
                timelineClient.init(conf);
                timelineDTRenewer = getTimelineDelegationTokenRenewer(conf);
                timelineService = TimelineUtils.buildTimelineTokenService(conf);
            } catch (NoClassDefFoundError error) {
                // When attempt to initiate the timeline client with
                // different set of dependencies, it may fail with
                // NoClassDefFoundError. When some of them are not compatible
                // with timeline server. This is not necessarily a fatal error
                // to the client.
                LOG.warn("Timeline client could not be initialized "
                        + "because dependency missing or incompatible,"
                        + " disabling timeline client.",
                    error);
                timelineServiceEnabled = false;
            }
        }

        // The AHSClientService is enabled by default when we start the
        // TimelineServer which means we are able to get history information
        // for applications/applicationAttempts/containers by using ahsClient
        // when the TimelineServer is running.
        if (timelineServiceEnabled || conf.getBoolean(
            YarnConfiguration.APPLICATION_HISTORY_ENABLED,
            YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED)) {
            historyServiceEnabled = true;
            historyClient = AHSClient.createAHSClient();
            historyClient.init(conf);
        }

        timelineServiceBestEffort = conf.getBoolean(
            YarnConfiguration.TIMELINE_SERVICE_CLIENT_BEST_EFFORT,
            YarnConfiguration.DEFAULT_TIMELINE_SERVICE_CLIENT_BEST_EFFORT);
        //父类服务初始化
        super.serviceInit(conf);
    }

    // TODO: 17/3/25 by zmyer
    TimelineClient createTimelineClient() throws IOException, YarnException {
        //创建时间线客户端对象
        return TimelineClient.createTimelineClient();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    protected void serviceStart() throws Exception {
        try {
            //创建资源客户端对象
            rmClient = ClientRMProxy.createRMProxy(getConfig(), ApplicationClientProtocol.class);
            if (historyServiceEnabled) {
                historyClient.start();
            }
            if (timelineServiceEnabled) {
                timelineClient.start();
            }
        } catch (IOException e) {
            throw new YarnRuntimeException(e);
        }
        //启动父类服务
        super.serviceStart();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    protected void serviceStop() throws Exception {
        if (this.rmClient != null) {
            //关闭rm客户端对象
            RPC.stopProxy(this.rmClient);
        }
        if (historyServiceEnabled) {
            historyClient.stop();
        }
        if (timelineServiceEnabled) {
            timelineClient.stop();
        }
        //关闭父类服务对象
        super.serviceStop();
    }

    // TODO: 17/3/25 by zmyer
    private GetNewApplicationResponse getNewApplication()
        throws YarnException, IOException {
        GetNewApplicationRequest request =
            Records.newRecord(GetNewApplicationRequest.class);
        //向服务请求新的应用id
        return rmClient.getNewApplication(request);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public YarnClientApplication createApplication()
        throws YarnException, IOException {
        ApplicationSubmissionContext context = Records.newRecord(ApplicationSubmissionContext.class);
        //首先从资源管理服务器读取新的应用id
        GetNewApplicationResponse newApp = getNewApplication();
        //读取新的应用id
        ApplicationId appId = newApp.getApplicationId();
        //为上下文对象设置新的应用id
        context.setApplicationId(appId);
        //返回yarn客户端应用对象
        return new YarnClientApplication(newApp, context);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public ApplicationId submitApplication(ApplicationSubmissionContext appContext)
        throws YarnException, IOException {
        //从应用提交上下文对象中读取新的应用id
        ApplicationId applicationId = appContext.getApplicationId();
        if (applicationId == null) {
            throw new ApplicationIdNotProvidedException(
                "ApplicationId is not provided in ApplicationSubmissionContext");
        }
        //构造应用提交请求对象
        SubmitApplicationRequest request = Records.newRecord(SubmitApplicationRequest.class);
        //为请求对象设置应用提交上下文对象
        request.setApplicationSubmissionContext(appContext);

        // Automatically add the timeline DT into the CLC
        // Only when the security and the timeline service are both enabled
        if (isSecurityEnabled() && timelineServiceEnabled) {
            addTimelineDelegationToken(appContext.getAMContainerSpec());
        }

        //TODO: YARN-1763:Handle RM failovers during the submitApplication call.
        //开始向资源管理器服务器提交请求
        rmClient.submitApplication(request);

        //poll数量
        int pollCount = 0;
        long startTime = System.currentTimeMillis();
        //应用等待状态的集合
        EnumSet<YarnApplicationState> waitingStates = EnumSet.of(YarnApplicationState.NEW,
            YarnApplicationState.NEW_SAVING, YarnApplicationState.SUBMITTED);
        //应用提交失败的状态集合
        EnumSet<YarnApplicationState> failToSubmitStates = EnumSet.of(YarnApplicationState.FAILED,
            YarnApplicationState.KILLED);
        while (true) {
            try {
                //读取应用运行报告
                ApplicationReport appReport = getApplicationReport(applicationId);
                //从应用运行报告中读取应用的运行状态
                YarnApplicationState state = appReport.getYarnApplicationState();
                if (!waitingStates.contains(state)) {
                    //如果提交的应用处于非正常状态
                    if (failToSubmitStates.contains(state)) {
                        throw new YarnException("Failed to submit " + applicationId +
                            " to YARN : " + appReport.getDiagnostics());
                    }
                    LOG.info("Submitted application " + applicationId);
                    break;
                }

                //计算话费的时间
                long elapsedMillis = System.currentTimeMillis() - startTime;
                if (enforceAsyncAPITimeout() && elapsedMillis >= asyncApiPollTimeoutMillis) {
                    throw new YarnException("Timed out while waiting for application " +
                        applicationId + " to be submitted successfully");
                }

                // Notify the client through the log every 10 poll, in case the client
                // is blocked here too long.
                if (++pollCount % 10 == 0) {
                    LOG.info("Application submission is not finished, " + "submitted application " + applicationId +
                        " is still in " + state);
                }
                try {
                    //等待片刻
                    Thread.sleep(submitPollIntervalMillis);
                } catch (InterruptedException ie) {
                    String msg = "Interrupted while waiting for application "
                        + applicationId + " to be successfully submitted.";
                    LOG.error(msg);
                    throw new YarnException(msg, ie);
                }
            } catch (ApplicationNotFoundException ex) {
                // FailOver or RM restart happens before RMStateStore saves
                // ApplicationState
                LOG.info("Re-submit application " + applicationId + "with the " +
                    "same ApplicationSubmissionContext");
                //重新提交应用对象
                rmClient.submitApplication(request);
            }
        }

        //返回应用id
        return applicationId;
    }

    // TODO: 17/3/25 by zmyer
    private void addTimelineDelegationToken(ContainerLaunchContext clc) throws YarnException, IOException {
        Credentials credentials = new Credentials();
        DataInputByteBuffer dibb = new DataInputByteBuffer();
        ByteBuffer tokens = clc.getTokens();
        if (tokens != null) {
            dibb.reset(tokens);
            credentials.readTokenStorageStream(dibb);
            tokens.rewind();
        }
        // If the timeline delegation token is already in the CLC, no need to add
        // one more
        for (org.apache.hadoop.security.token.Token<? extends TokenIdentifier> token : credentials
            .getAllTokens()) {
            if (token.getKind().equals(TimelineDelegationTokenIdentifier.KIND_NAME)) {
                return;
            }
        }
        org.apache.hadoop.security.token.Token<TimelineDelegationTokenIdentifier>
            timelineDelegationToken = getTimelineDelegationToken();
        if (timelineDelegationToken == null) {
            return;
        }
        credentials.addToken(timelineService, timelineDelegationToken);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Add timline delegation token into credentials: "
                + timelineDelegationToken);
        }
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        tokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
        clc.setTokens(tokens);
    }

    // TODO: 17/3/25 by zmyer
    @VisibleForTesting
    org.apache.hadoop.security.token.Token<TimelineDelegationTokenIdentifier>
    getTimelineDelegationToken() throws IOException, YarnException {
        try {
            return timelineClient.getDelegationToken(timelineDTRenewer);
        } catch (Exception e) {
            if (timelineServiceBestEffort) {
                LOG.warn("Failed to get delegation token from the timeline server: "
                    + e.getMessage());
                return null;
            }
            throw e;
        }
    }

    // TODO: 17/3/25 by zmyer
    private static String getTimelineDelegationTokenRenewer(Configuration conf)
        throws IOException, YarnException {
        // Parse the RM daemon user if it exists in the config
        String rmPrincipal = conf.get(YarnConfiguration.RM_PRINCIPAL);
        String renewer = null;
        if (rmPrincipal != null && rmPrincipal.length() > 0) {
            String rmHost = conf.getSocketAddr(
                YarnConfiguration.RM_ADDRESS,
                YarnConfiguration.DEFAULT_RM_ADDRESS,
                YarnConfiguration.DEFAULT_RM_PORT).getHostName();
            renewer = SecurityUtil.getServerPrincipal(rmPrincipal, rmHost);
        }
        return renewer;
    }

    @Private
    @VisibleForTesting
    // TODO: 17/3/25 by zmyer
    protected boolean isSecurityEnabled() {
        return UserGroupInformation.isSecurityEnabled();
    }

    @Override
    // TODO: 17/3/25 by zmyer
    public void failApplicationAttempt(ApplicationAttemptId attemptId)
        throws YarnException, IOException {
        LOG.info("Failing application attempt " + attemptId);
        //创建失败的应用提交请求对象
        FailApplicationAttemptRequest request = Records.newRecord(FailApplicationAttemptRequest.class);
        //为失败的应用提交请求对象设置应用运行id
        request.setApplicationAttemptId(attemptId);
        //向资源管理服务器提交失败的应用请求对象
        rmClient.failApplicationAttempt(request);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void killApplication(ApplicationId applicationId) throws YarnException, IOException {
        //杀死提交的应用对象
        killApplication(applicationId, null);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void killApplication(ApplicationId applicationId, String diagnostics) throws YarnException, IOException {
        //创建杀死应用请求对象
        KillApplicationRequest request = Records.newRecord(KillApplicationRequest.class);
        //为杀死应用请求对象设置应用id
        request.setApplicationId(applicationId);

        if (diagnostics != null) {
            request.setDiagnostics(diagnostics);
        }

        try {
            int pollCount = 0;
            long startTime = System.currentTimeMillis();

            while (true) {
                //向资源管理服务器提交杀死应用请求
                KillApplicationResponse response = rmClient.forceKillApplication(request);
                if (response.getIsKillCompleted()) {
                    LOG.info("Killed application " + applicationId);
                    break;
                }

                //计算花费的时间
                long elapsedMillis = System.currentTimeMillis() - startTime;
                if (enforceAsyncAPITimeout() && elapsedMillis >= this.asyncApiPollTimeoutMillis) {
                    throw new YarnException("Timed out while waiting for application "
                        + applicationId + " to be killed.");
                }

                if (++pollCount % 10 == 0) {
                    LOG.info("Waiting for application " + applicationId + " to be killed.");
                }
                //等待片刻
                Thread.sleep(asyncApiPollIntervalMillis);
            }
        } catch (InterruptedException e) {
            String msg = "Interrupted while waiting for application "
                + applicationId + " to be killed.";
            LOG.error(msg);
            throw new YarnException(msg, e);
        }
    }

    // TODO: 17/3/25 by zmyer
    @VisibleForTesting
    boolean enforceAsyncAPITimeout() {
        return asyncApiPollTimeoutMillis >= 0;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public ApplicationReport getApplicationReport(ApplicationId appId) throws YarnException, IOException {
        GetApplicationReportResponse response;
        try {
            //创建请求应用运行报告对象
            GetApplicationReportRequest request = Records.newRecord(GetApplicationReportRequest.class);
            //设置应用id
            request.setApplicationId(appId);
            //向资源管理服务器发送获取应用运行报告消息
            response = rmClient.getApplicationReport(request);
        } catch (ApplicationNotFoundException e) {
            if (!historyServiceEnabled) {
                // Just throw it as usual if historyService is not enabled.
                throw e;
            }
            //继续发送获取应用历史运行报告
            return historyClient.getApplicationReport(appId);
        }
        //返回应用运行报告
        return response.getApplicationReport();
    }

    // TODO: 17/3/25 by zmyer
    public org.apache.hadoop.security.token.Token<AMRMTokenIdentifier>
    getAMRMToken(ApplicationId appId) throws YarnException, IOException {
        //读取token
        Token token = getApplicationReport(appId).getAMRMToken();
        org.apache.hadoop.security.token.Token<AMRMTokenIdentifier> amrmToken =
            null;
        if (token != null) {
            amrmToken = ConverterUtils.convertFromYarn(token, (Text) null);
        }
        return amrmToken;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public List<ApplicationReport> getApplications() throws YarnException,
        IOException {
        //向资源管理服务器请求所有的运行应用对象
        return getApplications(null, null);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public List<ApplicationReport> getApplications(Set<String> applicationTypes)
        throws YarnException,
        IOException {
        //向资源管理服务器请求所有的运行应用对象
        return getApplications(applicationTypes, null);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public List<ApplicationReport> getApplications(EnumSet<YarnApplicationState> applicationStates)
        throws YarnException, IOException {
        //请求所有的应用对象
        return getApplications(null, applicationStates);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public List<ApplicationReport> getApplications(Set<String> applicationTypes,
        EnumSet<YarnApplicationState> applicationStates) throws YarnException, IOException {
        //获取应用列表请求
        GetApplicationsRequest request =
            GetApplicationsRequest.newInstance(applicationTypes, applicationStates);
        //向资源管理服务器请求获取应用列表
        GetApplicationsResponse response = rmClient.getApplications(request);
        //返回应答
        return response.getApplicationList();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public List<ApplicationReport> getApplications(Set<String> applicationTypes,
        EnumSet<YarnApplicationState> applicationStates,
        Set<String> applicationTags) throws YarnException, IOException {
        GetApplicationsRequest request = GetApplicationsRequest.newInstance(applicationTypes, applicationStates);
        request.setApplicationTags(applicationTags);
        GetApplicationsResponse response = rmClient.getApplications(request);
        return response.getApplicationList();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public List<ApplicationReport> getApplications(Set<String> queues,
        Set<String> users, Set<String> applicationTypes,
        EnumSet<YarnApplicationState> applicationStates) throws YarnException,
        IOException {
        GetApplicationsRequest request = GetApplicationsRequest.newInstance(applicationTypes, applicationStates);
        request.setQueues(queues);
        request.setUsers(users);
        GetApplicationsResponse response = rmClient.getApplications(request);
        return response.getApplicationList();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public List<ApplicationReport> getApplications(
        GetApplicationsRequest request) throws YarnException, IOException {
        GetApplicationsResponse response = rmClient.getApplications(request);
        return response.getApplicationList();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public YarnClusterMetrics getYarnClusterMetrics() throws YarnException, IOException {
        GetClusterMetricsRequest request =
            Records.newRecord(GetClusterMetricsRequest.class);
        GetClusterMetricsResponse response = rmClient.getClusterMetrics(request);
        return response.getClusterMetrics();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public List<NodeReport> getNodeReports(NodeState... states) throws YarnException,
        IOException {
        EnumSet<NodeState> statesSet = (states.length == 0) ?
            EnumSet.allOf(NodeState.class) : EnumSet.noneOf(NodeState.class);
        Collections.addAll(statesSet, states);
        GetClusterNodesRequest request = GetClusterNodesRequest.newInstance(statesSet);
        GetClusterNodesResponse response = rmClient.getClusterNodes(request);
        return response.getNodeReports();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public Token getRMDelegationToken(Text renewer)
        throws YarnException, IOException {
    /* get the token from RM */
        GetDelegationTokenRequest rmDTRequest = Records.newRecord(GetDelegationTokenRequest.class);
        rmDTRequest.setRenewer(renewer.toString());
        GetDelegationTokenResponse response = rmClient.getDelegationToken(rmDTRequest);
        return response.getRMDelegationToken();
    }

    // TODO: 17/3/25 by zmyer
    private GetQueueInfoRequest
    getQueueInfoRequest(String queueName, boolean includeApplications,
        boolean includeChildQueues, boolean recursive) {
        GetQueueInfoRequest request = Records.newRecord(GetQueueInfoRequest.class);
        request.setQueueName(queueName);
        request.setIncludeApplications(includeApplications);
        request.setIncludeChildQueues(includeChildQueues);
        request.setRecursive(recursive);
        return request;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public QueueInfo getQueueInfo(String queueName) throws YarnException,
        IOException {
        GetQueueInfoRequest request = getQueueInfoRequest(queueName, true, false, false);
        Records.newRecord(GetQueueInfoRequest.class);
        return rmClient.getQueueInfo(request).getQueueInfo();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public List<QueueUserACLInfo> getQueueAclsInfo() throws YarnException,
        IOException {
        GetQueueUserAclsInfoRequest request =
            Records.newRecord(GetQueueUserAclsInfoRequest.class);
        return rmClient.getQueueUserAcls(request).getUserAclsInfoList();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public List<QueueInfo> getAllQueues() throws YarnException, IOException {
        List<QueueInfo> queues = new ArrayList<QueueInfo>();

        QueueInfo rootQueue =
            rmClient.getQueueInfo(getQueueInfoRequest(ROOT, false, true, true)).getQueueInfo();
        getChildQueues(rootQueue, queues, true);
        return queues;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public List<QueueInfo> getRootQueueInfos() throws YarnException, IOException {
        List<QueueInfo> queues = new ArrayList<QueueInfo>();

        QueueInfo rootQueue = rmClient.getQueueInfo(getQueueInfoRequest(ROOT, false, true, true)).getQueueInfo();
        getChildQueues(rootQueue, queues, false);
        return queues;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public List<QueueInfo> getChildQueueInfos(String parent)
        throws YarnException, IOException {
        List<QueueInfo> queues = new ArrayList<QueueInfo>();

        QueueInfo parentQueue =
            rmClient.getQueueInfo(getQueueInfoRequest(parent, false, true, false))
                .getQueueInfo();
        getChildQueues(parentQueue, queues, true);
        return queues;
    }

    // TODO: 17/3/25 by zmyer
    private void getChildQueues(QueueInfo parent, List<QueueInfo> queues,
        boolean recursive) {
        List<QueueInfo> childQueues = parent.getChildQueues();

        for (QueueInfo child : childQueues) {
            queues.add(child);
            if (recursive) {
                getChildQueues(child, queues, recursive);
            }
        }
    }

    // TODO: 17/3/25 by zmyer
    @Private
    @VisibleForTesting
    public void setRMClient(ApplicationClientProtocol rmClient) {
        this.rmClient = rmClient;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public ApplicationAttemptReport getApplicationAttemptReport(
        ApplicationAttemptId appAttemptId) throws YarnException, IOException {
        try {
            GetApplicationAttemptReportRequest request = Records.newRecord(GetApplicationAttemptReportRequest.class);
            request.setApplicationAttemptId(appAttemptId);
            GetApplicationAttemptReportResponse response = rmClient.getApplicationAttemptReport(request);
            return response.getApplicationAttemptReport();
        } catch (YarnException e) {
            if (!historyServiceEnabled) {
                // Just throw it as usual if historyService is not enabled.
                throw e;
            }
            // Even if history-service is enabled, treat all exceptions still the same
            // except the following
            if (e.getClass() != ApplicationNotFoundException.class) {
                throw e;
            }
            return historyClient.getApplicationAttemptReport(appAttemptId);
        }
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public List<ApplicationAttemptReport> getApplicationAttempts(
        ApplicationId appId) throws YarnException, IOException {
        try {
            GetApplicationAttemptsRequest request = Records
                .newRecord(GetApplicationAttemptsRequest.class);
            request.setApplicationId(appId);
            GetApplicationAttemptsResponse response = rmClient.getApplicationAttempts(request);
            return response.getApplicationAttemptList();
        } catch (YarnException e) {
            if (!historyServiceEnabled) {
                // Just throw it as usual if historyService is not enabled.
                throw e;
            }
            // Even if history-service is enabled, treat all exceptions still the same
            // except the following
            if (e.getClass() != ApplicationNotFoundException.class) {
                throw e;
            }
            return historyClient.getApplicationAttempts(appId);
        }
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public ContainerReport getContainerReport(ContainerId containerId) throws YarnException, IOException {
        try {
            GetContainerReportRequest request = Records.newRecord(GetContainerReportRequest.class);
            request.setContainerId(containerId);
            GetContainerReportResponse response = rmClient.getContainerReport(request);
            return response.getContainerReport();
        } catch (YarnException e) {
            if (!historyServiceEnabled) {
                // Just throw it as usual if historyService is not enabled.
                throw e;
            }
            // Even if history-service is enabled, treat all exceptions still the same
            // except the following
            if (e.getClass() != ApplicationNotFoundException.class
                && e.getClass() != ContainerNotFoundException.class) {
                throw e;
            }
            return historyClient.getContainerReport(containerId);
        }
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public List<ContainerReport> getContainers(ApplicationAttemptId applicationAttemptId) throws YarnException,
        IOException {
        List<ContainerReport> containersForAttempt = new ArrayList<ContainerReport>();
        boolean appNotFoundInRM = false;
        try {
            //获取容器请求对象
            GetContainersRequest request = Records.newRecord(GetContainersRequest.class);
            //设置应用执行id
            request.setApplicationAttemptId(applicationAttemptId);
            //向资源管理服务器请求当前的应用运行的容器对象
            GetContainersResponse response = rmClient.getContainers(request);
            //将返回的容器对象列表注册到结果集合中
            containersForAttempt.addAll(response.getContainerList());
        } catch (YarnException e) {
            if (e.getClass() != ApplicationNotFoundException.class
                || !historyServiceEnabled) {
                // If Application is not in RM and history service is enabled then we
                // need to check with history service else throw exception.
                throw e;
            }
            appNotFoundInRM = true;
        }

        if (historyServiceEnabled) {
            // Check with AHS even if found in RM because to capture info of finished
            // containers also
            List<ContainerReport> containersListFromAHS = null;
            try {
                containersListFromAHS = historyClient.getContainers(applicationAttemptId);
            } catch (IOException e) {
                // History service access might be enabled but system metrics publisher
                // is disabled hence app not found exception is possible
                if (appNotFoundInRM) {
                    // app not found in bothM and RM then propagate the exception.
                    throw e;
                }
            }

            if (null != containersListFromAHS && containersListFromAHS.size() > 0) {
                // remove duplicates

                Set<ContainerId> containerIdsToBeKeptFromAHS = new HashSet<ContainerId>();
                for (ContainerReport containersListFromAH : containersListFromAHS) {
                    containerIdsToBeKeptFromAHS.add(containersListFromAH.getContainerId());
                }

                for (ContainerReport tmp : containersForAttempt) {
                    containerIdsToBeKeptFromAHS.remove(tmp.getContainerId());
                    // Remove containers from AHS as container from RM will have latest
                    // information
                }

                if (containerIdsToBeKeptFromAHS.size() > 0
                    && containersListFromAHS.size() != containerIdsToBeKeptFromAHS
                    .size()) {
                    for (ContainerReport containerReport : containersListFromAHS) {
                        if (containerIdsToBeKeptFromAHS.contains(containerReport
                            .getContainerId())) {
                            containersForAttempt.add(containerReport);
                        }
                    }
                } else if (containersListFromAHS.size() == containerIdsToBeKeptFromAHS
                    .size()) {
                    containersForAttempt.addAll(containersListFromAHS);
                }
            }
        }
        return containersForAttempt;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void moveApplicationAcrossQueues(ApplicationId appId,
        String queue) throws YarnException, IOException {
        MoveApplicationAcrossQueuesRequest request = MoveApplicationAcrossQueuesRequest.newInstance(appId, queue);
        rmClient.moveApplicationAcrossQueues(request);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public GetNewReservationResponse createReservation() throws YarnException, IOException {
        GetNewReservationRequest request = Records.newRecord(GetNewReservationRequest.class);
        return rmClient.getNewReservation(request);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public ReservationSubmissionResponse submitReservation(ReservationSubmissionRequest request)
        throws YarnException, IOException {
        return rmClient.submitReservation(request);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public ReservationUpdateResponse updateReservation(
        ReservationUpdateRequest request) throws YarnException, IOException {
        return rmClient.updateReservation(request);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public ReservationDeleteResponse deleteReservation(
        ReservationDeleteRequest request) throws YarnException, IOException {
        return rmClient.deleteReservation(request);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public ReservationListResponse listReservations(
        ReservationListRequest request) throws YarnException, IOException {
        return rmClient.listReservations(request);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public Map<NodeId, Set<NodeLabel>> getNodeToLabels() throws YarnException,
        IOException {
        return rmClient.getNodeToLabels(GetNodesToLabelsRequest.newInstance())
            .getNodeToLabels();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public Map<NodeLabel, Set<NodeId>> getLabelsToNodes() throws YarnException,
        IOException {
        return rmClient.getLabelsToNodes(GetLabelsToNodesRequest.newInstance())
            .getLabelsToNodes();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public Map<NodeLabel, Set<NodeId>> getLabelsToNodes(Set<String> labels)
        throws YarnException, IOException {
        return rmClient.getLabelsToNodes(GetLabelsToNodesRequest.newInstance(labels)).getLabelsToNodes();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public List<NodeLabel> getClusterNodeLabels() throws YarnException, IOException {
        return rmClient.getClusterNodeLabels(GetClusterNodeLabelsRequest.newInstance()).getNodeLabelList();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public Priority updateApplicationPriority(ApplicationId applicationId,
        Priority priority) throws YarnException, IOException {
        UpdateApplicationPriorityRequest request = UpdateApplicationPriorityRequest.newInstance(applicationId,
            priority);
        return rmClient.updateApplicationPriority(request).getApplicationPriority();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void signalToContainer(ContainerId containerId, SignalContainerCommand command)
        throws YarnException, IOException {
        LOG.info("Signalling container " + containerId + " with command " + command);
        SignalContainerRequest request = SignalContainerRequest.newInstance(containerId, command);
        rmClient.signalToContainer(request);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public UpdateApplicationTimeoutsResponse updateApplicationTimeouts(UpdateApplicationTimeoutsRequest request)
        throws YarnException, IOException {
        return rmClient.updateApplicationTimeouts(request);
    }
}
