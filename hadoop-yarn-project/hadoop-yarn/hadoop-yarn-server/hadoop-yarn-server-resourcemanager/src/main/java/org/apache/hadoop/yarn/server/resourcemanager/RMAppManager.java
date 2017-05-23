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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.security.AccessRequest;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRecoverEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

/**
 * This class manages the list of applications for the resource manager.
 */
// TODO: 17/3/22 by zmyer
public class RMAppManager implements EventHandler<RMAppManagerEvent>, Recoverable {

    private static final Log LOG = LogFactory.getLog(RMAppManager.class);
    //内存中保留的最大已完成的APP数量
    private int maxCompletedAppsInMemory;
    //状态存储对象中能够保留的最大的已完成的app数量
    private int maxCompletedAppsInStateStore;
    //状态对象中保留的已完成的app的数量
    protected int completedAppsInStateStore = 0;
    private LinkedList<ApplicationId> completedApps = new LinkedList<ApplicationId>();
    //rm执行上下文对象
    private final RMContext rmContext;
    //app master服务对象
    private final ApplicationMasterService masterService;
    //yarn调度对象
    private final YarnScheduler scheduler;
    //应用控制管理器
    private final ApplicationACLsManager applicationACLsManager;
    //配置对象
    private Configuration conf;
    //yarn权限提供者对象
    private YarnAuthorizationProvider authorizer;

    // TODO: 17/4/2 by zmyer
    public RMAppManager(RMContext context,
        YarnScheduler scheduler, ApplicationMasterService masterService,
        ApplicationACLsManager applicationACLsManager, Configuration conf) {
        this.rmContext = context;
        this.scheduler = scheduler;
        this.masterService = masterService;
        this.applicationACLsManager = applicationACLsManager;
        this.conf = conf;
        this.maxCompletedAppsInMemory = conf.getInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS,
            YarnConfiguration.DEFAULT_RM_MAX_COMPLETED_APPLICATIONS);
        this.maxCompletedAppsInStateStore = conf.getInt(YarnConfiguration.RM_STATE_STORE_MAX_COMPLETED_APPLICATIONS,
                this.maxCompletedAppsInMemory);
        if (this.maxCompletedAppsInStateStore > this.maxCompletedAppsInMemory) {
            this.maxCompletedAppsInStateStore = this.maxCompletedAppsInMemory;
        }
        this.authorizer = YarnAuthorizationProvider.getInstance(conf);
    }

    /**
     * This class is for logging the application summary.
     */
    // TODO: 17/4/2 by zmyer
    static class ApplicationSummary {
        static final Log LOG = LogFactory.getLog(ApplicationSummary.class);

        // Escape sequences
        static final char EQUALS = '=';
        static final char[] charsToEscape = {StringUtils.COMMA, EQUALS, StringUtils.ESCAPE_CHAR};

        // TODO: 17/4/3 by zmyer
        static class SummaryBuilder {
            final StringBuilder buffer = new StringBuilder();

            // A little optimization for a very common case
            SummaryBuilder add(String key, long value) {
                return _add(key, Long.toString(value));
            }

            <T> SummaryBuilder add(String key, T value) {
                String escapedString = StringUtils.escapeString(String.valueOf(value),
                    StringUtils.ESCAPE_CHAR, charsToEscape).replaceAll("\n", "\\\\n")
                    .replaceAll("\r", "\\\\r");
                return _add(key, escapedString);
            }

            SummaryBuilder add(SummaryBuilder summary) {
                if (buffer.length() > 0)
                    buffer.append(StringUtils.COMMA);
                buffer.append(summary.buffer);
                return this;
            }

            SummaryBuilder _add(String key, String value) {
                if (buffer.length() > 0)
                    buffer.append(StringUtils.COMMA);
                buffer.append(key).append(EQUALS).append(value);
                return this;
            }

            @Override public String toString() {
                return buffer.toString();
            }
        }

        /**
         * create a summary of the application's runtime.
         *
         * @param app {@link RMApp} whose summary is to be created, cannot be <code>null</code>.
         */
        // TODO: 17/4/2 by zmyer
        public static SummaryBuilder createAppSummary(RMApp app) {
            String trackingUrl = "N/A";
            String host = "N/A";
            RMAppAttempt attempt = app.getCurrentAppAttempt();
            if (attempt != null) {
                trackingUrl = attempt.getTrackingUrl();
                host = attempt.getHost();
            }
            RMAppMetrics metrics = app.getRMAppMetrics();
            SummaryBuilder summary = new SummaryBuilder()
                .add("appId", app.getApplicationId())
                .add("name", app.getName())
                .add("user", app.getUser())
                .add("queue", app.getQueue())
                .add("state", app.getState())
                .add("trackingUrl", trackingUrl)
                .add("appMasterHost", host)
                .add("startTime", app.getStartTime())
                .add("finishTime", app.getFinishTime())
                .add("finalStatus", app.getFinalApplicationStatus())
                .add("memorySeconds", metrics.getMemorySeconds())
                .add("vcoreSeconds", metrics.getVcoreSeconds())
                .add("preemptedMemorySeconds", metrics.getPreemptedMemorySeconds())
                .add("preemptedVcoreSeconds", metrics.getPreemptedVcoreSeconds())
                .add("preemptedAMContainers", metrics.getNumAMContainersPreempted())
                .add("preemptedNonAMContainers", metrics.getNumNonAMContainersPreempted())
                .add("preemptedResources", metrics.getResourcePreempted())
                .add("applicationType", app.getApplicationType());
            return summary;
        }

        /**
         * Log a summary of the application's runtime.
         *
         * @param app {@link RMApp} whose summary is to be logged
         */
        // TODO: 17/4/2 by zmyer
        public static void logAppSummary(RMApp app) {
            if (app != null) {
                LOG.info(createAppSummary(app));
            }
        }
    }

    // TODO: 17/4/2 by zmyer
    @VisibleForTesting
    public void logApplicationSummary(ApplicationId appId) {
        ApplicationSummary.logAppSummary(rmContext.getRMApps().get(appId));
    }

    // TODO: 17/4/2 by zmyer
    protected synchronized int getCompletedAppsListSize() {
        return this.completedApps.size();
    }

    // TODO: 17/4/2 by zmyer
    protected synchronized void finishApplication(ApplicationId applicationId) {
        if (applicationId == null) {
            LOG.error("RMAppManager received completed appId of null, skipping");
        } else {
            // Inform the DelegationTokenRenewer
            if (UserGroupInformation.isSecurityEnabled()) {
                rmContext.getDelegationTokenRenewer().applicationFinished(applicationId);
            }

            completedApps.add(applicationId);
            completedAppsInStateStore++;
            writeAuditLog(applicationId);
        }
    }

    // TODO: 17/4/2 by zmyer
    protected void writeAuditLog(ApplicationId appId) {
        RMApp app = rmContext.getRMApps().get(appId);
        String operation = "UNKONWN";
        boolean success = false;
        switch (app.getState()) {
            case FAILED:
                operation = AuditConstants.FINISH_FAILED_APP;
                break;
            case FINISHED:
                operation = AuditConstants.FINISH_SUCCESS_APP;
                success = true;
                break;
            case KILLED:
                operation = AuditConstants.FINISH_KILLED_APP;
                success = true;
                break;
            default:
                break;
        }

        if (success) {
            RMAuditLogger.logSuccess(app.getUser(), operation, "RMAppManager", app.getApplicationId());
        } else {
            StringBuilder diag = app.getDiagnostics();
            String msg = diag == null ? null : diag.toString();
            RMAuditLogger.logFailure(app.getUser(), operation, msg, "RMAppManager",
                "App failed with state: " + app.getState(), appId);
        }
    }

    /*
     * check to see if hit the limit for max # completed apps kept
     */
    // TODO: 17/4/2 by zmyer
    protected synchronized void checkAppNumCompletedLimit() {
        // check apps kept in state store.
        while (completedAppsInStateStore > this.maxCompletedAppsInStateStore) {
            ApplicationId removeId = completedApps.get(completedApps.size() - completedAppsInStateStore);
            RMApp removeApp = rmContext.getRMApps().get(removeId);
            LOG.info("Max number of completed apps kept in state store met:"
                + " maxCompletedAppsInStateStore = " + maxCompletedAppsInStateStore
                + ", removing app " + removeApp.getApplicationId()
                + " from state store.");
            rmContext.getStateStore().removeApplication(removeApp);
            completedAppsInStateStore--;
        }

        // check apps kept in memorty.
        while (completedApps.size() > this.maxCompletedAppsInMemory) {
            ApplicationId removeId = completedApps.remove();
            LOG.info("Application should be expired, max number of completed apps"
                + " kept in memory met: maxCompletedAppsInMemory = "
                + this.maxCompletedAppsInMemory + ", removing app " + removeId
                + " from memory: ");
            rmContext.getRMApps().remove(removeId);
            this.applicationACLsManager.removeApplication(removeId);
        }
    }

    // TODO: 17/4/2 by zmyer
    @SuppressWarnings("unchecked")
    protected void submitApplication(ApplicationSubmissionContext submissionContext, long submitTime,
        String user) throws YarnException {
        //从提交上下文对象中读取提交的application id
        ApplicationId applicationId = submissionContext.getApplicationId();
        // Passing start time as -1. It will be eventually set in RMAppImpl
        // constructor.
        //创建RM端保存的application对象
        RMAppImpl app = createAndPopulateNewRMApp(submissionContext, submitTime, user, false, -1);
        RMAppImpl application = app;
        Credentials credentials;
        try {
            //解析认证信息
            credentials = parseCredentials(submissionContext);
            if (UserGroupInformation.isSecurityEnabled()) {
                //开始将创建的应用对象以异步的方式加入到待执行的列表中
                this.rmContext.getDelegationTokenRenewer().addApplicationAsync(applicationId, credentials,
                    submissionContext.getCancelTokensWhenComplete(), application.getUser());
            } else {
                // Dispatcher is not yet started at this time, so these START events
                // enqueued should be guaranteed to be first processed when dispatcher
                // gets started.
                //开始向事件分发器发送start事件
                this.rmContext.getDispatcher().getEventHandler().handle(new RMAppEvent(applicationId, RMAppEventType.START));
            }
        } catch (Exception e) {
            LOG.warn("Unable to parse credentials.", e);
            // Sending APP_REJECTED is fine, since we assume that the
            // RMApp is in NEW state and thus we haven't yet informed the
            // scheduler about the existence of the application
            assert RMAppState.NEW == application.getState();
            //开始向事件分发器对象发送reject事件
            this.rmContext.getDispatcher().getEventHandler()
                .handle(new RMAppEvent(applicationId, RMAppEventType.APP_REJECTED, e.getMessage()));
            throw RPCUtil.getRemoteException(e);
        }
    }

    // TODO: 17/4/3 by zmyer
    protected void recoverApplication(ApplicationStateData appState, RMState rmState) throws Exception {
        ApplicationSubmissionContext appContext = appState.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        // create and recover app.
        RMAppImpl application = createAndPopulateNewRMApp(appContext, appState.getSubmitTime(),
            appState.getUser(), true, appState.getStartTime());

        application.handle(new RMAppRecoverEvent(appId, rmState));
    }

    // TODO: 17/4/2 by zmyer
    private RMAppImpl createAndPopulateNewRMApp(ApplicationSubmissionContext submissionContext,
        long submitTime, String user, boolean isRecovery, long startTime) throws YarnException {
        // Do queue mapping
        if (!isRecovery) {
            if (rmContext.getQueuePlacementManager() != null) {
                // We only do queue mapping when it's a new application
                rmContext.getQueuePlacementManager().placeApplication(submissionContext, user);
            }
        }

        //应用id
        ApplicationId applicationId = submissionContext.getApplicationId();
        //资源请求对象
        ResourceRequest amReq = validateAndCreateResourceRequest(submissionContext, isRecovery);

        // Verify and get the update application priority and set back to
        // submissionContext
        //应用优先级
        Priority appPriority = scheduler.checkAndGetApplicationPriority(
            submissionContext.getPriority(), user, submissionContext.getQueue(), applicationId);
        //设置应用提交上下文对象的优先级
        submissionContext.setPriority(appPriority);

        UserGroupInformation userUgi = UserGroupInformation.createRemoteUser(user);
        // Since FairScheduler queue mapping is done inside scheduler,
        // if FairScheduler is used and the queue doesn't exist, we should not
        // fail here because queue will be created inside FS. Ideally, FS queue
        // mapping should be done outside scheduler too like CS.
        // For now, exclude FS for the acl check.
        if (!isRecovery && YarnConfiguration.isAclEnabled(conf) && scheduler instanceof CapacityScheduler) {
            //队列名称
            String queueName = submissionContext.getQueue();
            //应用名称
            String appName = submissionContext.getApplicationName();
            //容量调度队列
            CSQueue csqueue = ((CapacityScheduler) scheduler).getQueue(queueName);
            if (null != csqueue && !authorizer.checkPermission(
                new AccessRequest(csqueue.getPrivilegedEntity(), userUgi,
                    SchedulerUtils.toAccessType(QueueACL.SUBMIT_APPLICATIONS),
                    applicationId.toString(), appName, Server.getRemoteAddress(),
                    null)) && !authorizer.checkPermission(
                new AccessRequest(csqueue.getPrivilegedEntity(), userUgi,
                    SchedulerUtils.toAccessType(QueueACL.ADMINISTER_QUEUE),
                    applicationId.toString(), appName, Server.getRemoteAddress(),
                    null))) {
                throw RPCUtil.getRemoteException(new AccessControlException(
                    "User " + user + " does not have permission to submit "
                        + applicationId + " to queue " + submissionContext.getQueue()));
            }
        }

        // fail the submission if configured application timeout value is invalid
        //验证应用的超时时间
        RMServerUtils.validateApplicationTimeouts(submissionContext.getApplicationTimeouts());

        // Create RMApp
        //创建app对象
        RMAppImpl application = new RMAppImpl(applicationId, rmContext, this.conf,
            submissionContext.getApplicationName(), user,
            submissionContext.getQueue(),
            submissionContext, this.scheduler, this.masterService,
            submitTime, submissionContext.getApplicationType(),
            submissionContext.getApplicationTags(), amReq, startTime);
        // Concurrent app submissions with same applicationId will fail here
        // Concurrent app submissions with different applicationIds will not
        // influence each other
        //将创建的app注册到app列表中
        if (rmContext.getRMApps().putIfAbsent(applicationId, application) != null) {
            String message = "Application with id " + applicationId + " is already present! Cannot add a duplicate!";
            LOG.warn(message);
            throw new YarnException(message);
        }

        if (YarnConfiguration.timelineServiceV2Enabled(conf)) {
            // Start timeline collector for the submitted app
            //启动时间线收集对象
            application.startTimelineCollector();
        }
        // Inform the ACLs Manager
        this.applicationACLsManager.addApplication(applicationId,
            submissionContext.getAMContainerSpec().getApplicationACLs());
        String appViewACLs = submissionContext.getAMContainerSpec()
            .getApplicationACLs().get(ApplicationAccessType.VIEW_APP);
        rmContext.getSystemMetricsPublisher().appACLsUpdated(application, appViewACLs, System.currentTimeMillis());
        //返回app对象
        return application;
    }

    // TODO: 17/4/3 by zmyer
    private ResourceRequest validateAndCreateResourceRequest(
        ApplicationSubmissionContext submissionContext, boolean isRecovery)
        throws InvalidResourceRequestException {
        // Validation of the ApplicationSubmissionContext needs to be completed
        // here. Only those fields that are dependent on RM's configuration are
        // checked here as they have to be validated whether they are part of new
        // submission or just being recovered.

        // Check whether AM resource requirements are within required limits
        if (!submissionContext.getUnmanagedAM()) {
            //读取资源请求对象
            ResourceRequest amReq = submissionContext.getAMContainerResourceRequest();
            if (amReq == null) {
                //创建资源请求对象
                amReq = BuilderUtils.newResourceRequest(RMAppAttemptImpl.AM_CONTAINER_PRIORITY,
                    ResourceRequest.ANY, submissionContext.getResource(), 1);
            }

            // set label expression for AM container
            if (null == amReq.getNodeLabelExpression()) {
                amReq.setNodeLabelExpression(submissionContext.getNodeLabelExpression());
            }

            try {
                SchedulerUtils.normalizeAndValidateRequest(amReq,
                    scheduler.getMaximumResourceCapability(),
                    submissionContext.getQueue(), scheduler, isRecovery, rmContext);
            } catch (InvalidResourceRequestException e) {
                LOG.warn("RM app submission failed in validating AM resource request"
                    + " for application " + submissionContext.getApplicationId(), e);
                throw e;
            }

            scheduler.normalizeRequest(amReq);
            return amReq;
        }

        return null;
    }

    // TODO: 17/4/3 by zmyer
    protected Credentials parseCredentials(ApplicationSubmissionContext application) throws IOException {
        Credentials credentials = new Credentials();
        DataInputByteBuffer dibb = new DataInputByteBuffer();
        ByteBuffer tokens = application.getAMContainerSpec().getTokens();
        if (tokens != null) {
            dibb.reset(tokens);
            credentials.readTokenStorageStream(dibb);
            tokens.rewind();
        }
        return credentials;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public void recover(RMState state) throws Exception {
        //读取状态存储对象
        RMStateStore store = rmContext.getStateStore();
        assert store != null;
        // recover applications
        //读取app的状态存储数据
        Map<ApplicationId, ApplicationStateData> appStates = state.getApplicationState();
        LOG.info("Recovering " + appStates.size() + " applications");

        int count = 0;
        try {
            for (ApplicationStateData appState : appStates.values()) {
                //恢复应用的状态信息
                recoverApplication(appState, state);
                count += 1;
            }
        } finally {
            LOG.info("Successfully recovered " + count + " out of "
                + appStates.size() + " applications");
        }
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public void handle(RMAppManagerEvent event) {
        //应用id
        ApplicationId applicationId = event.getApplicationId();
        LOG.debug("RMAppManager processing event for " + applicationId + " of type " + event.getType());
        switch (event.getType()) {
            case APP_COMPLETED:
                //将完成的app插入到完成列表中
                finishApplication(applicationId);
                logApplicationSummary(applicationId);
                //检查内存中完成的app的数量
                checkAppNumCompletedLimit();
                break;
            case APP_MOVE:
                // moveAllApps from scheduler will fire this event for each of
                // those applications which needed to be moved to a new queue.
                // Use the standard move application api to do the same.
                try {
                    moveApplicationAcrossQueue(applicationId, event.getTargetQueueForMove());
                } catch (YarnException e) {
                    LOG.warn("Move Application has failed: " + e.getMessage());
                }
                break;
            default:
                LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
        }
    }

    // TODO: 17/4/3 by zmyer
    // transaction method.
    public void updateApplicationTimeout(RMApp app,
        Map<ApplicationTimeoutType, String> newTimeoutInISO8601Format)
        throws YarnException {
        ApplicationId applicationId = app.getApplicationId();
        synchronized (applicationId) {
            if (app.isAppInCompletedStates()) {
                return;
            }

            Map<ApplicationTimeoutType, Long> newExpireTime = RMServerUtils
                .validateISO8601AndConvertToLocalTimeEpoch(newTimeoutInISO8601Format);

            SettableFuture<Object> future = SettableFuture.create();

            Map<ApplicationTimeoutType, Long> currentExpireTimeouts =
                app.getApplicationTimeouts();
            currentExpireTimeouts.putAll(newExpireTime);

            ApplicationStateData appState =
                ApplicationStateData.newInstance(app.getSubmitTime(),
                    app.getStartTime(), app.getApplicationSubmissionContext(),
                    app.getUser(), app.getCallerContext());
            appState.setApplicationTimeouts(currentExpireTimeouts);

            // update to state store. Though it synchronous call, update via future to
            // know any exception has been set. It is required because in non-HA mode,
            // state-store errors are skipped.
            this.rmContext.getStateStore()
                .updateApplicationStateSynchronously(appState, false, future);

            Futures.get(future, YarnException.class);

            // update in-memory
            ((RMAppImpl) app).updateApplicationTimeout(newExpireTime);
        }
    }

    /**
     * updateApplicationPriority will invoke scheduler api to update the
     * new priority to RM and StateStore.
     *
     * @param applicationId Application Id
     * @param newAppPriority proposed new application priority
     * @throws YarnException Handle exceptions
     */
    // TODO: 17/4/3 by zmyer
    public void updateApplicationPriority(ApplicationId applicationId,
        Priority newAppPriority) throws YarnException {
        RMApp app = this.rmContext.getRMApps().get(applicationId);

        synchronized (applicationId) {
            if (app.isAppInCompletedStates()) {
                return;
            }

            // Create a future object to capture exceptions from StateStore.
            SettableFuture<Object> future = SettableFuture.create();

            // Invoke scheduler api to update priority in scheduler and to
            // State Store.
            Priority appPriority = rmContext.getScheduler()
                .updateApplicationPriority(newAppPriority, applicationId, future);

            if (app.getApplicationPriority().equals(appPriority)) {
                return;
            }

            Futures.get(future, YarnException.class);

            // update in-memory
            ((RMAppImpl) app).setApplicationPriority(appPriority);
        }

        // Update the changed application state to timeline server
        rmContext.getSystemMetricsPublisher().appUpdated(app,
            System.currentTimeMillis());
    }

    /**
     * moveToQueue will invoke scheduler api to perform move queue operation.
     *
     * @param applicationId Application Id.
     * @param targetQueue Target queue to which this app has to be moved.
     * @throws YarnException Handle exceptions.
     */
    // TODO: 17/4/3 by zmyer
    public void moveApplicationAcrossQueue(ApplicationId applicationId, String targetQueue)
        throws YarnException {
        //读取每个app对象
        RMApp app = this.rmContext.getRMApps().get(applicationId);

        // Capacity scheduler will directly follow below approach.
        // 1. Do a pre-validate check to ensure that changes are fine.
        // 2. Update this information to state-store
        // 3. Perform real move operation and update in-memory data structures.
        synchronized (applicationId) {
            //如果app已经完成了,则直接退出
            if (app.isAppInCompletedStates()) {
                return;
            }

            //读取app对应的队列名称
            String sourceQueue = app.getQueue();
            // 1. pre-validate move application request to check for any access
            // violations or other errors. If there are any violations, YarnException
            // will be thrown.
            rmContext.getScheduler().preValidateMoveApplication(applicationId, targetQueue);

            // 2. Update to state store with new queue and throw exception is failed.
            updateAppDataToStateStore(targetQueue, app, false);

            // 3. Perform the real move application
            String queue = "";
            try {
                queue = rmContext.getScheduler().moveApplication(applicationId, targetQueue);
            } catch (YarnException e) {
                // Revert to source queue since in-memory move has failed. Chances
                // of this is very rare as we have already done the pre-validation.
                updateAppDataToStateStore(sourceQueue, app, true);
                throw e;
            }

            // update in-memory
            if (queue != null && !queue.isEmpty()) {
                app.setQueue(queue);
            }
        }
        rmContext.getSystemMetricsPublisher().appUpdated(app, System.currentTimeMillis());
    }

    // TODO: 17/4/3 by zmyer
    private void updateAppDataToStateStore(String queue, RMApp app,
        boolean toSuppressException) throws YarnException {
        // Create a future object to capture exceptions from StateStore.
        SettableFuture<Object> future = SettableFuture.create();

        // Update new queue in Submission Context to update to StateStore.
        app.getApplicationSubmissionContext().setQueue(queue);

        ApplicationStateData appState = ApplicationStateData.newInstance(
            app.getSubmitTime(), app.getStartTime(),
            app.getApplicationSubmissionContext(), app.getUser(),
            app.getCallerContext());
        appState.setApplicationTimeouts(app.getApplicationTimeouts());
        rmContext.getStateStore().updateApplicationStateSynchronously(appState,
            false, future);

        try {
            Futures.get(future, YarnException.class);
        } catch (YarnException ex) {
            if (!toSuppressException) {
                throw ex;
            }
            LOG.error("Statestore update failed for move application '"
                + app.getApplicationId() + "' to queue '" + queue
                + "' with below exception:" + ex.getMessage());
        }
    }
}
