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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeout;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.resourcemanager.ApplicationMasterService;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.blacklist.BlacklistManager;
import org.apache.hadoop.yarn.server.resourcemanager.blacklist.DisabledBlacklistManager;
import org.apache.hadoop.yarn.server.resourcemanager.blacklist.SimpleBlacklistManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppNodeUpdateEvent.RMAppNodeUpdateType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AggregateAppResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppStartAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.timelineservice.collector.AppLevelTimelineCollector;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

// TODO: 17/4/2 by zmyer
@SuppressWarnings({"rawtypes", "unchecked"})
public class RMAppImpl implements RMApp, Recoverable {
    private static final Log LOG = LogFactory.getLog(RMAppImpl.class);
    private static final String UNAVAILABLE = "N/A";
    private static final String UNLIMITED = "UNLIMITED";
    private static final long UNKNOWN = -1L;
    private static final EnumSet<RMAppState> COMPLETED_APP_STATES =
        EnumSet.of(RMAppState.FINISHED, RMAppState.FINISHING, RMAppState.FAILED,
            RMAppState.KILLED, RMAppState.FINAL_SAVING, RMAppState.KILLING);
    private static final String STATE_CHANGE_MESSAGE = "%s State change from %s to %s on event = %s";
    private static final String RECOVERY_MESSAGE =
        "Recovering app: %s with %d attempts and final state = %s";

    // Immutable fields
    //应用id
    private final ApplicationId applicationId;
    //rm执行上下文对象
    private final RMContext rmContext;
    //配置对象
    private final Configuration conf;
    private final String user;
    private final String name;
    //应用提交上下文对象
    private final ApplicationSubmissionContext submissionContext;
    //事件分发对象
    private final Dispatcher dispatcher;
    //yarn调度器
    private final YarnScheduler scheduler;
    //app master服务对象
    private final ApplicationMasterService masterService;
    private final StringBuilder diagnostics = new StringBuilder();
    //最大的尝试次数
    private final int maxAppAttempts;
    //读锁
    private final ReadLock readLock;
    //写锁
    private final WriteLock writeLock;
    //application尝试执行对象映射表
    private final Map<ApplicationAttemptId, RMAppAttempt> attempts = new LinkedHashMap<ApplicationAttemptId, RMAppAttempt>();
    //application提交时间
    private final long submitTime;
    //节点更新列表
    private final Set<RMNode> updatedNodes = new HashSet<RMNode>();
    //应用类型
    private final String applicationType;
    //应用标记列表
    private final Set<String> applicationTags;
    private final long attemptFailuresValidityInterval;
    private boolean amBlacklistingEnabled = false;
    private float blacklistDisableThreshold;
    //系统时钟
    private Clock systemClock;
    private boolean isNumAttemptsBeyondThreshold = false;

    // Mutable fields
    //应用开始处理时间
    private long startTime;
    //应用结束时间
    private long finishTime = 0;
    private long storedFinishTime = 0;
    private int firstAttemptIdInStateStore = 1;
    private int nextAttemptId = 1;
    private String collectorAddr;
    // This field isn't protected by readlock now.
    //当前application尝试执行对象
    private volatile RMAppAttempt currentAttempt;
    //队列名称
    private String queue;
    //事件处理对象
    private EventHandler handler;
    //应用完成状态转移对象
    private static final AppFinishedTransition FINISHED_TRANSITION = new AppFinishedTransition();
    //正在运行的节点列表
    private Set<NodeId> ranNodes = new ConcurrentSkipListSet<NodeId>();

    private final boolean logAggregationEnabled;
    private long logAggregationStartTime = 0;
    private final long logAggregationStatusTimeout;
    private final Map<NodeId, LogAggregationReport> logAggregationStatus = new HashMap<NodeId, LogAggregationReport>();
    private LogAggregationStatus logAggregationStatusForAppReport;
    private int logAggregationSucceed = 0;
    private int logAggregationFailed = 0;
    private Map<NodeId, List<String>> logAggregationDiagnosticsForNMs = new HashMap<NodeId, List<String>>();
    private Map<NodeId, List<String>> logAggregationFailureMessagesForNMs = new HashMap<NodeId, List<String>>();
    private final int maxLogAggregationDiagnosticsInMemory;
    private Map<ApplicationTimeoutType, Long> applicationTimeouts;

    // These states stored are only valid when app is at killing or final_saving.
    private RMAppState stateBeforeKilling;
    private RMAppState stateBeforeFinalSaving;
    private RMAppEvent eventCausingFinalSaving;
    private RMAppState targetedFinalState;
    private RMAppState recoveredFinalState;
    private ResourceRequest amReq;

    private CallerContext callerContext;

    Object transitionTodo;
    //应用优先级
    private Priority applicationPriority;
    //应用状态机工厂对象
    private static final StateMachineFactory<RMAppImpl, RMAppState, RMAppEventType, RMAppEvent> stateMachineFactory
        = new StateMachineFactory<RMAppImpl, RMAppState, RMAppEventType, RMAppEvent>(RMAppState.NEW)

        // Transitions from NEW state
        .addTransition(RMAppState.NEW, RMAppState.NEW, RMAppEventType.NODE_UPDATE,
            new RMAppNodeUpdateTransition())
        .addTransition(RMAppState.NEW, RMAppState.NEW, RMAppEventType.COLLECTOR_UPDATE,
            new RMAppCollectorUpdateTransition())
        .addTransition(RMAppState.NEW, RMAppState.NEW_SAVING, RMAppEventType.START,
            new RMAppNewlySavingTransition())
        .addTransition(RMAppState.NEW, EnumSet.of(RMAppState.SUBMITTED,
            RMAppState.ACCEPTED, RMAppState.FINISHED, RMAppState.FAILED,
            RMAppState.KILLED, RMAppState.FINAL_SAVING),
            RMAppEventType.RECOVER, new RMAppRecoveredTransition())
        .addTransition(RMAppState.NEW, RMAppState.KILLED, RMAppEventType.KILL,
            new AppKilledTransition())
        .addTransition(RMAppState.NEW, RMAppState.FINAL_SAVING, RMAppEventType.APP_REJECTED,
            new FinalSavingTransition(new AppRejectedTransition(), RMAppState.FAILED))

        // Transitions from NEW_SAVING state
        .addTransition(RMAppState.NEW_SAVING, RMAppState.NEW_SAVING, RMAppEventType.NODE_UPDATE,
            new RMAppNodeUpdateTransition())
        .addTransition(RMAppState.NEW_SAVING, RMAppState.NEW_SAVING, RMAppEventType.COLLECTOR_UPDATE,
            new RMAppCollectorUpdateTransition())
        .addTransition(RMAppState.NEW_SAVING, RMAppState.SUBMITTED, RMAppEventType.APP_NEW_SAVED,
            new AddApplicationToSchedulerTransition())
        .addTransition(RMAppState.NEW_SAVING, RMAppState.FINAL_SAVING, RMAppEventType.KILL,
            new FinalSavingTransition(new AppKilledTransition(), RMAppState.KILLED))
        .addTransition(RMAppState.NEW_SAVING, RMAppState.FINAL_SAVING, RMAppEventType.APP_REJECTED,
            new FinalSavingTransition(new AppRejectedTransition(), RMAppState.FAILED))

        // Transitions from SUBMITTED state
        .addTransition(RMAppState.SUBMITTED, RMAppState.SUBMITTED, RMAppEventType.NODE_UPDATE,
            new RMAppNodeUpdateTransition())
        .addTransition(RMAppState.SUBMITTED, RMAppState.SUBMITTED, RMAppEventType.COLLECTOR_UPDATE,
            new RMAppCollectorUpdateTransition())
        .addTransition(RMAppState.SUBMITTED, RMAppState.FINAL_SAVING, RMAppEventType.APP_REJECTED,
            new FinalSavingTransition(new AppRejectedTransition(), RMAppState.FAILED))
        .addTransition(RMAppState.SUBMITTED, RMAppState.ACCEPTED, RMAppEventType.APP_ACCEPTED,
            new StartAppAttemptTransition())
        .addTransition(RMAppState.SUBMITTED, RMAppState.FINAL_SAVING, RMAppEventType.KILL,
            new FinalSavingTransition(new AppKilledTransition(), RMAppState.KILLED))

        // Transitions from ACCEPTED state
        .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
            RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
        .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
            RMAppEventType.COLLECTOR_UPDATE, new RMAppCollectorUpdateTransition())
        .addTransition(RMAppState.ACCEPTED, RMAppState.RUNNING,
            RMAppEventType.ATTEMPT_REGISTERED, new RMAppStateUpdateTransition(
                YarnApplicationState.RUNNING))
        .addTransition(RMAppState.ACCEPTED,
            EnumSet.of(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING),
            // ACCEPTED state is possible to receive ATTEMPT_FAILED/ATTEMPT_FINISHED
            // event because RMAppRecoveredTransition is returning ACCEPTED state
            // directly and waiting for the previous AM to exit.
            RMAppEventType.ATTEMPT_FAILED,
            new AttemptFailedTransition(RMAppState.ACCEPTED))
        .addTransition(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING,
            RMAppEventType.ATTEMPT_FINISHED,
            new FinalSavingTransition(FINISHED_TRANSITION, RMAppState.FINISHED))
        .addTransition(RMAppState.ACCEPTED, RMAppState.KILLING,
            RMAppEventType.KILL, new KillAttemptTransition())
        .addTransition(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING,
            RMAppEventType.ATTEMPT_KILLED,
            new FinalSavingTransition(new AppKilledTransition(), RMAppState.KILLED))
        .addTransition(RMAppState.ACCEPTED, RMAppState.ACCEPTED,
            RMAppEventType.APP_RUNNING_ON_NODE,
            new AppRunningOnNodeTransition())

        // Transitions from RUNNING state
        .addTransition(RMAppState.RUNNING, RMAppState.RUNNING,
            RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
        .addTransition(RMAppState.RUNNING, RMAppState.RUNNING,
            RMAppEventType.COLLECTOR_UPDATE, new RMAppCollectorUpdateTransition())
        .addTransition(RMAppState.RUNNING, RMAppState.FINAL_SAVING,
            RMAppEventType.ATTEMPT_UNREGISTERED,
            new FinalSavingTransition(new AttemptUnregisteredTransition(),
                RMAppState.FINISHING, RMAppState.FINISHED))
        .addTransition(RMAppState.RUNNING, RMAppState.FINISHED,
            // UnManagedAM directly jumps to finished
            RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
        .addTransition(RMAppState.RUNNING, RMAppState.RUNNING,
            RMAppEventType.APP_RUNNING_ON_NODE,
            new AppRunningOnNodeTransition())
        .addTransition(RMAppState.RUNNING,
            EnumSet.of(RMAppState.ACCEPTED, RMAppState.FINAL_SAVING),
            RMAppEventType.ATTEMPT_FAILED,
            new AttemptFailedTransition(RMAppState.ACCEPTED))
        .addTransition(RMAppState.RUNNING, RMAppState.KILLING,
            RMAppEventType.KILL, new KillAttemptTransition())

        // Transitions from FINAL_SAVING state
        .addTransition(RMAppState.FINAL_SAVING,
            EnumSet.of(RMAppState.FINISHING, RMAppState.FAILED,
                RMAppState.KILLED, RMAppState.FINISHED), RMAppEventType.APP_UPDATE_SAVED,
            new FinalStateSavedTransition())
        .addTransition(RMAppState.FINAL_SAVING, RMAppState.FINAL_SAVING,
            RMAppEventType.ATTEMPT_FINISHED,
            new AttemptFinishedAtFinalSavingTransition())
        .addTransition(RMAppState.FINAL_SAVING, RMAppState.FINAL_SAVING,
            RMAppEventType.APP_RUNNING_ON_NODE,
            new AppRunningOnNodeTransition())
        .addTransition(RMAppState.FINAL_SAVING, RMAppState.FINAL_SAVING,
            RMAppEventType.COLLECTOR_UPDATE, new RMAppCollectorUpdateTransition())
        // ignorable transitions
        .addTransition(RMAppState.FINAL_SAVING, RMAppState.FINAL_SAVING,
            EnumSet.of(RMAppEventType.NODE_UPDATE, RMAppEventType.KILL, RMAppEventType.APP_NEW_SAVED))

        // Transitions from FINISHING state
        .addTransition(RMAppState.FINISHING, RMAppState.FINISHED,
            RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
        .addTransition(RMAppState.FINISHING, RMAppState.FINISHING,
            RMAppEventType.APP_RUNNING_ON_NODE,
            new AppRunningOnNodeTransition())
        .addTransition(RMAppState.FINISHING, RMAppState.FINISHING,
            RMAppEventType.COLLECTOR_UPDATE, new RMAppCollectorUpdateTransition())
        // ignorable transitions
        .addTransition(RMAppState.FINISHING, RMAppState.FINISHING,
            EnumSet.of(RMAppEventType.NODE_UPDATE,
                // ignore Kill/Move as we have already saved the final Finished state
                // in state store.
                RMAppEventType.KILL))

        // Transitions from KILLING state
        .addTransition(RMAppState.KILLING, RMAppState.KILLING,
            RMAppEventType.APP_RUNNING_ON_NODE,
            new AppRunningOnNodeTransition())
        .addTransition(RMAppState.KILLING, RMAppState.KILLING,
            RMAppEventType.COLLECTOR_UPDATE, new RMAppCollectorUpdateTransition())
        .addTransition(RMAppState.KILLING, RMAppState.FINAL_SAVING,
            RMAppEventType.ATTEMPT_KILLED,
            new FinalSavingTransition(
                new AppKilledTransition(), RMAppState.KILLED))
        .addTransition(RMAppState.KILLING, RMAppState.FINAL_SAVING,
            RMAppEventType.ATTEMPT_UNREGISTERED,
            new FinalSavingTransition(
                new AttemptUnregisteredTransition(),
                RMAppState.FINISHING, RMAppState.FINISHED))
        .addTransition(RMAppState.KILLING, RMAppState.FINISHED,
            // UnManagedAM directly jumps to finished
            RMAppEventType.ATTEMPT_FINISHED, FINISHED_TRANSITION)
        .addTransition(RMAppState.KILLING,
            EnumSet.of(RMAppState.FINAL_SAVING),
            RMAppEventType.ATTEMPT_FAILED,
            new AttemptFailedTransition(RMAppState.KILLING))

        .addTransition(RMAppState.KILLING, RMAppState.KILLING,
            EnumSet.of(RMAppEventType.NODE_UPDATE,
                RMAppEventType.ATTEMPT_REGISTERED,
                RMAppEventType.APP_UPDATE_SAVED,
                RMAppEventType.KILL))

        // Transitions from FINISHED state
        // ignorable transitions
        .addTransition(RMAppState.FINISHED, RMAppState.FINISHED,
            RMAppEventType.APP_RUNNING_ON_NODE,
            new AppRunningOnNodeTransition())
        .addTransition(RMAppState.FINISHED, RMAppState.FINISHED,
            EnumSet.of(RMAppEventType.NODE_UPDATE,
                RMAppEventType.ATTEMPT_UNREGISTERED,
                RMAppEventType.ATTEMPT_FINISHED,
                RMAppEventType.KILL))

        // Transitions from FAILED state
        // ignorable transitions
        .addTransition(RMAppState.FAILED, RMAppState.FAILED,
            RMAppEventType.APP_RUNNING_ON_NODE,
            new AppRunningOnNodeTransition())
        .addTransition(RMAppState.FAILED, RMAppState.FAILED,
            EnumSet.of(RMAppEventType.KILL, RMAppEventType.NODE_UPDATE))

        // Transitions from KILLED state
        // ignorable transitions
        .addTransition(RMAppState.KILLED, RMAppState.KILLED,
            RMAppEventType.APP_RUNNING_ON_NODE,
            new AppRunningOnNodeTransition())
        .addTransition(RMAppState.KILLED,
            RMAppState.KILLED,
            EnumSet.of(RMAppEventType.APP_ACCEPTED,
                RMAppEventType.APP_REJECTED, RMAppEventType.KILL,
                RMAppEventType.ATTEMPT_FINISHED, RMAppEventType.ATTEMPT_FAILED,
                RMAppEventType.NODE_UPDATE))
        .installTopology();

    //应用状态机
    private final StateMachine<RMAppState, RMAppEventType, RMAppEvent> stateMachine;
    private static final int DUMMY_APPLICATION_ATTEMPT_NUMBER = -1;
    private static final float MINIMUM_AM_BLACKLIST_THRESHOLD_VALUE = 0.0f;
    private static final float MAXIMUM_AM_BLACKLIST_THRESHOLD_VALUE = 1.0f;

    // TODO: 17/4/3 by zmyer
    public RMAppImpl(ApplicationId applicationId, RMContext rmContext,
        Configuration config, String name, String user, String queue,
        ApplicationSubmissionContext submissionContext, YarnScheduler scheduler,
        ApplicationMasterService masterService, long submitTime,
        String applicationType, Set<String> applicationTags,
        ResourceRequest amReq) {
        this(applicationId, rmContext, config, name, user, queue, submissionContext,
            scheduler, masterService, submitTime, applicationType, applicationTags, amReq, -1);
    }

    // TODO: 17/4/3 by zmyer
    public RMAppImpl(ApplicationId applicationId, RMContext rmContext,
        Configuration config, String name, String user, String queue,
        ApplicationSubmissionContext submissionContext, YarnScheduler scheduler,
        ApplicationMasterService masterService, long submitTime,
        String applicationType, Set<String> applicationTags,
        ResourceRequest amReq, long startTime) {

        this.systemClock = SystemClock.getInstance();
        this.applicationId = applicationId;
        this.name = name;
        this.rmContext = rmContext;
        this.dispatcher = rmContext.getDispatcher();
        this.handler = dispatcher.getEventHandler();
        this.conf = config;
        this.user = user;
        this.queue = queue;
        this.submissionContext = submissionContext;
        this.scheduler = scheduler;
        this.masterService = masterService;
        this.submitTime = submitTime;
        if (startTime <= 0) {
            this.startTime = this.systemClock.getTime();
        } else {
            this.startTime = startTime;
        }
        this.applicationType = applicationType;
        this.applicationTags = applicationTags;
        this.amReq = amReq;
        if (submissionContext.getPriority() != null) {
            this.applicationPriority = Priority.newInstance(submissionContext.getPriority().getPriority());
        }

        int globalMaxAppAttempts = conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
        int individualMaxAppAttempts = submissionContext.getMaxAppAttempts();
        if (individualMaxAppAttempts <= 0 || individualMaxAppAttempts > globalMaxAppAttempts) {
            this.maxAppAttempts = globalMaxAppAttempts;
            LOG.warn("The specific max attempts: " + individualMaxAppAttempts
                + " for application: " + applicationId.getId()
                + " is invalid, because it is out of the range [1, "
                + globalMaxAppAttempts + "]. Use the global max attempts instead.");
        } else {
            this.maxAppAttempts = individualMaxAppAttempts;
        }

        this.attemptFailuresValidityInterval = submissionContext.getAttemptFailuresValidityInterval();
        if (this.attemptFailuresValidityInterval > 0) {
            LOG.info("The attemptFailuresValidityInterval for the application: "
                + this.applicationId + " is " + this.attemptFailuresValidityInterval
                + ".");
        }

        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();

        this.stateMachine = stateMachineFactory.make(this);
        this.callerContext = CallerContext.getCurrent();

        long localLogAggregationStatusTimeout = conf.getLong(YarnConfiguration.LOG_AGGREGATION_STATUS_TIME_OUT_MS,
            YarnConfiguration.DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS);
        if (localLogAggregationStatusTimeout <= 0) {
            this.logAggregationStatusTimeout = YarnConfiguration.DEFAULT_LOG_AGGREGATION_STATUS_TIME_OUT_MS;
        } else {
            this.logAggregationStatusTimeout = localLogAggregationStatusTimeout;
        }
        this.logAggregationEnabled = conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
            YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED);
        if (this.logAggregationEnabled) {
            this.logAggregationStatusForAppReport = LogAggregationStatus.NOT_START;
        } else {
            this.logAggregationStatusForAppReport = LogAggregationStatus.DISABLED;
        }
        maxLogAggregationDiagnosticsInMemory = conf.getInt(
            YarnConfiguration.RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY,
            YarnConfiguration.DEFAULT_RM_MAX_LOG_AGGREGATION_DIAGNOSTICS_IN_MEMORY);

        // amBlacklistingEnabled can be configured globally
        // Just use the global values
        amBlacklistingEnabled = conf.getBoolean(YarnConfiguration.AM_SCHEDULING_NODE_BLACKLISTING_ENABLED,
            YarnConfiguration.DEFAULT_AM_SCHEDULING_NODE_BLACKLISTING_ENABLED);
        if (amBlacklistingEnabled) {

            blacklistDisableThreshold = conf.getFloat(YarnConfiguration.AM_SCHEDULING_NODE_BLACKLISTING_DISABLE_THRESHOLD,
                YarnConfiguration.DEFAULT_AM_SCHEDULING_NODE_BLACKLISTING_DISABLE_THRESHOLD);
            // Verify whether blacklistDisableThreshold is valid. And for invalid
            // threshold, reset to global level blacklistDisableThreshold
            // configured.
            if (blacklistDisableThreshold < MINIMUM_AM_BLACKLIST_THRESHOLD_VALUE ||
                blacklistDisableThreshold > MAXIMUM_AM_BLACKLIST_THRESHOLD_VALUE) {
                blacklistDisableThreshold = YarnConfiguration.
                    DEFAULT_AM_SCHEDULING_NODE_BLACKLISTING_DISABLE_THRESHOLD;
            }
        }
        applicationTimeouts = new HashMap<>();
    }

    /**
     * Starts the application level timeline collector for this app. This should
     * be used only if the timeline service v.2 is enabled.
     */
    // TODO: 17/4/3 by zmyer
    public void startTimelineCollector() {
        //创建基于等级的时间线收集对象
        AppLevelTimelineCollector collector = new AppLevelTimelineCollector(applicationId);
        //开始注册该收集对象
        rmContext.getRMTimelineCollectorManager().putIfAbsent(applicationId, collector);
    }

    /**
     * Stops the application level timeline collector for this app. This should be
     * used only if the timeline service v.2 is enabled.
     */
    // TODO: 17/4/3 by zmyer
    public void stopTimelineCollector() {
        rmContext.getRMTimelineCollectorManager().remove(applicationId);
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public ApplicationId getApplicationId() {
        return this.applicationId;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public ApplicationSubmissionContext getApplicationSubmissionContext() {
        return this.submissionContext;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public FinalApplicationStatus getFinalApplicationStatus() {
        // finish state is obtained based on the state machine's current state
        // as a fall-back in case the application has not been unregistered
        // ( or if the app never unregistered itself )
        // when the report is requested
        if (currentAttempt != null && currentAttempt.getFinalApplicationStatus() != null) {
            //返回当前的尝试执行对象完成的状态信息
            return currentAttempt.getFinalApplicationStatus();
        }
        //返回当前应用的完成执行的状态信息
        return createFinalApplicationStatus(this.stateMachine.getCurrentState());
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public RMAppState getState() {
        this.readLock.lock();
        try {
            //返回当前的应用状态
            return this.stateMachine.getCurrentState();
        } finally {
            this.readLock.unlock();
        }
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public String getUser() {
        return this.user;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public float getProgress() {
        RMAppAttempt attempt = this.currentAttempt;
        if (attempt != null) {
            //读取当前的应用执行进度
            return attempt.getProgress();
        }
        return 0;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public RMAppAttempt getRMAppAttempt(ApplicationAttemptId appAttemptId) {
        this.readLock.lock();

        try {
            //返回本次应用尝试执行的对象
            return this.attempts.get(appAttemptId);
        } finally {
            this.readLock.unlock();
        }
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public String getQueue() {
        return this.queue;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public void setQueue(String queue) {
        this.queue = queue;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public String getCollectorAddr() {
        return this.collectorAddr;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public void setCollectorAddr(String collectorAddress) {
        this.collectorAddr = collectorAddress;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public void removeCollectorAddr() {
        this.collectorAddr = null;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public String getName() {
        return this.name;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public RMAppAttempt getCurrentAppAttempt() {
        return this.currentAttempt;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public Map<ApplicationAttemptId, RMAppAttempt> getAppAttempts() {
        this.readLock.lock();

        try {
            //读取应用尝试执行对象映射表
            return Collections.unmodifiableMap(this.attempts);
        } finally {
            this.readLock.unlock();
        }
    }

    // TODO: 17/4/3 by zmyer
    private FinalApplicationStatus createFinalApplicationStatus(RMAppState state) {
        switch (state) {
            case NEW:
            case NEW_SAVING:
            case SUBMITTED:
            case ACCEPTED:
            case RUNNING:
            case FINAL_SAVING:
            case KILLING:
                return FinalApplicationStatus.UNDEFINED;
            // finished without a proper final state is the same as failed
            case FINISHING:
            case FINISHED:
            case FAILED:
                return FinalApplicationStatus.FAILED;
            case KILLED:
                return FinalApplicationStatus.KILLED;
        }
        throw new YarnRuntimeException("Unknown state passed!");
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public int pullRMNodeUpdates(Collection<RMNode> updatedNodes) {
        this.writeLock.lock();
        try {
            //读取节点变更列表长度
            int updatedNodeCount = this.updatedNodes.size();
            //更新变更列表
            updatedNodes.addAll(this.updatedNodes);
            this.updatedNodes.clear();
            return updatedNodeCount;
        } finally {
            this.writeLock.unlock();
        }
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public ApplicationReport createAndGetApplicationReport(String clientUserName,
        boolean allowAccess) {
        this.readLock.lock();

        try {
            ApplicationAttemptId currentApplicationAttemptId = null;
            org.apache.hadoop.yarn.api.records.Token clientToAMToken = null;
            String trackingUrl = UNAVAILABLE;
            String host = UNAVAILABLE;
            String origTrackingUrl = UNAVAILABLE;
            LogAggregationStatus logAggregationStatus = null;
            int rpcPort = -1;
            ApplicationResourceUsageReport appUsageReport =
                RMServerUtils.DUMMY_APPLICATION_RESOURCE_USAGE_REPORT;
            FinalApplicationStatus finishState = getFinalApplicationStatus();
            String diags = UNAVAILABLE;
            float progress = 0.0f;
            org.apache.hadoop.yarn.api.records.Token amrmToken = null;
            if (allowAccess) {
                trackingUrl = getDefaultProxyTrackingUrl();
                if (this.currentAttempt != null) {
                    currentApplicationAttemptId = this.currentAttempt.getAppAttemptId();
                    trackingUrl = this.currentAttempt.getTrackingUrl();
                    origTrackingUrl = this.currentAttempt.getOriginalTrackingUrl();
                    if (UserGroupInformation.isSecurityEnabled()) {
                        // get a token so the client can communicate with the app attempt
                        // NOTE: token may be unavailable if the attempt is not running
                        Token<ClientToAMTokenIdentifier> attemptClientToAMToken =
                            this.currentAttempt.createClientToken(clientUserName);
                        if (attemptClientToAMToken != null) {
                            clientToAMToken = BuilderUtils.newClientToAMToken(
                                attemptClientToAMToken.getIdentifier(),
                                attemptClientToAMToken.getKind().toString(),
                                attemptClientToAMToken.getPassword(),
                                attemptClientToAMToken.getService().toString());
                        }
                    }
                    host = this.currentAttempt.getHost();
                    rpcPort = this.currentAttempt.getRpcPort();
                    appUsageReport = currentAttempt.getApplicationResourceUsageReport();
                    progress = currentAttempt.getProgress();
                    logAggregationStatus = this.getLogAggregationStatusForAppReport();
                }
                //if the diagnostics is not already set get it from attempt
                diags = getDiagnostics().toString();

                if (currentAttempt != null &&
                    currentAttempt.getAppAttemptState() == RMAppAttemptState.LAUNCHED) {
                    if (getApplicationSubmissionContext().getUnmanagedAM() &&
                        clientUserName != null && getUser().equals(clientUserName)) {
                        Token<AMRMTokenIdentifier> token = currentAttempt.getAMRMToken();
                        if (token != null) {
                            amrmToken = BuilderUtils.newAMRMToken(token.getIdentifier(),
                                token.getKind().toString(), token.getPassword(),
                                token.getService().toString());
                        }
                    }
                }

                RMAppMetrics rmAppMetrics = getRMAppMetrics();
                appUsageReport.setMemorySeconds(rmAppMetrics.getMemorySeconds());
                appUsageReport.setVcoreSeconds(rmAppMetrics.getVcoreSeconds());
                appUsageReport.
                    setPreemptedMemorySeconds(rmAppMetrics.
                        getPreemptedMemorySeconds());
                appUsageReport.
                    setPreemptedVcoreSeconds(rmAppMetrics.
                        getPreemptedVcoreSeconds());
            }

            if (currentApplicationAttemptId == null) {
                currentApplicationAttemptId =
                    BuilderUtils.newApplicationAttemptId(this.applicationId,
                        DUMMY_APPLICATION_ATTEMPT_NUMBER);
            }

            ApplicationReport report = BuilderUtils.newApplicationReport(
                this.applicationId, currentApplicationAttemptId, this.user,
                this.queue, this.name, host, rpcPort, clientToAMToken,
                createApplicationState(), diags, trackingUrl, this.startTime,
                this.finishTime, finishState, appUsageReport, origTrackingUrl,
                progress, this.applicationType, amrmToken, applicationTags,
                this.getApplicationPriority());
            report.setLogAggregationStatus(logAggregationStatus);
            report.setUnmanagedApp(submissionContext.getUnmanagedAM());
            report.setAppNodeLabelExpression(getAppNodeLabelExpression());
            report.setAmNodeLabelExpression(getAmNodeLabelExpression());

            ApplicationTimeout timeout = ApplicationTimeout
                .newInstance(ApplicationTimeoutType.LIFETIME, UNLIMITED, UNKNOWN);
            // Currently timeout type supported is LIFETIME. When more timeout types
            // are supported in YARN-5692, the below logic need to be changed.
            if (!this.applicationTimeouts.isEmpty()) {
                long timeoutInMillis = applicationTimeouts
                    .get(ApplicationTimeoutType.LIFETIME).longValue();
                timeout.setExpiryTime(Times.formatISO8601(timeoutInMillis));
                if (isAppInCompletedStates()) {
                    // if application configured with timeout and finished before timeout
                    // happens then remaining time should not be calculated.
                    timeout.setRemainingTime(0);
                } else {
                    timeout.setRemainingTime(
                        Math.max((timeoutInMillis - systemClock.getTime()) / 1000, 0));
                }
            }
            report.setApplicationTimeouts(
                Collections.singletonMap(timeout.getTimeoutType(), timeout));
            return report;
        } finally {
            this.readLock.unlock();
        }
    }

    // TODO: 17/4/3 by zmyer
    private String getDefaultProxyTrackingUrl() {
        try {
            final String scheme = WebAppUtils.getHttpSchemePrefix(conf);
            String proxy = WebAppUtils.getProxyHostAndPort(conf);
            URI proxyUri = ProxyUriUtils.getUriFromAMUrl(scheme, proxy);
            URI result = ProxyUriUtils.getProxyUri(null, proxyUri, applicationId);
            return result.toASCIIString();
        } catch (URISyntaxException e) {
            LOG.warn("Could not generate default proxy tracking URL for "
                + applicationId);
            return UNAVAILABLE;
        }
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public long getFinishTime() {
        this.readLock.lock();

        try {
            return this.finishTime;
        } finally {
            this.readLock.unlock();
        }
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public long getStartTime() {
        this.readLock.lock();

        try {
            return this.startTime;
        } finally {
            this.readLock.unlock();
        }
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public long getSubmitTime() {
        return this.submitTime;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public String getTrackingUrl() {
        RMAppAttempt attempt = this.currentAttempt;
        if (attempt != null) {
            return attempt.getTrackingUrl();
        }
        return null;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public String getOriginalTrackingUrl() {
        RMAppAttempt attempt = this.currentAttempt;
        if (attempt != null) {
            return attempt.getOriginalTrackingUrl();
        }
        return null;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public StringBuilder getDiagnostics() {
        this.readLock.lock();
        try {
            if (diagnostics.length() == 0 && getCurrentAppAttempt() != null) {
                String appAttemptDiagnostics = getCurrentAppAttempt().getDiagnostics();
                if (appAttemptDiagnostics != null) {
                    return new StringBuilder(appAttemptDiagnostics);
                }
            }
            return this.diagnostics;
        } finally {
            this.readLock.unlock();
        }
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public int getMaxAppAttempts() {
        return this.maxAppAttempts;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public void handle(RMAppEvent event) {
        this.writeLock.lock();
        try {
            //读取应用id
            ApplicationId appID = event.getApplicationId();
            LOG.debug("Processing event for " + appID + " of type " + event.getType());
            //读取应用的旧的状态
            final RMAppState oldState = getState();
            try {/* keep the master in sync with the state machine */
                //开始进行状态转移
                this.stateMachine.doTransition(event.getType(), event);
            } catch (InvalidStateTransitionException e) {
                LOG.error("Can't handle this event at current state", e);
                /* TODO fail the application on the failed transition */
            }

            // Log at INFO if we're not recovering or not in a terminal state.
            // Log at DEBUG otherwise.
            if ((oldState != getState()) && (((recoveredFinalState == null)) ||
                (event.getType() != RMAppEventType.RECOVER))) {
                LOG.info(String.format(STATE_CHANGE_MESSAGE, appID, oldState, getState(), event.getType()));
            } else if ((oldState != getState()) && LOG.isDebugEnabled()) {
                LOG.debug(String.format(STATE_CHANGE_MESSAGE, appID, oldState, getState(), event.getType()));
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public void recover(RMState state) {
        //读取应用状态数据
        ApplicationStateData appState = state.getApplicationState().get(getApplicationId());
        //读取应用恢复状态信息
        this.recoveredFinalState = appState.getState();

        if (recoveredFinalState == null) {
            LOG.info(String.format(RECOVERY_MESSAGE, getApplicationId(),
                appState.getAttemptCount(), "NONE"));
        } else if (LOG.isDebugEnabled()) {
            LOG.debug(String.format(RECOVERY_MESSAGE, getApplicationId(),
                appState.getAttemptCount(), recoveredFinalState));
        }

        this.diagnostics.append(null == appState.getDiagnostics() ? "" : appState
            .getDiagnostics());
        this.storedFinishTime = appState.getFinishTime();
        this.startTime = appState.getStartTime();
        this.callerContext = appState.getCallerContext();
        this.applicationTimeouts = appState.getApplicationTimeouts();
        // If interval > 0, some attempts might have been deleted.
        if (this.attemptFailuresValidityInterval > 0) {
            this.firstAttemptIdInStateStore = appState.getFirstAttemptId();
            this.nextAttemptId = firstAttemptIdInStateStore;
        }
        //TODO recover collector address.
        //this.collectorAddr = appState.getCollectorAddr();

        // send the ATS create Event during RM recovery.
        // NOTE: it could be duplicated with events sent before RM get restarted.
        sendATSCreateEvent();
        RMAppAttemptImpl preAttempt = null;
        for (ApplicationAttemptId attemptId : new TreeSet<>(appState.attempts.keySet())) {
            // create attempt
            //创建新的应用尝试执行对象
            createNewAttempt(attemptId);
            //首先需要及时恢复当前应用执行的状态信息
            ((RMAppAttemptImpl) this.currentAttempt).recover(state);
            // If previous attempt is not in final state, it means we failed to store
            // its final state. We set it to FAILED now because we could not make sure
            // about its final state.
            if (preAttempt != null && preAttempt.getRecoveredFinalState() == null) {
                preAttempt.setRecoveredFinalState(RMAppAttemptState.FAILED);
            }
            preAttempt = (RMAppAttemptImpl) currentAttempt;
        }
        if (currentAttempt != null) {
            nextAttemptId = currentAttempt.getAppAttemptId().getAttemptId() + 1;
        }
    }

    // TODO: 17/4/3 by zmyer
    private void createNewAttempt() {
        //应用尝试执行对象
        ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(applicationId, nextAttemptId++);
        //开始创建新的尝试执行对象
        createNewAttempt(appAttemptId);
    }

    // TODO: 17/4/3 by zmyer
    private void createNewAttempt(ApplicationAttemptId appAttemptId) {
        //AM黑名单列表
        BlacklistManager currentAMBlacklistManager;
        if (currentAttempt != null) {
            // Transfer over the blacklist from the previous app-attempt.
            //读取当前执行对象中的黑名单
            currentAMBlacklistManager = currentAttempt.getAMBlacklistManager();
        } else {
            if (amBlacklistingEnabled) {
                currentAMBlacklistManager = new SimpleBlacklistManager(
                    scheduler.getNumClusterNodes(), blacklistDisableThreshold);
            } else {
                currentAMBlacklistManager = new DisabledBlacklistManager();
            }
        }
        //创建app本次尝试执行的对象
        RMAppAttempt attempt = new RMAppAttemptImpl(appAttemptId, rmContext,
            scheduler, masterService,
            submissionContext, conf,
            // The newly created attempt maybe last attempt if (number of
            // previously failed attempts(which should not include Preempted,
            // hardware error and NM resync) + 1) equal to the max-attempt
            // limit.
            maxAppAttempts == (getNumFailedAppAttempts() + 1), amReq, currentAMBlacklistManager);
        attempts.put(appAttemptId, attempt);
        currentAttempt = attempt;
    }

    // TODO: 17/4/3 by zmyer
    private void
    createAndStartNewAttempt(boolean transferStateFromPreviousAttempt) {
        //创建新的执行对象
        createNewAttempt();
        //开始启动执行对象
        handler.handle(new RMAppStartAttemptEvent(currentAttempt.getAppAttemptId(), transferStateFromPreviousAttempt));
    }

    // TODO: 17/4/3 by zmyer
    private void processNodeUpdate(RMAppNodeUpdateType type, RMNode node) {
        NodeState nodeState = node.getState();
        updatedNodes.add(node);
        LOG.debug("Received node update event:" + type + " for node:" + node
            + " with state:" + nodeState);
    }

    // TODO: 17/4/3 by zmyer
    private static class RMAppTransition implements SingleArcTransition<RMAppImpl, RMAppEvent> {
        public void transition(RMAppImpl app, RMAppEvent event) {
        }
    }

    // TODO: 17/4/3 by zmyer
    private static final class RMAppCollectorUpdateTransition extends RMAppTransition {

        // TODO: 17/4/3 by zmyer
        public void transition(RMAppImpl app, RMAppEvent event) {
            if (YarnConfiguration.timelineServiceV2Enabled(app.conf)) {
                LOG.info("Updating collector info for app: " + app.getApplicationId());

                //读取容器变更事件
                RMAppCollectorUpdateEvent appCollectorUpdateEvent = (RMAppCollectorUpdateEvent) event;
                // Update collector address
                //开始设置容器地址信息
                app.setCollectorAddr(appCollectorUpdateEvent.getAppCollectorAddr());

                // TODO persistent to RMStateStore for recover
                // Save to RMStateStore
            }
        }

        ;
    }

    // TODO: 17/4/3 by zmyer
    private static final class RMAppNodeUpdateTransition extends RMAppTransition {
        // TODO: 17/4/3 by zmyer
        public void transition(RMAppImpl app, RMAppEvent event) {
            RMAppNodeUpdateEvent nodeUpdateEvent = (RMAppNodeUpdateEvent) event;
            app.processNodeUpdate(nodeUpdateEvent.getUpdateType(), nodeUpdateEvent.getNode());
        }

        ;
    }

    // TODO: 17/4/3 by zmyer
    private static final class RMAppStateUpdateTransition extends RMAppTransition {
        private YarnApplicationState stateToATS;

        // TODO: 17/4/3 by zmyer
        public RMAppStateUpdateTransition(YarnApplicationState state) {
            stateToATS = state;
        }

        // TODO: 17/4/3 by zmyer
        public void transition(RMAppImpl app, RMAppEvent event) {
            app.rmContext.getSystemMetricsPublisher().appStateUpdated(
                app, stateToATS, app.systemClock.getTime());
        }

        ;
    }

    // TODO: 17/4/3 by zmyer
    private static final class AppRunningOnNodeTransition extends RMAppTransition {
        // TODO: 17/4/3 by zmyer
        public void transition(RMAppImpl app, RMAppEvent event) {
            //app运行在节点上的事件对象
            RMAppRunningOnNodeEvent nodeAddedEvent = (RMAppRunningOnNodeEvent) event;

            // if final state already stored, notify RMNode
            if (isAppInFinalState(app)) {
                //如果当前的应用已经处于执行完毕的状态,则需要及时清理该app对应的容器资源
                app.handler.handle(new RMNodeCleanAppEvent(nodeAddedEvent.getNodeId(), nodeAddedEvent.getApplicationId()));
                return;
            }

            // otherwise, add it to ranNodes for further process
            app.ranNodes.add(nodeAddedEvent.getNodeId());

            if (!app.logAggregationStatus.containsKey(nodeAddedEvent.getNodeId())) {
                app.logAggregationStatus.put(nodeAddedEvent.getNodeId(),
                    LogAggregationReport.newInstance(app.applicationId,
                        app.logAggregationEnabled ? LogAggregationStatus.NOT_START
                            : LogAggregationStatus.DISABLED, ""));
            }
        }
    }

    // synchronously recover attempt to ensure any incoming external events
    // to be processed after the attempt processes the recover event.
    // TODO: 17/4/3 by zmyer
    private void recoverAppAttempts() {
        // TODO: 17/4/3 by zmyer
        for (RMAppAttempt attempt : getAppAttempts().values()) {
            //开始恢复app执行对象
            attempt.handle(new RMAppAttemptEvent(attempt.getAppAttemptId(),
                RMAppAttemptEventType.RECOVER));
        }
    }

    // TODO: 17/4/3 by zmyer
    private static final class RMAppRecoveredTransition implements
        MultipleArcTransition<RMAppImpl, RMAppEvent, RMAppState> {

        // TODO: 17/4/3 by zmyer
        @Override
        public RMAppState transition(RMAppImpl app, RMAppEvent event) {

            //app恢复事件对下那个
            RMAppRecoverEvent recoverEvent = (RMAppRecoverEvent) event;
            //开始恢复app的状态信息
            app.recover(recoverEvent.getRMState());
            // The app has completed.
            if (app.recoveredFinalState != null) {
                //如果app还没有收到最后的状态信息。则需要恢复相关的应用执行对象
                app.recoverAppAttempts();
                new FinalTransition(app.recoveredFinalState).transition(app, event);
                return app.recoveredFinalState;
            }

            if (UserGroupInformation.isSecurityEnabled()) {
                // asynchronously renew delegation token on recovery.
                try {
                    app.rmContext.getDelegationTokenRenewer()
                        .addApplicationAsyncDuringRecovery(app.getApplicationId(),
                            app.parseCredentials(),
                            app.submissionContext.getCancelTokensWhenComplete(),
                            app.getUser());
                } catch (Exception e) {
                    String msg = "Failed to fetch user credentials from application:"
                        + e.getMessage();
                    app.diagnostics.append(msg);
                    LOG.error(msg, e);
                }
            }

            for (Map.Entry<ApplicationTimeoutType, Long> timeout :
                app.applicationTimeouts.entrySet()) {
                app.rmContext.getRMAppLifetimeMonitor().registerApp(app.applicationId,
                    timeout.getKey(), timeout.getValue());
                if (LOG.isDebugEnabled()) {
                    long remainingTime = timeout.getValue() - app.systemClock.getTime();
                    LOG.debug("Application " + app.applicationId
                        + " is registered for timeout monitor, type=" + timeout.getKey()
                        + " remaining timeout="
                        + (remainingTime > 0 ? remainingTime / 1000 : 0) + " seconds");
                }
            }

            // No existent attempts means the attempt associated with this app was not
            // started or started but not yet saved.
            if (app.attempts.isEmpty()) {
                app.scheduler.handle(new AppAddedSchedulerEvent(app.user,
                    app.submissionContext, false, app.applicationPriority));
                return RMAppState.SUBMITTED;
            }

            // Add application to scheduler synchronously to guarantee scheduler
            // knows applications before AM or NM re-registers.
            //开始调度执行当前的应用对象
            app.scheduler.handle(new AppAddedSchedulerEvent(app.user,
                app.submissionContext, true, app.applicationPriority));

            // recover attempts
            //恢复app执行对象
            app.recoverAppAttempts();

            // YARN-1507 is saving the application state after the application is
            // accepted. So after YARN-1507, an app is saved meaning it is accepted.
            // Thus we return ACCECPTED state on recovery.
            return RMAppState.ACCEPTED;
        }
    }

    // TODO: 17/4/3 by zmyer
    private static final class AddApplicationToSchedulerTransition extends RMAppTransition {
        // TODO: 17/4/3 by zmyer
        @Override
        public void transition(RMAppImpl app, RMAppEvent event) {
            //发送添加app事件对象
            app.handler.handle(new AppAddedSchedulerEvent(app.user, app.submissionContext, false, app.applicationPriority));
            // send the ATS create Event
            app.sendATSCreateEvent();
        }
    }

    // TODO: 17/4/3 by zmyer
    private static final class StartAppAttemptTransition extends RMAppTransition {
        // TODO: 17/4/3 by zmyer
        @Override
        public void transition(RMAppImpl app, RMAppEvent event) {
            app.createAndStartNewAttempt(false);
        }
    }

    // TODO: 17/4/3 by zmyer
    private static final class FinalStateSavedTransition implements
        MultipleArcTransition<RMAppImpl, RMAppEvent, RMAppState> {

        // TODO: 17/4/3 by zmyer
        @Override
        public RMAppState transition(RMAppImpl app, RMAppEvent event) {
            Map<ApplicationTimeoutType, Long> timeouts =
                app.submissionContext.getApplicationTimeouts();
            if (timeouts != null && timeouts.size() > 0) {
                app.rmContext.getRMAppLifetimeMonitor()
                    .unregisterApp(app.getApplicationId(), timeouts.keySet());
            }

            if (app.transitionTodo instanceof SingleArcTransition) {
                ((SingleArcTransition) app.transitionTodo).transition(app,
                    app.eventCausingFinalSaving);
            } else if (app.transitionTodo instanceof MultipleArcTransition) {
                ((MultipleArcTransition) app.transitionTodo).transition(app,
                    app.eventCausingFinalSaving);
            }
            return app.targetedFinalState;
        }
    }

    // TODO: 17/4/3 by zmyer
    private static class AttemptFailedFinalStateSavedTransition extends RMAppTransition {
        // TODO: 17/4/3 by zmyer
        @Override
        public void transition(RMAppImpl app, RMAppEvent event) {
            String msg = null;
            if (event instanceof RMAppFailedAttemptEvent) {
                msg = app.getAppAttemptFailedDiagnostics(event);
            }
            LOG.info(msg);
            app.diagnostics.append(msg);
            // Inform the node for app-finish
            new FinalTransition(RMAppState.FAILED).transition(app, event);
        }
    }

    // TODO: 17/4/3 by zmyer
    private String getAppAttemptFailedDiagnostics(RMAppEvent event) {
        String msg = null;
        RMAppFailedAttemptEvent failedEvent = (RMAppFailedAttemptEvent) event;
        if (this.submissionContext.getUnmanagedAM()) {
            // RM does not manage the AM. Do not retry
            msg = "Unmanaged application " + this.getApplicationId()
                + " failed due to " + failedEvent.getDiagnosticMsg()
                + ". Failing the application.";
        } else if (this.isNumAttemptsBeyondThreshold) {
            int globalLimit = conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
                YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
            msg = String.format(
                "Application %s failed %d times%s%s due to %s. Failing the application.",
                getApplicationId(),
                maxAppAttempts,
                (attemptFailuresValidityInterval <= 0 ? ""
                    : (" in previous " + attemptFailuresValidityInterval
                    + " milliseconds")),
                (globalLimit == maxAppAttempts) ? ""
                    : (" (global limit =" + globalLimit
                    + "; local limit is =" + maxAppAttempts + ")"),
                failedEvent.getDiagnosticMsg());
        }
        return msg;
    }

    // TODO: 17/4/3 by zmyer
    private static final class RMAppNewlySavingTransition extends RMAppTransition {
        // TODO: 17/4/3 by zmyer
        @Override
        public void transition(RMAppImpl app, RMAppEvent event) {
            //读取应用的生命周期时间
            long applicationLifetime = app.getApplicationLifetime(ApplicationTimeoutType.LIFETIME);
            if (applicationLifetime > 0) {
                // calculate next timeout value
                //计算应用的超时时间
                Long newTimeout = app.submitTime + (applicationLifetime * 1000);
                //开始向app生命周期监视器发送周期事件
                app.rmContext.getRMAppLifetimeMonitor().registerApp(app.applicationId,
                    ApplicationTimeoutType.LIFETIME, newTimeout);

                // update applicationTimeouts with new absolute value.
                //将该app插入到超时列表中
                app.applicationTimeouts.put(ApplicationTimeoutType.LIFETIME, newTimeout);

                LOG.info("Application " + app.applicationId + " is registered for timeout monitor, type="
                    + ApplicationTimeoutType.LIFETIME + " value=" + applicationLifetime + " seconds");
            }

            // If recovery is enabled then store the application information in a
            // non-blocking call so make sure that RM has stored the information
            // needed to restart the AM after RM restart without further client
            // communication
            LOG.info("Storing application with id " + app.applicationId);
            //开始将该app存储到状态存储对象中
            app.rmContext.getStateStore().storeNewApplication(app);
        }
    }

    // TODO: 17/4/3 by zmyer
    private void rememberTargetTransitions(RMAppEvent event,
        Object transitionToDo, RMAppState targetFinalState) {
        transitionTodo = transitionToDo;
        targetedFinalState = targetFinalState;
        eventCausingFinalSaving = event;
    }

    // TODO: 17/4/3 by zmyer
    private void rememberTargetTransitionsAndStoreState(RMAppEvent event,
        Object transitionToDo, RMAppState targetFinalState,
        RMAppState stateToBeStored) {
        rememberTargetTransitions(event, transitionToDo, targetFinalState);
        this.stateBeforeFinalSaving = getState();
        this.storedFinishTime = this.systemClock.getTime();

        LOG.info("Updating application " + this.applicationId
            + " with final state: " + this.targetedFinalState);
        // we lost attempt_finished diagnostics in app, because attempt_finished
        // diagnostics is sent after app final state is saved. Later on, we will
        // create GetApplicationAttemptReport specifically for getting per attempt
        // info.
        String diags = null;
        switch (event.getType()) {
            case APP_REJECTED:
            case ATTEMPT_FINISHED:
            case ATTEMPT_KILLED:
                diags = event.getDiagnosticMsg();
                break;
            case ATTEMPT_FAILED:
                RMAppFailedAttemptEvent failedEvent = (RMAppFailedAttemptEvent) event;
                diags = getAppAttemptFailedDiagnostics(failedEvent);
                break;
            default:
                break;
        }

        ApplicationStateData appState =
            ApplicationStateData.newInstance(this.submitTime, this.startTime,
                this.user, this.submissionContext,
                stateToBeStored, diags, this.storedFinishTime, this.callerContext);
        appState.setApplicationTimeouts(this.applicationTimeouts);
        this.rmContext.getStateStore().updateApplicationState(appState);
    }

    // TODO: 17/4/3 by zmyer
    private static final class FinalSavingTransition extends RMAppTransition {
        Object transitionToDo;
        RMAppState targetedFinalState;
        RMAppState stateToBeStored;

        // TODO: 17/4/3 by zmyer
        public FinalSavingTransition(Object transitionToDo,
            RMAppState targetedFinalState) {
            this(transitionToDo, targetedFinalState, targetedFinalState);
        }

        // TODO: 17/4/3 by zmyer
        public FinalSavingTransition(Object transitionToDo,
            RMAppState targetedFinalState, RMAppState stateToBeStored) {
            this.transitionToDo = transitionToDo;
            this.targetedFinalState = targetedFinalState;
            this.stateToBeStored = stateToBeStored;
        }

        // TODO: 17/4/3 by zmyer
        @Override
        public void transition(RMAppImpl app, RMAppEvent event) {
            app.rememberTargetTransitionsAndStoreState(event, transitionToDo,
                targetedFinalState, stateToBeStored);
        }
    }

    // TODO: 17/4/3 by zmyer
    private static class AttemptUnregisteredTransition extends RMAppTransition {
        // TODO: 17/4/3 by zmyer
        @Override
        public void transition(RMAppImpl app, RMAppEvent event) {
            app.finishTime = app.storedFinishTime;
        }
    }

    // TODO: 17/4/3 by zmyer
    private static class AppFinishedTransition extends FinalTransition {
        public AppFinishedTransition() {
            super(RMAppState.FINISHED);
        }

        // TODO: 17/4/3 by zmyer
        public void transition(RMAppImpl app, RMAppEvent event) {
            app.diagnostics.append(event.getDiagnosticMsg());
            super.transition(app, event);
        }

        ;
    }

    // TODO: 17/4/3 by zmyer
    private static class AttemptFinishedAtFinalSavingTransition extends
        RMAppTransition {
        // TODO: 17/4/3 by zmyer
        @Override
        public void transition(RMAppImpl app, RMAppEvent event) {
            if (app.targetedFinalState.equals(RMAppState.FAILED)
                || app.targetedFinalState.equals(RMAppState.KILLED)) {
                // Ignore Attempt_Finished event if we were supposed to reach FAILED
                // FINISHED state
                return;
            }

            // pass in the earlier attempt_unregistered event, as it is needed in
            // AppFinishedFinalStateSavedTransition later on
            app.rememberTargetTransitions(event,
                new AppFinishedFinalStateSavedTransition(app.eventCausingFinalSaving),
                RMAppState.FINISHED);
        }

        ;
    }

    // TODO: 17/4/3 by zmyer
    private static class AppFinishedFinalStateSavedTransition extends RMAppTransition {
        RMAppEvent attemptUnregistered;

        // TODO: 17/4/3 by zmyer
        public AppFinishedFinalStateSavedTransition(RMAppEvent attemptUnregistered) {
            this.attemptUnregistered = attemptUnregistered;
        }

        // TODO: 17/4/3 by zmyer
        @Override
        public void transition(RMAppImpl app, RMAppEvent event) {
            new AttemptUnregisteredTransition().transition(app, attemptUnregistered);
            FINISHED_TRANSITION.transition(app, event);
        }

        ;
    }

    /**
     * Log the audit event for kill by client.
     *
     * @param event The {@link RMAppEvent} to be logged
     */
    // TODO: 17/4/3 by zmyer
    static void auditLogKillEvent(RMAppEvent event) {
        if (event instanceof RMAppKillByClientEvent) {
            RMAppKillByClientEvent killEvent = (RMAppKillByClientEvent) event;
            UserGroupInformation callerUGI = killEvent.getCallerUGI();
            String userName = null;
            if (callerUGI != null) {
                userName = callerUGI.getShortUserName();
            }
            InetAddress remoteIP = killEvent.getIp();
            RMAuditLogger.logSuccess(userName, AuditConstants.KILL_APP_REQUEST,
                "RMAppImpl", event.getApplicationId(), remoteIP);
        }
    }

    // TODO: 17/4/3 by zmyer
    private static class AppKilledTransition extends FinalTransition {
        public AppKilledTransition() {
            super(RMAppState.KILLED);
        }

        // TODO: 17/4/3 by zmyer
        @Override
        public void transition(RMAppImpl app, RMAppEvent event) {
            app.diagnostics.append(event.getDiagnosticMsg());
            super.transition(app, event);
            RMAppImpl.auditLogKillEvent(event);
        }

        ;
    }

    // TODO: 17/4/3 by zmyer
    private static class KillAttemptTransition extends RMAppTransition {
        // TODO: 17/4/3 by zmyer
        @Override
        public void transition(RMAppImpl app, RMAppEvent event) {
            app.stateBeforeKilling = app.getState();
            // Forward app kill diagnostics in the event to kill app attempt.
            // These diagnostics will be returned back in ATTEMPT_KILLED event sent by
            // RMAppAttemptImpl.
            app.handler.handle(new RMAppAttemptEvent(app.currentAttempt.getAppAttemptId(),
                RMAppAttemptEventType.KILL, event.getDiagnosticMsg()));
            RMAppImpl.auditLogKillEvent(event);
        }
    }

    // TODO: 17/4/3 by zmyer
    private static final class AppRejectedTransition extends FinalTransition {
        public AppRejectedTransition() {
            super(RMAppState.FAILED);
        }

        // TODO: 17/4/3 by zmyer
        public void transition(RMAppImpl app, RMAppEvent event) {
            app.diagnostics.append(event.getDiagnosticMsg());
            super.transition(app, event);
        }

    }

    // TODO: 17/4/3 by zmyer
    private static class FinalTransition extends RMAppTransition {

        private final RMAppState finalState;

        public FinalTransition(RMAppState finalState) {
            this.finalState = finalState;
        }

        // TODO: 17/4/3 by zmyer
        public void transition(RMAppImpl app, RMAppEvent event) {
            app.logAggregationStartTime = System.currentTimeMillis();
            for (NodeId nodeId : app.getRanNodes()) {
                app.handler.handle(new RMNodeCleanAppEvent(nodeId, app.applicationId));
            }
            app.finishTime = app.storedFinishTime;
            if (app.finishTime == 0) {
                app.finishTime = app.systemClock.getTime();
            }
            // Recovered apps that are completed were not added to scheduler, so no
            // need to remove them from scheduler.
            if (app.recoveredFinalState == null) {
                app.handler.handle(new AppRemovedSchedulerEvent(app.applicationId, finalState));
            }
            app.handler.handle(new RMAppManagerEvent(app.applicationId,
                RMAppManagerEventType.APP_COMPLETED));

            app.rmContext.getRMApplicationHistoryWriter()
                .applicationFinished(app, finalState);
            app.rmContext.getSystemMetricsPublisher()
                .appFinished(app, finalState, app.finishTime);
        }
    }

    // TODO: 17/4/3 by zmyer
    private int getNumFailedAppAttempts() {
        int completedAttempts = 0;
        long endTime = this.systemClock.getTime();
        // Do not count AM preemption, hardware failures or NM resync
        // as attempt failure.
        for (RMAppAttempt attempt : attempts.values()) {
            if (attempt.shouldCountTowardsMaxAttemptRetry()) {
                if (this.attemptFailuresValidityInterval <= 0
                    || (attempt.getFinishTime() > endTime
                    - this.attemptFailuresValidityInterval)) {
                    completedAttempts++;
                }
            }
        }
        return completedAttempts;
    }

    // TODO: 17/4/3 by zmyer
    private static final class AttemptFailedTransition implements
        MultipleArcTransition<RMAppImpl, RMAppEvent, RMAppState> {

        private final RMAppState initialState;

        public AttemptFailedTransition(RMAppState initialState) {
            this.initialState = initialState;
        }

        // TODO: 17/4/3 by zmyer
        @Override
        public RMAppState transition(RMAppImpl app, RMAppEvent event) {
            int numberOfFailure = app.getNumFailedAppAttempts();
            LOG.info("The number of failed attempts"
                + (app.attemptFailuresValidityInterval > 0 ? " in previous "
                + app.attemptFailuresValidityInterval + " milliseconds " : " ")
                + "is " + numberOfFailure + ". The max attempts is "
                + app.maxAppAttempts);

            if (app.attemptFailuresValidityInterval > 0) {
                removeExcessAttempts(app);
            }

            if (!app.submissionContext.getUnmanagedAM()
                && numberOfFailure < app.maxAppAttempts) {
                if (initialState.equals(RMAppState.KILLING)) {
                    // If this is not last attempt, app should be killed instead of
                    // launching a new attempt
                    app.rememberTargetTransitionsAndStoreState(event,
                        new AppKilledTransition(), RMAppState.KILLED, RMAppState.KILLED);
                    return RMAppState.FINAL_SAVING;
                }

                boolean transferStateFromPreviousAttempt;
                RMAppFailedAttemptEvent failedEvent = (RMAppFailedAttemptEvent) event;
                transferStateFromPreviousAttempt =
                    failedEvent.getTransferStateFromPreviousAttempt();

                RMAppAttempt oldAttempt = app.currentAttempt;
                app.createAndStartNewAttempt(transferStateFromPreviousAttempt);
                // Transfer the state from the previous attempt to the current attempt.
                // Note that the previous failed attempt may still be collecting the
                // container events from the scheduler and update its data structures
                // before the new attempt is created. We always transferState for
                // finished containers so that they can be acked to NM,
                // but when pulling finished container we will check this flag again.
                ((RMAppAttemptImpl) app.currentAttempt)
                    .transferStateFromAttempt(oldAttempt);
                return initialState;
            } else {
                if (numberOfFailure >= app.maxAppAttempts) {
                    app.isNumAttemptsBeyondThreshold = true;
                }
                app.rememberTargetTransitionsAndStoreState(event,
                    new AttemptFailedFinalStateSavedTransition(), RMAppState.FAILED,
                    RMAppState.FAILED);
                return RMAppState.FINAL_SAVING;
            }
        }

        // TODO: 17/4/3 by zmyer
        private void removeExcessAttempts(RMAppImpl app) {
            while (app.nextAttemptId
                - app.firstAttemptIdInStateStore > app.maxAppAttempts) {
                // attempts' first element is oldest attempt because it is a
                // LinkedHashMap
                ApplicationAttemptId attemptId = ApplicationAttemptId.newInstance(
                    app.getApplicationId(), app.firstAttemptIdInStateStore);
                RMAppAttempt rmAppAttempt = app.getRMAppAttempt(attemptId);
                long endTime = app.systemClock.getTime();
                if (rmAppAttempt.getFinishTime() < (endTime
                    - app.attemptFailuresValidityInterval)) {
                    app.firstAttemptIdInStateStore++;
                    LOG.info("Remove attempt from state store : " + attemptId);
                    app.rmContext.getStateStore().removeApplicationAttempt(attemptId);
                } else {
                    break;
                }
            }
        }
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public String getApplicationType() {
        return this.applicationType;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public Set<String> getApplicationTags() {
        return this.applicationTags;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public boolean isAppFinalStateStored() {
        RMAppState state = getState();
        return state.equals(RMAppState.FINISHING)
            || state.equals(RMAppState.FINISHED) || state.equals(RMAppState.FAILED)
            || state.equals(RMAppState.KILLED);
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public YarnApplicationState createApplicationState() {
        RMAppState rmAppState = getState();
        // If App is in FINAL_SAVING state, return its previous state.
        if (rmAppState.equals(RMAppState.FINAL_SAVING)) {
            rmAppState = stateBeforeFinalSaving;
        }
        if (rmAppState.equals(RMAppState.KILLING)) {
            rmAppState = stateBeforeKilling;
        }
        return RMServerUtils.createApplicationState(rmAppState);
    }

    // TODO: 17/4/3 by zmyer
    public static boolean isAppInFinalState(RMApp rmApp) {
        RMAppState appState = ((RMAppImpl) rmApp).getRecoveredFinalState();
        if (appState == null) {
            appState = rmApp.getState();
        }
        return appState == RMAppState.FAILED || appState == RMAppState.FINISHED
            || appState == RMAppState.KILLED;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public boolean isAppInCompletedStates() {
        RMAppState appState = getState();
        return appState == RMAppState.FINISHED || appState == RMAppState.FINISHING
            || appState == RMAppState.FAILED || appState == RMAppState.KILLED
            || appState == RMAppState.FINAL_SAVING
            || appState == RMAppState.KILLING;
    }

    // TODO: 17/4/3 by zmyer
    public RMAppState getRecoveredFinalState() {
        return this.recoveredFinalState;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public Set<NodeId> getRanNodes() {
        return ranNodes;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public RMAppMetrics getRMAppMetrics() {
        Resource resourcePreempted = Resource.newInstance(0, 0);
        int numAMContainerPreempted = 0;
        int numNonAMContainerPreempted = 0;
        long memorySeconds = 0;
        long vcoreSeconds = 0;
        long preemptedMemorySeconds = 0;
        long preemptedVcoreSeconds = 0;
        for (RMAppAttempt attempt : attempts.values()) {
            if (null != attempt) {
                RMAppAttemptMetrics attemptMetrics =
                    attempt.getRMAppAttemptMetrics();
                Resources.addTo(resourcePreempted,
                    attemptMetrics.getResourcePreempted());
                numAMContainerPreempted += attemptMetrics.getIsPreempted() ? 1 : 0;
                numNonAMContainerPreempted +=
                    attemptMetrics.getNumNonAMContainersPreempted();
                // getAggregateAppResourceUsage() will calculate resource usage stats
                // for both running and finished containers.
                AggregateAppResourceUsage resUsage =
                    attempt.getRMAppAttemptMetrics().getAggregateAppResourceUsage();
                memorySeconds += resUsage.getMemorySeconds();
                vcoreSeconds += resUsage.getVcoreSeconds();
                preemptedMemorySeconds += attemptMetrics.getPreemptedMemory();
                preemptedVcoreSeconds += attemptMetrics.getPreemptedVcore();
            }
        }

        return new RMAppMetrics(resourcePreempted,
            numNonAMContainerPreempted, numAMContainerPreempted,
            memorySeconds, vcoreSeconds,
            preemptedMemorySeconds, preemptedVcoreSeconds);
    }

    // TODO: 17/4/3 by zmyer
    @Private
    @VisibleForTesting
    public void setSystemClock(Clock clock) {
        this.systemClock = clock;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public ReservationId getReservationId() {
        return submissionContext.getReservationID();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public ResourceRequest getAMResourceRequest() {
        return this.amReq;
    }

    // TODO: 17/4/3 by zmyer
    protected Credentials parseCredentials() throws IOException {
        Credentials credentials = new Credentials();
        DataInputByteBuffer dibb = new DataInputByteBuffer();
        ByteBuffer tokens = submissionContext.getAMContainerSpec().getTokens();
        if (tokens != null) {
            dibb.reset(tokens);
            credentials.readTokenStorageStream(dibb);
            tokens.rewind();
        }
        return credentials;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public Map<NodeId, LogAggregationReport> getLogAggregationReportsForApp() {
        try {
            this.readLock.lock();
            Map<NodeId, LogAggregationReport> outputs = new HashMap<NodeId, LogAggregationReport>();
            outputs.putAll(logAggregationStatus);
            if (!isLogAggregationFinished()) {
                for (Entry<NodeId, LogAggregationReport> output : outputs.entrySet()) {
                    if (!output.getValue().getLogAggregationStatus()
                        .equals(LogAggregationStatus.TIME_OUT)
                        && !output.getValue().getLogAggregationStatus()
                        .equals(LogAggregationStatus.SUCCEEDED)
                        && !output.getValue().getLogAggregationStatus()
                        .equals(LogAggregationStatus.FAILED)
                        && isAppInFinalState(this)
                        && System.currentTimeMillis() > this.logAggregationStartTime
                        + this.logAggregationStatusTimeout) {
                        output.getValue().setLogAggregationStatus(LogAggregationStatus.TIME_OUT);
                    }
                }
            }
            return outputs;
        } finally {
            this.readLock.unlock();
        }
    }

    // TODO: 17/4/3 by zmyer
    public void aggregateLogReport(NodeId nodeId, LogAggregationReport report) {
        try {
            this.writeLock.lock();
            if (this.logAggregationEnabled && !isLogAggregationFinished()) {
                LogAggregationReport curReport = this.logAggregationStatus.get(nodeId);
                boolean stateChangedToFinal = false;
                if (curReport == null) {
                    this.logAggregationStatus.put(nodeId, report);
                    if (isLogAggregationFinishedForNM(report)) {
                        stateChangedToFinal = true;
                    }
                } else {
                    if (isLogAggregationFinishedForNM(report)) {
                        if (!isLogAggregationFinishedForNM(curReport)) {
                            stateChangedToFinal = true;
                        }
                    }
                    if (report.getLogAggregationStatus() != LogAggregationStatus.RUNNING
                        || curReport.getLogAggregationStatus() !=
                        LogAggregationStatus.RUNNING_WITH_FAILURE) {
                        if (curReport.getLogAggregationStatus()
                            == LogAggregationStatus.TIME_OUT
                            && report.getLogAggregationStatus()
                            == LogAggregationStatus.RUNNING) {
                            // If the log aggregation status got from latest nm heartbeat
                            // is Running, and current log aggregation status is TimeOut,
                            // based on whether there are any failure messages for this NM,
                            // we will reset the log aggregation status as RUNNING or
                            // RUNNING_WITH_FAILURE
                            if (logAggregationFailureMessagesForNMs.get(nodeId) != null &&
                                !logAggregationFailureMessagesForNMs.get(nodeId).isEmpty()) {
                                report.setLogAggregationStatus(
                                    LogAggregationStatus.RUNNING_WITH_FAILURE);
                            }
                        }
                        curReport.setLogAggregationStatus(report
                            .getLogAggregationStatus());
                    }
                }
                updateLogAggregationDiagnosticMessages(nodeId, report);
                if (isAppInFinalState(this) && stateChangedToFinal) {
                    updateLogAggregationStatus(nodeId);
                }
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public LogAggregationStatus getLogAggregationStatusForAppReport() {
        try {
            this.readLock.lock();
            if (!logAggregationEnabled) {
                return LogAggregationStatus.DISABLED;
            }
            if (isLogAggregationFinished()) {
                return this.logAggregationStatusForAppReport;
            }
            Map<NodeId, LogAggregationReport> reports =
                getLogAggregationReportsForApp();
            if (reports.size() == 0) {
                return this.logAggregationStatusForAppReport;
            }
            int logNotStartCount = 0;
            int logCompletedCount = 0;
            int logTimeOutCount = 0;
            int logFailedCount = 0;
            int logRunningWithFailure = 0;
            for (Entry<NodeId, LogAggregationReport> report : reports.entrySet()) {
                switch (report.getValue().getLogAggregationStatus()) {
                    case NOT_START:
                        logNotStartCount++;
                        break;
                    case RUNNING_WITH_FAILURE:
                        logRunningWithFailure++;
                        break;
                    case SUCCEEDED:
                        logCompletedCount++;
                        break;
                    case FAILED:
                        logFailedCount++;
                        logCompletedCount++;
                        break;
                    case TIME_OUT:
                        logTimeOutCount++;
                        logCompletedCount++;
                        break;
                    default:
                        break;
                }
            }
            if (logNotStartCount == reports.size()) {
                return LogAggregationStatus.NOT_START;
            } else if (logCompletedCount == reports.size()) {
                // We should satisfy two condition in order to return SUCCEEDED or FAILED
                // 1) make sure the application is in final state
                // 2) logs status from all NMs are SUCCEEDED/FAILED/TIMEOUT
                // The SUCCEEDED/FAILED status is the final status which means
                // the log aggregation is finished. And the log aggregation status will
                // not be updated anymore.
                if (logFailedCount > 0 && isAppInFinalState(this)) {
                    return LogAggregationStatus.FAILED;
                } else if (logTimeOutCount > 0) {
                    return LogAggregationStatus.TIME_OUT;
                }
                if (isAppInFinalState(this)) {
                    return LogAggregationStatus.SUCCEEDED;
                }
            } else if (logRunningWithFailure > 0) {
                return LogAggregationStatus.RUNNING_WITH_FAILURE;
            }
            return LogAggregationStatus.RUNNING;
        } finally {
            this.readLock.unlock();
        }
    }

    // TODO: 17/4/3 by zmyer
    private boolean isLogAggregationFinished() {
        return this.logAggregationStatusForAppReport.equals(LogAggregationStatus.SUCCEEDED)
            || this.logAggregationStatusForAppReport.equals(LogAggregationStatus.FAILED);

    }

    // TODO: 17/4/3 by zmyer
    private boolean isLogAggregationFinishedForNM(LogAggregationReport report) {
        return report.getLogAggregationStatus() == LogAggregationStatus.SUCCEEDED
            || report.getLogAggregationStatus() == LogAggregationStatus.FAILED;
    }

    // TODO: 17/4/3 by zmyer
    private void updateLogAggregationDiagnosticMessages(NodeId nodeId, LogAggregationReport report) {
        if (report.getDiagnosticMessage() != null
            && !report.getDiagnosticMessage().isEmpty()) {
            if (report.getLogAggregationStatus() == LogAggregationStatus.RUNNING) {
                List<String> diagnostics = logAggregationDiagnosticsForNMs.get(nodeId);
                if (diagnostics == null) {
                    diagnostics = new ArrayList<String>();
                    logAggregationDiagnosticsForNMs.put(nodeId, diagnostics);
                } else {
                    if (diagnostics.size() == maxLogAggregationDiagnosticsInMemory) {
                        diagnostics.remove(0);
                    }
                }
                diagnostics.add(report.getDiagnosticMessage());
                this.logAggregationStatus.get(nodeId).setDiagnosticMessage(
                    StringUtils.join(diagnostics, "\n"));
            } else if (report.getLogAggregationStatus()
                == LogAggregationStatus.RUNNING_WITH_FAILURE) {
                List<String> failureMessages =
                    logAggregationFailureMessagesForNMs.get(nodeId);
                if (failureMessages == null) {
                    failureMessages = new ArrayList<String>();
                    logAggregationFailureMessagesForNMs.put(nodeId, failureMessages);
                } else {
                    if (failureMessages.size()
                        == maxLogAggregationDiagnosticsInMemory) {
                        failureMessages.remove(0);
                    }
                }
                failureMessages.add(report.getDiagnosticMessage());
            }
        }
    }

    // TODO: 17/4/3 by zmyer
    private void updateLogAggregationStatus(NodeId nodeId) {
        LogAggregationStatus status =
            this.logAggregationStatus.get(nodeId).getLogAggregationStatus();
        if (status.equals(LogAggregationStatus.SUCCEEDED)) {
            this.logAggregationSucceed++;
        } else if (status.equals(LogAggregationStatus.FAILED)) {
            this.logAggregationFailed++;
        }
        if (this.logAggregationSucceed == this.logAggregationStatus.size()) {
            this.logAggregationStatusForAppReport =
                LogAggregationStatus.SUCCEEDED;
            // Since the log aggregation status for this application for all NMs
            // is SUCCEEDED, it means all logs are aggregated successfully.
            // We could remove all the cached log aggregation reports
            this.logAggregationStatus.clear();
            this.logAggregationDiagnosticsForNMs.clear();
            this.logAggregationFailureMessagesForNMs.clear();
        } else if (this.logAggregationSucceed + this.logAggregationFailed
            == this.logAggregationStatus.size()) {
            this.logAggregationStatusForAppReport = LogAggregationStatus.FAILED;
            // We have collected the log aggregation status for all NMs.
            // The log aggregation status is FAILED which means the log
            // aggregation fails in some NMs. We are only interested in the
            // nodes where the log aggregation is failed. So we could remove
            // the log aggregation details for those succeeded NMs
            for (Iterator<Map.Entry<NodeId, LogAggregationReport>> it =
                this.logAggregationStatus.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<NodeId, LogAggregationReport> entry = it.next();
                if (entry.getValue().getLogAggregationStatus()
                    .equals(LogAggregationStatus.SUCCEEDED)) {
                    it.remove();
                }
            }
            // the log aggregation has finished/failed.
            // and the status will not be updated anymore.
            this.logAggregationDiagnosticsForNMs.clear();
        }
    }

    // TODO: 17/4/3 by zmyer
    public String getLogAggregationFailureMessagesForNM(NodeId nodeId) {
        try {
            this.readLock.lock();
            List<String> failureMessages = this.logAggregationFailureMessagesForNMs.get(nodeId);
            if (failureMessages == null || failureMessages.isEmpty()) {
                return StringUtils.EMPTY;
            }
            return StringUtils.join(failureMessages, "\n");
        } finally {
            this.readLock.unlock();
        }
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public String getAppNodeLabelExpression() {
        String appNodeLabelExpression = getApplicationSubmissionContext().getNodeLabelExpression();
        appNodeLabelExpression = (appNodeLabelExpression == null)
            ? NodeLabel.NODE_LABEL_EXPRESSION_NOT_SET : appNodeLabelExpression;
        appNodeLabelExpression = (appNodeLabelExpression.trim().isEmpty())
            ? NodeLabel.DEFAULT_NODE_LABEL_PARTITION : appNodeLabelExpression;
        return appNodeLabelExpression;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public String getAmNodeLabelExpression() {
        String amNodeLabelExpression = null;
        if (!getApplicationSubmissionContext().getUnmanagedAM()) {
            amNodeLabelExpression = getAMResourceRequest().getNodeLabelExpression();
            amNodeLabelExpression = (amNodeLabelExpression == null)
                ? NodeLabel.NODE_LABEL_EXPRESSION_NOT_SET : amNodeLabelExpression;
            amNodeLabelExpression = (amNodeLabelExpression.trim().isEmpty())
                ? NodeLabel.DEFAULT_NODE_LABEL_PARTITION : amNodeLabelExpression;
        }
        return amNodeLabelExpression;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public CallerContext getCallerContext() {
        return callerContext;
    }

    // TODO: 17/4/3 by zmyer
    private void sendATSCreateEvent() {
        rmContext.getRMApplicationHistoryWriter().applicationStarted(this);
        rmContext.getSystemMetricsPublisher().appCreated(this, this.startTime);
    }

    // TODO: 17/4/3 by zmyer
    @Private
    @VisibleForTesting
    public int getNextAttemptId() {
        return nextAttemptId;
    }

    // TODO: 17/4/3 by zmyer
    private long getApplicationLifetime(ApplicationTimeoutType type) {
        Map<ApplicationTimeoutType, Long> timeouts =
            this.submissionContext.getApplicationTimeouts();
        long applicationLifetime = -1;
        if (timeouts != null && timeouts.containsKey(type)) {
            applicationLifetime = timeouts.get(type);
        }
        return applicationLifetime;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public Map<ApplicationTimeoutType, Long> getApplicationTimeouts() {
        this.readLock.lock();
        try {
            return new HashMap(this.applicationTimeouts);
        } finally {
            this.readLock.unlock();
        }
    }

    // TODO: 17/4/3 by zmyer
    public void updateApplicationTimeout(Map<ApplicationTimeoutType, Long> updateTimeout) {
        this.writeLock.lock();
        try {
            if (COMPLETED_APP_STATES.contains(getState())) {
                return;
            }
            // update monitoring service
            this.rmContext.getRMAppLifetimeMonitor()
                .updateApplicationTimeouts(getApplicationId(), updateTimeout);
            this.applicationTimeouts.putAll(updateTimeout);

        } finally {
            this.writeLock.unlock();
        }
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public Priority getApplicationPriority() {
        return applicationPriority;
    }

    // TODO: 17/4/3 by zmyer
    public void setApplicationPriority(Priority applicationPriority) {
        this.applicationPriority = applicationPriority;
    }
}
