/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AuthenticationFilterInitializer;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.HttpCrossOriginFilterInitializer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.ConfigurationProviderFactory;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.AMLauncherEventType;
import org.apache.hadoop.yarn.server.resourcemanager.amlauncher.ApplicationMasterLauncher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.NoOpSystemMetricPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.TimelineServiceV1Publisher;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.TimelineServiceV2Publisher;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingEditPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMDelegatedNodeLabelsUpdater;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.AbstractReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.monitor.RMAppLifetimeMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.QueueACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWebApp;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.security.http.RMAuthenticationFilter;
import org.apache.hadoop.yarn.server.security.http.RMAuthenticationFilterInitializer;
import org.apache.hadoop.yarn.server.webproxy.AppReportFetcher;
import org.apache.hadoop.yarn.server.webproxy.ProxyUriUtils;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxy;
import org.apache.hadoop.yarn.server.webproxy.WebAppProxyServlet;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import org.apache.hadoop.yarn.webapp.WebApps.Builder;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.eclipse.jetty.webapp.WebAppContext;

/**
 * The ResourceManager is the main class that is a set of components.
 * "I am the ResourceManager. All your resources belong to us..."
 */
// TODO: 17/3/22 by zmyer
@SuppressWarnings("unchecked")
public class ResourceManager extends CompositeService implements Recoverable {

    /**
     * Priority of the ResourceManager shutdown hook.
     */
    //资源管理器关闭优先级
    public static final int SHUTDOWN_HOOK_PRIORITY = 30;

    /**
     * Used for generation of various ids.
     */
    public static final int EPOCH_BIT_SHIFT = 40;

    private static final Log LOG = LogFactory.getLog(ResourceManager.class);
    //集群时间戳
    private static long clusterTimeStamp = System.currentTimeMillis();

    /*
     * UI2 webapp name
     */
    public static final String UI2_WEBAPP_NAME = "/ui2";

    /**
     * "Always On" services. Services that need to run always irrespective of
     * the HA state of the RM.
     */
    @VisibleForTesting
    //资源管理器上下文对象
    protected RMContextImpl rmContext;
    //事件分发器对象
    private Dispatcher rmDispatcher;
    @VisibleForTesting
    //管理服务对象
    protected AdminService adminService;

    /**
     * "Active" services. Services that need to run only on the Active RM.
     * These services are managed (initialized, started, stopped) by the
     * {@link CompositeService} RMActiveServices.
     * <p>
     * RM is active when (1) HA is disabled, or (2) HA is enabled and the RM is
     * in Active state.
     */
    //存活服务对象
    protected RMActiveServices activeServices;
    //安全管理服务对象
    protected RMSecretManagerService rmSecretManagerService;
    //资源调度对象
    protected ResourceScheduler scheduler;
    //资源保留系统对象
    protected ReservationSystem reservationSystem;
    //客户端资源服务对象
    private ClientRMService clientRM;
    //应用master服务对象
    protected ApplicationMasterService masterService;
    //节点服务器存活监视器对象
    protected NMLivelinessMonitor nmLivelinessMonitor;
    //节点列表管理器
    protected NodesListManager nodesListManager;
    //应用管理对象
    protected RMAppManager rmAppManager;
    //应用acl管理器
    protected ApplicationACLsManager applicationACLsManager;
    //队列acl管理器
    protected QueueACLsManager queueACLsManager;
    //web应用对象
    private WebApp webApp;
    //
    private AppReportFetcher fetcher = null;
    //资源跟踪服务对象
    protected ResourceTrackerService resourceTracker;
    //jvm统计对象
    private JvmMetrics jvmMetrics;
    //是否开启zk
    private boolean curatorEnabled = false;
    //zk客户端
    private CuratorFramework curator;
    //zk根节点密码
    private final String zkRootNodePassword = Long.toString(new SecureRandom().nextLong());
    //恢复标记
    private boolean recoveryEnabled;

    @VisibleForTesting
    //web应用地址
    protected String webAppAddress;
    //配置提供对象
    private ConfigurationProvider configurationProvider = null;
    /**
     * End of Active services
     */
    //配置对象
    private Configuration conf;

    //用户组信息
    private UserGroupInformation rmLoginUGI;

    // TODO: 17/2/19 by zmyer
    public ResourceManager() {
        super("ResourceManager");
    }

    // TODO: 17/3/22 by zmyer
    public RMContext getRMContext() {
        return this.rmContext;
    }

    // TODO: 17/3/22 by zmyer
    public static long getClusterTimeStamp() {
        return clusterTimeStamp;
    }

    // TODO: 17/3/22 by zmyer
    @VisibleForTesting
    protected static void setClusterTimeStamp(long timestamp) {
        clusterTimeStamp = timestamp;
    }

    // TODO: 17/3/22 by zmyer
    @VisibleForTesting
    Dispatcher getRmDispatcher() {
        return rmDispatcher;
    }

    // TODO: 17/3/22 by zmyer
    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        this.conf = conf;
        //创建执行上下文对象
        this.rmContext = new RMContextImpl();

        //创建配置提供者
        this.configurationProvider = ConfigurationProviderFactory.getConfigurationProvider(conf);
        //初始化配置提供者对象
        this.configurationProvider.init(this.conf);
        //设置配置提供者
        rmContext.setConfigurationProvider(configurationProvider);

        // load core-site.xml
        //读取core-site.xml文件
        InputStream coreSiteXMLInputStream = this.configurationProvider.getConfigurationInputStream(this.conf,
            YarnConfiguration.CORE_SITE_CONFIGURATION_FILE);
        if (coreSiteXMLInputStream != null) {
            //开始加载core-site文件
            this.conf.addResource(coreSiteXMLInputStream, YarnConfiguration.CORE_SITE_CONFIGURATION_FILE);
        }

        // Do refreshUserToGroupsMappings with loaded core-site.xml
        //刷新用户组信息
        Groups.getUserToGroupsMappingServiceWithLoadedConfiguration(this.conf).refresh();

        // Do refreshSuperUserGroupsConfiguration with loaded core-site.xml
        // Or use RM specific configurations to overwrite the common ones first
        // if they exist
        RMServerUtils.processRMProxyUsersConf(conf);
        ProxyUsers.refreshSuperUserGroupsConfiguration(this.conf);

        // load yarn-site.xml
        //加载yarn-site配置文件
        InputStream yarnSiteXMLInputStream = this.configurationProvider.getConfigurationInputStream(this.conf,
            YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
        if (yarnSiteXMLInputStream != null) {
            //加载yarn-site
            this.conf.addResource(yarnSiteXMLInputStream, YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
        }

        //验证配置文件
        validateConfigs(this.conf);
        // Set HA configuration should be done before login
        //设置是否开启HA
        this.rmContext.setHAEnabled(HAUtil.isHAEnabled(this.conf));
        if (this.rmContext.isHAEnabled()) {
            //开始验证HA配置
            HAUtil.verifyAndSetConfiguration(this.conf);

            // If the RM is configured to use an embedded leader elector,
            // initialize the leader elector.
            if (HAUtil.isAutomaticFailoverEnabled(conf) && HAUtil.isAutomaticFailoverEmbedded(conf)) {
                //创建嵌入式的选主对象
                EmbeddedElector elector = createEmbeddedElector();
                //注册选主对象
                addIfService(elector);
                //为上下文对象注册选主对象
                rmContext.setLeaderElectorService(elector);
            }
        }

        // Set UGI and do login
        // If security is enabled, use login user
        // If security is not enabled, use current user
        this.rmLoginUGI = UserGroupInformation.getCurrentUser();
        try {
            //开始登录
            doSecureLogin();
        } catch (IOException ie) {
            throw new YarnRuntimeException("Failed to login", ie);
        }

        // register the handlers for all AlwaysOn services using setupDispatcher().
        //创建事件分发器对象
        rmDispatcher = setupDispatcher();
        //注册事件分发器对象
        addIfService(rmDispatcher);
        //将该分发器对象注册到上下文对象中
        rmContext.setDispatcher(rmDispatcher);

        //创建管理服务对象
        adminService = createAdminService();
        addService(adminService);
        //向该管理服务对象注册到上下文对象中
        rmContext.setRMAdminService(adminService);

        //设置yarn配置
        rmContext.setYarnConfiguration(conf);

        //创建并初始化激活服务对象
        createAndInitActiveServices(false);

        //读取web应用地址
        webAppAddress = WebAppUtils.getWebAppBindURL(this.conf, YarnConfiguration.RM_BIND_HOST,
            WebAppUtils.getRMWebAppURLWithoutScheme(this.conf));

        //创建记录application历史写入者
        RMApplicationHistoryWriter rmApplicationHistoryWriter = createRMApplicationHistoryWriter();
        addService(rmApplicationHistoryWriter);
        //将该写入者注册到执行上下文对象
        rmContext.setRMApplicationHistoryWriter(rmApplicationHistoryWriter);

        // initialize the RM timeline collector first so that the system metrics
        // publisher can bind to it
        if (YarnConfiguration.timelineServiceV2Enabled(this.conf)) {
            //创建时间线收集管理器对象
            RMTimelineCollectorManager timelineCollectorManager = createRMTimelineCollectorManager();
            addService(timelineCollectorManager);
            //将该时间线收集管理器注册到上下文对象中
            rmContext.setRMTimelineCollectorManager(timelineCollectorManager);
        }

        //创建系统日志统计发布对象
        SystemMetricsPublisher systemMetricsPublisher = createSystemMetricsPublisher();
        //注册该服务对象
        addIfService(systemMetricsPublisher);
        //将该系统日志统计对象注册到上下文对象中
        rmContext.setSystemMetricsPublisher(systemMetricsPublisher);

        //父类服务初始化
        super.serviceInit(this.conf);
    }

    // TODO: 17/3/22 by zmyer
    protected EmbeddedElector createEmbeddedElector() throws IOException {
        EmbeddedElector elector;
        //读取是否开启zk
        curatorEnabled = conf.getBoolean(YarnConfiguration.CURATOR_LEADER_ELECTOR,
            YarnConfiguration.DEFAULT_CURATOR_LEADER_ELECTOR_ENABLED);
        if (curatorEnabled) {
            //创建zk客户端对象
            this.curator = createAndStartCurator(conf);
            //创建基于zk客户端选主服务对象
            elector = new CuratorBasedElectorService(rmContext, this);
        } else {
            //创建独立的选主服务对象
            elector = new ActiveStandbyElectorBasedElectorService(rmContext);
        }
        //返回选主对象
        return elector;
    }

    // TODO: 17/3/22 by zmyer
    public CuratorFramework createAndStartCurator(Configuration conf)
        throws IOException {
        //读取zk主机和端口号
        String zkHostPort = conf.get(YarnConfiguration.RM_ZK_ADDRESS);
        if (zkHostPort == null) {
            throw new YarnRuntimeException(YarnConfiguration.RM_ZK_ADDRESS + " is not configured.");
        }
        //读取重试次数
        int numRetries = conf.getInt(YarnConfiguration.RM_ZK_NUM_RETRIES, YarnConfiguration.DEFAULT_ZK_RM_NUM_RETRIES);
        //读取zk会话超时时间
        int zkSessionTimeout = conf.getInt(YarnConfiguration.RM_ZK_TIMEOUT_MS,
            YarnConfiguration.DEFAULT_RM_ZK_TIMEOUT_MS);
        //读取zk重试时间
        int zkRetryInterval = conf.getInt(YarnConfiguration.RM_ZK_RETRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_ZK_RETRY_INTERVAL_MS);

        // set up zk auths
        //从zk中读取认证信息
        List<ZKUtil.ZKAuthInfo> zkAuths = RMZKUtils.getZKAuths(conf);
        List<AuthInfo> authInfos = new ArrayList<>();
        for (ZKUtil.ZKAuthInfo zkAuth : zkAuths) {
            //将认证信息插入到集合中
            authInfos.add(new AuthInfo(zkAuth.getScheme(), zkAuth.getAuth()));
        }

        if (HAUtil.isHAEnabled(conf) && HAUtil.getConfValueForRMInstance(
            YarnConfiguration.ZK_RM_STATE_STORE_ROOT_NODE_ACL, conf) == null) {
            //读取zk根节点用户名
            String zkRootNodeUsername = HAUtil.getConfValueForRMInstance(YarnConfiguration.RM_ADDRESS,
                YarnConfiguration.DEFAULT_RM_ADDRESS, conf);
            byte[] defaultFencingAuth =
                (zkRootNodeUsername + ":" + zkRootNodePassword).getBytes(Charset.forName("UTF-8"));
            authInfos.add(new AuthInfo(new DigestAuthenticationProvider().getScheme(),
                defaultFencingAuth));
        }

        //创建zk客户端对象
        CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(zkHostPort)
            .sessionTimeoutMs(zkSessionTimeout)
            .retryPolicy(new RetryNTimes(numRetries, zkRetryInterval))
            .authorization(authInfos).build();
        //启动zk客户端对象
        client.start();
        return client;
    }

    // TODO: 17/3/22 by zmyer
    public CuratorFramework getCurator() {
        return this.curator;
    }

    // TODO: 17/3/22 by zmyer
    public String getZkRootNodePassword() {
        return this.zkRootNodePassword;
    }

    // TODO: 17/3/22 by zmyer
    protected QueueACLsManager createQueueACLsManager(ResourceScheduler scheduler, Configuration conf) {
        return new QueueACLsManager(scheduler, conf);
    }

    // TODO: 17/3/22 by zmyer
    @VisibleForTesting
    protected void setRMStateStore(RMStateStore rmStore) {
        rmStore.setRMDispatcher(rmDispatcher);
        rmStore.setResourceManager(this);
        rmContext.setStateStore(rmStore);
    }

    // TODO: 17/3/22 by zmyer
    protected EventHandler<SchedulerEvent> createSchedulerEventDispatcher() {
        //创建事件分发器对象
        return new EventDispatcher(this.scheduler, "SchedulerEventDispatcher");
    }

    // TODO: 17/3/22 by zmyer
    protected Dispatcher createDispatcher() {
        return new AsyncDispatcher();
    }

    // TODO: 17/3/22 by zmyer
    protected ResourceScheduler createScheduler() {
        //读取调度类名
        String schedulerClassName = conf.get(YarnConfiguration.RM_SCHEDULER, YarnConfiguration.DEFAULT_RM_SCHEDULER);
        LOG.info("Using Scheduler: " + schedulerClassName);
        try {
            //首先需要加载该类对象
            Class<?> schedulerClazz = Class.forName(schedulerClassName);
            if (ResourceScheduler.class.isAssignableFrom(schedulerClazz)) {
                //根据类对象实例化调度对象
                return (ResourceScheduler) ReflectionUtils.newInstance(schedulerClazz, this.conf);
            } else {
                throw new YarnRuntimeException("Class: " + schedulerClassName
                    + " not instance of " + ResourceScheduler.class.getCanonicalName());
            }
        } catch (ClassNotFoundException e) {
            throw new YarnRuntimeException("Could not instantiate Scheduler: " + schedulerClassName, e);
        }
    }

    // TODO: 17/3/22 by zmyer
    protected ReservationSystem createReservationSystem() {
        //资源保留类名
        String reservationClassName = conf.get(YarnConfiguration.RM_RESERVATION_SYSTEM_CLASS,
            AbstractReservationSystem.getDefaultReservationSystem(scheduler));
        if (reservationClassName == null) {
            return null;
        }
        LOG.info("Using ReservationSystem: " + reservationClassName);
        try {
            //创建资源保留类对象
            Class<?> reservationClazz = Class.forName(reservationClassName);
            if (ReservationSystem.class.isAssignableFrom(reservationClazz)) {
                //根据类对象创建实例对象
                return (ReservationSystem) ReflectionUtils.newInstance(reservationClazz, this.conf);
            } else {
                throw new YarnRuntimeException("Class: " + reservationClassName
                    + " not instance of " + ReservationSystem.class.getCanonicalName());
            }
        } catch (ClassNotFoundException e) {
            throw new YarnRuntimeException(
                "Could not instantiate ReservationSystem: " + reservationClassName, e);
        }
    }

    // TODO: 17/3/22 by zmyer
    protected ApplicationMasterLauncher createAMLauncher() {
        return new ApplicationMasterLauncher(this.rmContext);
    }

    // TODO: 17/3/22 by zmyer
    private NMLivelinessMonitor createNMLivelinessMonitor() {
        return new NMLivelinessMonitor(this.rmContext.getDispatcher());
    }

    // TODO: 17/3/22 by zmyer
    protected AMLivelinessMonitor createAMLivelinessMonitor() {
        return new AMLivelinessMonitor(this.rmDispatcher);
    }

    // TODO: 17/3/22 by zmyer
    protected RMNodeLabelsManager createNodeLabelManager() throws InstantiationException, IllegalAccessException {
        return new RMNodeLabelsManager();
    }

    // TODO: 17/3/22 by zmyer
    protected DelegationTokenRenewer createDelegationTokenRenewer() {
        return new DelegationTokenRenewer();
    }

    // TODO: 17/3/22 by zmyer
    protected RMAppManager createRMAppManager() {
        return new RMAppManager(this.rmContext, this.scheduler, this.masterService,
            this.applicationACLsManager, this.conf);
    }

    // TODO: 17/3/22 by zmyer
    protected RMApplicationHistoryWriter createRMApplicationHistoryWriter() {
        return new RMApplicationHistoryWriter();
    }

    // TODO: 17/3/22 by zmyer
    private RMTimelineCollectorManager createRMTimelineCollectorManager() {
        return new RMTimelineCollectorManager(rmContext);
    }

    // TODO: 17/3/22 by zmyer
    protected SystemMetricsPublisher createSystemMetricsPublisher() {
        //系统日志统计发布对象
        SystemMetricsPublisher publisher;
        if (YarnConfiguration.timelineServiceEnabled(conf) &&
            YarnConfiguration.systemMetricsPublisherEnabled(conf)) {
            if (YarnConfiguration.timelineServiceV2Enabled(conf)) {
                // we're dealing with the v.2.x publisher
                LOG.info("system metrics publisher with the timeline service V2 is " + "configured");
                //创建基于时间线服务发布者对象
                publisher = new TimelineServiceV2Publisher(rmContext);
            } else {
                // we're dealing with the v.1.x publisher
                LOG.info("system metrics publisher with the timeline service V1 is " + "configured");
                publisher = new TimelineServiceV1Publisher();
            }
        } else {
            LOG.info("TimelineServicePublisher is not configured");
            publisher = new NoOpSystemMetricPublisher();
        }
        //返回发布者对象
        return publisher;
    }

    // TODO: 17/3/22 by zmyer
    // sanity check for configurations
    protected static void validateConfigs(Configuration conf) {
        // validate max-attempts
        //全局最大的应用执行次数
        int globalMaxAppAttempts = conf.getInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS);
        if (globalMaxAppAttempts <= 0) {
            throw new YarnRuntimeException("Invalid global max attempts configuration"
                + ", " + YarnConfiguration.RM_AM_MAX_ATTEMPTS
                + "=" + globalMaxAppAttempts + ", it should be a positive integer.");
        }

        // validate expireIntvl >= heartbeatIntvl
        //超时时间间隔
        long expireIntvl = conf.getLong(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);

        //心跳时间间隔
        long heartbeatIntvl = conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
        if (expireIntvl < heartbeatIntvl) {
            throw new YarnRuntimeException("Nodemanager expiry interval should be no"
                + " less than heartbeat interval, "
                + YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS + "=" + expireIntvl
                + ", " + YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS + "="
                + heartbeatIntvl);
        }
    }

    /**
     * RMActiveServices handles all the Active services in the RM.
     */
    // TODO: 17/3/22 by zmyer
    @Private
    public class RMActiveServices extends CompositeService {
        //token重新创建委托对象
        private DelegationTokenRenewer delegationTokenRenewer;
        //事件调度分发器对象
        private EventHandler<SchedulerEvent> schedulerDispatcher;
        //应用主节点启动器
        private ApplicationMasterLauncher applicationMasterLauncher;
        //容器分配超时对象
        private ContainerAllocationExpirer containerAllocationExpirer;
        //资源管理器
        private ResourceManager rm;
        //激活服务上下文对象
        private RMActiveServiceContext activeServiceContext;
        private boolean fromActive = false;

        // TODO: 17/3/22 by zmyer
        RMActiveServices(ResourceManager rm) {
            super("RMActiveServices");
            this.rm = rm;
        }

        // TODO: 17/3/22 by zmyer
        @Override
        protected void serviceInit(Configuration configuration) throws Exception {
            //创建激活服务上下文对象
            activeServiceContext = new RMActiveServiceContext();
            //设置激活服务上下文对象
            rmContext.setActiveServiceContext(activeServiceContext);

            conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);
            //创建安全管理服务对象
            rmSecretManagerService = createRMSecretManagerService();
            addService(rmSecretManagerService);

            //创建容器分配超时对象
            containerAllocationExpirer = new ContainerAllocationExpirer(rmDispatcher);
            //注册容器分配服务超时对象
            addService(containerAllocationExpirer);
            //在资源执行上下文对象中注册该超时对象
            rmContext.setContainerAllocationExpirer(containerAllocationExpirer);

            //创建节点存活监听器对象
            AMLivelinessMonitor amLivelinessMonitor = createAMLivelinessMonitor();
            //注册监听器对象
            addService(amLivelinessMonitor);
            //将该监听器对象注册到上下文对象
            rmContext.setAMLivelinessMonitor(amLivelinessMonitor);

            //创建节点存活监视器对象
            AMLivelinessMonitor amFinishingMonitor = createAMLivelinessMonitor();
            addService(amFinishingMonitor);
            rmContext.setAMFinishingMonitor(amFinishingMonitor);

            //创建应用生命周期监视器对象
            RMAppLifetimeMonitor rmAppLifetimeMonitor = createRMAppLifetimeMonitor();
            //注册该监视器对象
            addService(rmAppLifetimeMonitor);
            //将该监视器对象注册到上下文对象中
            rmContext.setRMAppLifetimeMonitor(rmAppLifetimeMonitor);

            //创建节点label管理器对象
            RMNodeLabelsManager nlm = createNodeLabelManager();
            //为该管理器注册上下文对象
            nlm.setRMContext(rmContext);
            addService(nlm);
            rmContext.setNodeLabelManager(nlm);

            //创建节点label更新委托对象
            RMDelegatedNodeLabelsUpdater delegatedNodeLabelsUpdater = createRMDelegatedNodeLabelsUpdater();
            if (delegatedNodeLabelsUpdater != null) {
                addService(delegatedNodeLabelsUpdater);
                rmContext.setRMDelegatedNodeLabelsUpdater(delegatedNodeLabelsUpdater);
            }

            //读取恢复标记
            recoveryEnabled = conf.getBoolean(YarnConfiguration.RECOVERY_ENABLED,
                YarnConfiguration.DEFAULT_RM_RECOVERY_ENABLED);

            RMStateStore rmStore;
            if (recoveryEnabled) {
                //读取资源管理器状态存储对象
                rmStore = RMStateStoreFactory.getStore(conf);
                boolean isWorkPreservingRecoveryEnabled = conf.getBoolean(
                    YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_ENABLED,
                    YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_ENABLED);
                rmContext.setWorkPreservingRecoveryEnabled(isWorkPreservingRecoveryEnabled);
            } else {
                //创建null状态存储对象
                rmStore = new NullRMStateStore();
            }

            try {
                //为状态存储对象设置资源管理器对象
                rmStore.setResourceManager(rm);
                //初始化状态存储对象
                rmStore.init(conf);
                //为状态存储对象注册事件分发器对象
                rmStore.setRMDispatcher(rmDispatcher);
            } catch (Exception e) {
                // the Exception from stateStore.init() needs to be handled for
                // HA and we need to give up master status if we got fenced
                LOG.error("Failed to init state store", e);
                throw e;
            }
            //为上下文设置状态存储对象
            rmContext.setStateStore(rmStore);

            if (UserGroupInformation.isSecurityEnabled()) {
                delegationTokenRenewer = createDelegationTokenRenewer();
                rmContext.setDelegationTokenRenewer(delegationTokenRenewer);
            }

            // Register event handler for NodesListManager
            //创建节点列表管理器
            nodesListManager = new NodesListManager(rmContext);
            //将该节点列表管理器注册到事件分发器对象
            rmDispatcher.register(NodesListManagerEventType.class, nodesListManager);
            //将节点列表管理器注册到服务列表中
            addService(nodesListManager);
            //将该节点列表管理器注册到上下文对象中
            rmContext.setNodesListManager(nodesListManager);

            // Initialize the scheduler
            //初始化调度器对象
            scheduler = createScheduler();
            //将上下文对象注册到调度器对象中
            scheduler.setRMContext(rmContext);
            //注册该调度器对象
            addIfService(scheduler);
            //将该调度器注册到上下文对象
            rmContext.setScheduler(scheduler);

            //创建事件分发调度器
            schedulerDispatcher = createSchedulerEventDispatcher();
            //注册该事件分发调度器
            addIfService(schedulerDispatcher);
            //将该事件分发调度器注册到分发器对象
            rmDispatcher.register(SchedulerEventType.class, schedulerDispatcher);

            // Register event handler for RmAppEvents
            //注册appevent事件处理器
            rmDispatcher.register(RMAppEventType.class, new ApplicationEventDispatcher(rmContext));

            // Register event handler for RmAppAttemptEvents
            //注册appAttemptEvent事件处理器
            rmDispatcher.register(RMAppAttemptEventType.class, new ApplicationAttemptEventDispatcher(rmContext));

            // Register event handler for RmNodes
            //注册rmNodes事件处理器
            rmDispatcher.register(RMNodeEventType.class, new NodeEventDispatcher(rmContext));

            //创建节点存活监视器对象
            nmLivelinessMonitor = createNMLivelinessMonitor();
            //将该监视器注册到服务列表中
            addService(nmLivelinessMonitor);

            //创建资源跟踪对象
            resourceTracker = createResourceTrackerService();
            //将该资源跟踪对象注册到服务列表中
            addService(resourceTracker);
            //将该对象注册到上下文对象中
            rmContext.setResourceTrackerService(resourceTracker);

            //创建日志统计对象
            MetricsSystem ms = DefaultMetricsSystem.initialize("ResourceManager");
            if (fromActive) {
                //将jvm日子统计对象注册到ms对象中
                JvmMetrics.reattach(ms, jvmMetrics);
                UserGroupInformation.reattachMetrics();
            } else {
                jvmMetrics = JvmMetrics.initSingleton("ResourceManager", null);
            }

            //创建jvm暂停监视器对象
            JvmPauseMonitor pauseMonitor = new JvmPauseMonitor();
            addService(pauseMonitor);
            jvmMetrics.setPauseMonitor(pauseMonitor);

            // Initialize the Reservation system
            // TODO: 17/3/22 by zmyer
            if (conf.getBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE,
                YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_ENABLE)) {
                reservationSystem = createReservationSystem();
                if (reservationSystem != null) {
                    reservationSystem.setRMContext(rmContext);
                    addIfService(reservationSystem);
                    rmContext.setReservationSystem(reservationSystem);
                    LOG.info("Initialized Reservation system");
                }
            }

            // creating monitors that handle preemption
            createPolicyMonitors();
            //创建application master服务
            masterService = createApplicationMasterService();
            addService(masterService);
            //社会app master服务对象
            rmContext.setApplicationMasterService(masterService);

            //创建application的应用控制列表管理器
            applicationACLsManager = new ApplicationACLsManager(conf);
            queueACLsManager = createQueueACLsManager(scheduler, conf);

            //创建rm app管理器对象
            rmAppManager = createRMAppManager();
            // Register event handler for RMAppManagerEvents
            //将该管理器注册到事件分发对象中
            rmDispatcher.register(RMAppManagerEventType.class, rmAppManager);

            //创建客户端与rm之间通信的服务对象
            clientRM = createClientRMService();
            //将该对象注册到服务列表中
            addService(clientRM);
            //将该服务对象注册到rm执行上下文中
            rmContext.setClientRMService(clientRM);

            //创建app master启动对象
            applicationMasterLauncher = createAMLauncher();
            //将该对象注册到rm事件分发对象中,方便将该相关请求转发到指定的NM上
            rmDispatcher.register(AMLauncherEventType.class, applicationMasterLauncher);
            //将该对象注册到服务列表中
            addService(applicationMasterLauncher);
            if (UserGroupInformation.isSecurityEnabled()) {
                addService(delegationTokenRenewer);
                delegationTokenRenewer.setRMContext(rmContext);
            }

            new RMNMInfo(rmContext, scheduler);
            //启动父类的服务
            super.serviceInit(conf);
        }

        // TODO: 17/4/2 by zmyer
        @Override
        protected void serviceStart() throws Exception {
            RMStateStore rmStore = rmContext.getStateStore();
            // The state store needs to start irrespective of recoveryEnabled as apps
            // need events to move to further states.
            rmStore.start();

            if (recoveryEnabled) {
                try {
                    LOG.info("Recovery started");
                    rmStore.checkVersion();
                    if (rmContext.isWorkPreservingRecoveryEnabled()) {
                        rmContext.setEpoch(rmStore.getAndIncrementEpoch());
                    }
                    RMState state = rmStore.loadState();
                    recover(state);
                    LOG.info("Recovery ended");
                } catch (Exception e) {
                    // the Exception from loadState() needs to be handled for
                    // HA and we need to give up master status if we got fenced
                    LOG.error("Failed to load/recover state", e);
                    throw e;
                }
            }

            super.serviceStart();
        }

        @Override
        protected void serviceStop() throws Exception {

            super.serviceStop();

            DefaultMetricsSystem.shutdown();
            if (rmContext != null) {
                RMStateStore store = rmContext.getStateStore();
                try {
                    if (null != store) {
                        store.close();
                    }
                } catch (Exception e) {
                    LOG.error("Error closing store.", e);
                }
            }

        }

        // TODO: 17/3/22 by zmyer
        protected void createPolicyMonitors() {
            if (scheduler instanceof PreemptableResourceScheduler
                && conf.getBoolean(YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS,
                YarnConfiguration.DEFAULT_RM_SCHEDULER_ENABLE_MONITORS)) {
                LOG.info("Loading policy monitors");
                //读取调度策略集合
                List<SchedulingEditPolicy> policies = conf.getInstances(
                    YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES, SchedulingEditPolicy.class);
                if (policies.size() > 0) {
                    for (SchedulingEditPolicy policy : policies) {
                        LOG.info("LOADING SchedulingEditPolicy:" + policy.getPolicyName());
                        // periodically check whether we need to take action to guarantee
                        // constraints
                        //创建调度监视器对象
                        SchedulingMonitor mon = new SchedulingMonitor(rmContext, policy);
                        //将该监视器对象插入到服务列表中
                        addService(mon);
                    }
                } else {
                    LOG.warn("Policy monitors configured (" +
                        YarnConfiguration.RM_SCHEDULER_ENABLE_MONITORS +
                        ") but none specified (" +
                        YarnConfiguration.RM_SCHEDULER_MONITOR_POLICIES + ")");
                }
            }
        }
    }

    // TODO: 17/3/23 by zmyer
    @Private
    public static class RMFatalEventDispatcher implements EventHandler<RMFatalEvent> {
        // TODO: 17/3/23 by zmyer
        @Override
        public void handle(RMFatalEvent event) {
            LOG.fatal("Received a " + RMFatalEvent.class.getName() + " of type " +
                event.getType().name() + ". Cause:\n" + event.getCause());
            //处理错误事件
            ExitUtil.terminate(1, event.getCause());
        }
    }

    // TODO: 17/3/23 by zmyer
    public void handleTransitionToStandBy() {
        if (rmContext.isHAEnabled()) {
            try {
                // Transition to standby and reinit active services
                LOG.info("Transitioning RM to Standby mode");
                //切换到备节点模式
                transitionToStandby(true);
                //读取选主服务对象
                EmbeddedElector elector = rmContext.getLeaderElectorService();
                if (elector != null) {
                    //如果当前的选主服务不为空,则将该节点加入到选主流程
                    elector.rejoinElection();
                }
            } catch (Exception e) {
                LOG.fatal("Failed to transition RM to Standby mode.", e);
                ExitUtil.terminate(1, e);
            }
        }
    }

    // TODO: 17/3/22 by zmyer
    @Private
    public static final class ApplicationEventDispatcher implements EventHandler<RMAppEvent> {
        //上下文对象
        private final RMContext rmContext;

        // TODO: 17/3/23 by zmyer
        public ApplicationEventDispatcher(RMContext rmContext) {
            this.rmContext = rmContext;
        }

        // TODO: 17/3/23 by zmyer
        @Override
        public void handle(RMAppEvent event) {
            //读取应用id
            ApplicationId appID = event.getApplicationId();
            //根据应用id,读取应用对象
            RMApp rmApp = this.rmContext.getRMApps().get(appID);
            if (rmApp != null) {
                try {
                    //开始处理相关事件
                    rmApp.handle(event);
                } catch (Throwable t) {
                    LOG.error("Error in handling event type " + event.getType()
                        + " for application " + appID, t);
                }
            }
        }
    }

    // TODO: 17/3/22 by zmyer
    @Private
    public static final class ApplicationAttemptEventDispatcher implements EventHandler<RMAppAttemptEvent> {

        private final RMContext rmContext;

        // TODO: 17/3/23 by zmyer
        public ApplicationAttemptEventDispatcher(RMContext rmContext) {
            this.rmContext = rmContext;
        }

        // TODO: 17/3/23 by zmyer
        @Override
        public void handle(RMAppAttemptEvent event) {
            //读取本次应用执行id
            ApplicationAttemptId appAttemptID = event.getApplicationAttemptId();
            //读取应用id
            ApplicationId appAttemptId = appAttemptID.getApplicationId();
            //读取应用对象
            RMApp rmApp = this.rmContext.getRMApps().get(appAttemptId);
            if (rmApp != null) {
                //根据本次执行id,读取本次执行对象
                RMAppAttempt rmAppAttempt = rmApp.getRMAppAttempt(appAttemptID);
                if (rmAppAttempt != null) {
                    try {
                        //开始执行本次事件处理
                        rmAppAttempt.handle(event);
                    } catch (Throwable t) {
                        LOG.error("Error in handling event type " + event.getType()
                            + " for applicationAttempt " + appAttemptId, t);
                    }
                } else if (rmApp.getApplicationSubmissionContext() != null && rmApp.getApplicationSubmissionContext()
                    .getKeepContainersAcrossApplicationAttempts() && event.getType() == RMAppAttemptEventType.CONTAINER_FINISHED) {
                    // For work-preserving AM restart, failed attempts are still
                    // capturing CONTAINER_FINISHED events and record the finished
                    // containers which will be used by current attempt.
                    // We just keep 'yarn.resourcemanager.am.max-attempts' in
                    // RMStateStore. If the finished container's attempt is deleted, we
                    // use the first attempt in app.attempts to deal with these events.

                    //读取上一次执行失败的对象
                    RMAppAttempt previousFailedAttempt = rmApp.getAppAttempts().values().iterator().next();
                    if (previousFailedAttempt != null) {
                        try {
                            LOG.debug("Event " + event.getType() + " handled by " + previousFailedAttempt);
                            //由上一次执行失败的对象处理该事件
                            previousFailedAttempt.handle(event);
                        } catch (Throwable t) {
                            LOG.error("Error in handling event type " + event.getType()
                                + " for applicationAttempt " + appAttemptId
                                + " with " + previousFailedAttempt, t);
                        }
                    } else {
                        LOG.error("Event " + event.getType()
                            + " not handled, because previousFailedAttempt is null");
                    }
                }
            }
        }
    }

    // TODO: 17/3/23 by zmyer
    @Private
    public static final class NodeEventDispatcher implements EventHandler<RMNodeEvent> {

        private final RMContext rmContext;

        // TODO: 17/3/23 by zmyer
        public NodeEventDispatcher(RMContext rmContext) {
            this.rmContext = rmContext;
        }

        // TODO: 17/3/23 by zmyer
        @Override
        public void handle(RMNodeEvent event) {
            //从事件中读取节点id
            NodeId nodeId = event.getNodeId();
            //读取指定的节点对象
            RMNode node = this.rmContext.getRMNodes().get(nodeId);
            if (node != null) {
                try {
                    //开始由指定的节点对象处理该事件
                    ((EventHandler<RMNodeEvent>) node).handle(event);
                } catch (Throwable t) {
                    LOG.error("Error in handling event type " + event.getType()
                        + " for node " + nodeId, t);
                }
            }
        }
    }

    /**
     * Return a HttpServer.Builder that the journalnode / namenode / secondary
     * namenode can use to initialize their HTTP / HTTPS server.
     *
     * @param conf configuration object
     * @param httpAddr HTTP address
     * @param httpsAddr HTTPS address
     * @param name Name of the server
     * @return builder object
     * @throws IOException from Builder
     */
    // TODO: 17/3/23 by zmyer
    public static HttpServer2.Builder httpServerTemplateForRM(Configuration conf,
        final InetSocketAddress httpAddr, final InetSocketAddress httpsAddr,
        String name) throws IOException {
        HttpServer2.Builder builder = new HttpServer2.Builder().setName(name)
            .setConf(conf).setSecurityEnabled(false);

        if (httpAddr.getPort() == 0) {
            builder.setFindPort(true);
        }

        URI uri = URI.create("http://" + NetUtils.getHostPortString(httpAddr));
        builder.addEndpoint(uri);
        LOG.info("Starting Web-server for " + name + " at: " + uri);

        return builder;
    }

    // TODO: 17/3/23 by zmyer
    protected void startWepApp() {

        // Use the customized yarn filter instead of the standard kerberos filter to
        // allow users to authenticate using delegation tokens
        // 4 conditions need to be satisfied -
        // 1. security is enabled
        // 2. http auth type is set to kerberos
        // 3. "yarn.resourcemanager.webapp.use-yarn-filter" override is set to true
        // 4. hadoop.http.filter.initializers container AuthenticationFilterInitializer

        //读取配置对象
        Configuration conf = getConfig();
        boolean enableCorsFilter = conf.getBoolean(YarnConfiguration.RM_WEBAPP_ENABLE_CORS_FILTER,
            YarnConfiguration.DEFAULT_RM_WEBAPP_ENABLE_CORS_FILTER);
        boolean useYarnAuthenticationFilter = conf.getBoolean(
            YarnConfiguration.RM_WEBAPP_DELEGATION_TOKEN_AUTH_FILTER,
            YarnConfiguration.DEFAULT_RM_WEBAPP_DELEGATION_TOKEN_AUTH_FILTER);
        String authPrefix = "hadoop.http.authentication.";
        String authTypeKey = authPrefix + "type";
        String filterInitializerConfKey = "hadoop.http.filter.initializers";
        String actualInitializers = "";
        Class<?>[] initializersClasses = conf.getClasses(filterInitializerConfKey);

        // setup CORS
        if (enableCorsFilter) {
            conf.setBoolean(HttpCrossOriginFilterInitializer.PREFIX
                + HttpCrossOriginFilterInitializer.ENABLED_SUFFIX, true);
        }

        boolean hasHadoopAuthFilterInitializer = false;
        boolean hasRMAuthFilterInitializer = false;
        if (initializersClasses != null) {
            for (Class<?> initializer : initializersClasses) {
                if (initializer.getName().equals(AuthenticationFilterInitializer.class.getName())) {
                    hasHadoopAuthFilterInitializer = true;
                }
                if (initializer.getName().equals(RMAuthenticationFilterInitializer.class.getName())) {
                    hasRMAuthFilterInitializer = true;
                }
            }
            if (UserGroupInformation.isSecurityEnabled()
                && useYarnAuthenticationFilter
                && hasHadoopAuthFilterInitializer
                && conf.get(authTypeKey, "").equals(
                KerberosAuthenticationHandler.TYPE)) {
                ArrayList<String> target = new ArrayList<String>();
                for (Class<?> filterInitializer : initializersClasses) {
                    if (filterInitializer.getName().equals(AuthenticationFilterInitializer.class.getName())) {
                        if (hasRMAuthFilterInitializer == false) {
                            target.add(RMAuthenticationFilterInitializer.class.getName());
                        }
                        continue;
                    }
                    target.add(filterInitializer.getName());
                }
                actualInitializers = StringUtils.join(",", target);

                LOG.info("Using RM authentication filter(kerberos/delegation-token)"
                    + " for RM webapp authentication");
                RMAuthenticationFilter
                    .setDelegationTokenSecretManager(getClientRMService().rmDTSecretManager);
                conf.set(filterInitializerConfKey, actualInitializers);
            }
        }

        // if security is not enabled and the default filter initializer has not
        // been set, set the initializer to include the
        // RMAuthenticationFilterInitializer which in turn will set up the simple
        // auth filter.

        String initializers = conf.get(filterInitializerConfKey);
        if (!UserGroupInformation.isSecurityEnabled()) {
            if (initializersClasses == null || initializersClasses.length == 0) {
                conf.set(filterInitializerConfKey, RMAuthenticationFilterInitializer.class.getName());
                conf.set(authTypeKey, "simple");
            } else if (initializers.equals(StaticUserWebFilter.class.getName())) {
                conf.set(filterInitializerConfKey, RMAuthenticationFilterInitializer.class.getName() + ","
                    + initializers);
                conf.set(authTypeKey, "simple");
            }
        }

        Builder<ApplicationMasterService> builder =
            WebApps.$for("cluster", ApplicationMasterService.class, masterService, "ws")
                .with(conf)
                .withHttpSpnegoPrincipalKey(YarnConfiguration.RM_WEBAPP_SPNEGO_USER_NAME_KEY)
                .withHttpSpnegoKeytabKey(YarnConfiguration.RM_WEBAPP_SPNEGO_KEYTAB_FILE_KEY)
                .withCSRFProtection(YarnConfiguration.RM_CSRF_PREFIX)
                .withXFSProtection(YarnConfiguration.RM_XFS_PREFIX)
                .at(webAppAddress);
        String proxyHostAndPort = WebAppUtils.getProxyHostAndPort(conf);
        if (WebAppUtils.getResolvedRMWebAppURLWithoutScheme(conf).
            equals(proxyHostAndPort)) {
            if (HAUtil.isHAEnabled(conf)) {
                fetcher = new AppReportFetcher(conf);
            } else {
                fetcher = new AppReportFetcher(conf, getClientRMService());
            }
            builder.withServlet(ProxyUriUtils.PROXY_SERVLET_NAME,
                ProxyUriUtils.PROXY_PATH_SPEC, WebAppProxyServlet.class);
            builder.withAttribute(WebAppProxy.FETCHER_ATTRIBUTE, fetcher);
            String[] proxyParts = proxyHostAndPort.split(":");
            builder.withAttribute(WebAppProxy.PROXY_HOST_ATTRIBUTE, proxyParts[0]);
        }

        WebAppContext uiWebAppContext = null;
        if (getConfig().getBoolean(YarnConfiguration.YARN_WEBAPP_UI2_ENABLE,
            YarnConfiguration.DEFAULT_YARN_WEBAPP_UI2_ENABLE)) {
            String webPath = UI2_WEBAPP_NAME;
            String onDiskPath = getConfig()
                .get(YarnConfiguration.YARN_WEBAPP_UI2_WARFILE_PATH);

            if (null == onDiskPath) {
                String war = "hadoop-yarn-ui-" + VersionInfo.getVersion() + ".war";
                URLClassLoader cl = (URLClassLoader) ClassLoader.getSystemClassLoader();
                URL url = cl.findResource(war);

                if (null == url) {
                    onDiskPath = "";
                } else {
                    onDiskPath = url.getFile();
                }

                LOG.info(
                    "New web UI war file name:" + war + ", and path:" + onDiskPath);
            }

            uiWebAppContext = new WebAppContext();
            uiWebAppContext.setContextPath(webPath);
            uiWebAppContext.setWar(onDiskPath);
        }

        webApp = builder.start(new RMWebApp(this), uiWebAppContext);
    }

    /**
     * Helper method to create and init {@link #activeServices}. This creates an
     * instance of {@link RMActiveServices} and initializes it.
     *
     * @param fromActive Indicates if the call is from the active state transition or the RM initialization.
     */
    // TODO: 17/3/23 by zmyer
    protected void createAndInitActiveServices(boolean fromActive) {
        //创建激活服务对象
        activeServices = new RMActiveServices(this);
        activeServices.fromActive = fromActive;
        //初始化激活服务对象
        activeServices.init(conf);
    }

    /**
     * Helper method to start {@link #activeServices}.
     *
     * @throws Exception
     */
    // TODO: 17/3/23 by zmyer
    void startActiveServices() throws Exception {
        if (activeServices != null) {
            //记录当前集群时间
            clusterTimeStamp = System.currentTimeMillis();
            //启动激活服务对象
            activeServices.start();
        }
    }

    /**
     * Helper method to stop {@link #activeServices}.
     *
     * @throws Exception
     */
    // TODO: 17/3/23 by zmyer
    void stopActiveServices() {
        if (activeServices != null) {
            //暂停激活服务
            activeServices.stop();
            activeServices = null;
        }
    }

    // TODO: 17/3/23 by zmyer
    void reinitialize(boolean initialize) {
        //首先需要销毁集之前的集群日志统计对象
        ClusterMetrics.destroy();
        //清空日志统计队列
        QueueMetrics.clearQueueMetrics();
        if (initialize) {
            //重置事件分发器
            resetDispatcher();
            //创建并初始化激活服务对象
            createAndInitActiveServices(true);
        }
    }

    // TODO: 17/3/23 by zmyer
    @VisibleForTesting
    protected boolean areActiveServicesRunning() {
        return activeServices != null && activeServices.isInState(STATE.STARTED);
    }

    // TODO: 17/3/23 by zmyer
    synchronized void transitionToActive() throws Exception {
        //如果处在激活状态,则返回
        if (rmContext.getHAServiceState() == HAServiceProtocol.HAServiceState.ACTIVE) {
            LOG.info("Already in active state");
            return;
        }
        LOG.info("Transitioning to active state");

        this.rmLoginUGI.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                try {
                    //启动激活服务
                    startActiveServices();
                    return null;
                } catch (Exception e) {
                    //如果有异常出现,则直接重新初始化
                    reinitialize(true);
                    throw e;
                }
            }
        });

        //设置当前的HA服务状态为激活状态
        rmContext.setHAServiceState(HAServiceProtocol.HAServiceState.ACTIVE);
        LOG.info("Transitioned to active state");
    }

    // TODO: 17/3/23 by zmyer
    synchronized void transitionToStandby(boolean initialize) throws Exception {
        //如果当前的资源管理器已经处于备节点状态,则直接返回
        if (rmContext.getHAServiceState() == HAServiceProtocol.HAServiceState.STANDBY) {
            LOG.info("Already in standby state");
            return;
        }

        LOG.info("Transitioning to standby state");
        //读取HA服务状态
        HAServiceState state = rmContext.getHAServiceState();
        //设置HA备节点状态
        rmContext.setHAServiceState(HAServiceProtocol.HAServiceState.STANDBY);
        if (state == HAServiceProtocol.HAServiceState.ACTIVE) {
            //如果当前的HA状态已经激活,则需要暂停一下
            stopActiveServices();
            //重新初始化
            reinitialize(initialize);
        }
        LOG.info("Transitioned to standby state");
    }

    // TODO: 17/3/23 by zmyer
    @Override
    protected void serviceStart() throws Exception {
        if (this.rmContext.isHAEnabled()) {
            //如果开启了HA模式,则首先需要将管理器切换到备节点模式
            transitionToStandby(false);
        } else {
            //如果未开启HA,则直接激活管理器
            transitionToActive();
        }

        //启动web app应用
        startWepApp();
        if (getConfig().getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            int port = webApp.port();
            WebAppUtils.setRMWebAppPort(conf, port);
        }
        //启动父类服务
        super.serviceStart();
    }

    // TODO: 17/3/23 by zmyer
    protected void doSecureLogin() throws IOException {
        InetSocketAddress socAddr = getBindAddress(conf);
        SecurityUtil.login(this.conf, YarnConfiguration.RM_KEYTAB,
            YarnConfiguration.RM_PRINCIPAL, socAddr.getHostName());

        // if security is enable, set rmLoginUGI as UGI of loginUser
        if (UserGroupInformation.isSecurityEnabled()) {
            this.rmLoginUGI = UserGroupInformation.getLoginUser();
        }
    }

    // TODO: 17/3/23 by zmyer
    @Override
    protected void serviceStop() throws Exception {
        if (webApp != null) {
            webApp.stop();
        }
        if (fetcher != null) {
            fetcher.stop();
        }
        if (configurationProvider != null) {
            configurationProvider.close();
        }
        super.serviceStop();
        if (curator != null) {
            curator.close();
        }
        //将当前的节点切换备节点状态
        transitionToStandby(false);
        //将HA状态设置为停止
        rmContext.setHAServiceState(HAServiceState.STOPPING);
    }

    // TODO: 17/3/23 by zmyer
    protected ResourceTrackerService createResourceTrackerService() {
        //创建资源跟踪服务对象
        return new ResourceTrackerService(this.rmContext, this.nodesListManager,
            this.nmLivelinessMonitor,
            this.rmContext.getContainerTokenSecretManager(),
            this.rmContext.getNMTokenSecretManager());
    }

    // TODO: 17/3/23 by zmyer
    protected ClientRMService createClientRMService() {
        //创建资源管理器中客户端服务对象
        return new ClientRMService(this.rmContext, scheduler, this.rmAppManager,
            this.applicationACLsManager, this.queueACLsManager,
            this.rmContext.getRMDelegationTokenSecretManager());
    }

    // TODO: 17/3/23 by zmyer
    protected ApplicationMasterService createApplicationMasterService() {
        //读取yarn配置
        Configuration config = this.rmContext.getYarnConfiguration();
        if (YarnConfiguration.isOpportunisticContainerAllocationEnabled(config)
            || YarnConfiguration.isDistSchedulingEnabled(config)) {
            if (YarnConfiguration.isDistSchedulingEnabled(config) &&
                !YarnConfiguration.isOpportunisticContainerAllocationEnabled(config)) {
                throw new YarnRuntimeException(
                    "Invalid parameters: opportunistic container allocation has to " +
                        "be enabled when distributed scheduling is enabled.");
            }
            //创建容器分配服务对象
            OpportunisticContainerAllocatorAMService
                oppContainerAllocatingAMService = new OpportunisticContainerAllocatorAMService(this.rmContext,
                scheduler);
            //容器分配事件分发器
            EventDispatcher oppContainerAllocEventDispatcher = new EventDispatcher(oppContainerAllocatingAMService,
                OpportunisticContainerAllocatorAMService.class.getName());
            // Add an event dispatcher for the
            // OpportunisticContainerAllocatorAMService to handle node
            // additions, updates and removals. Since the SchedulerEvent is currently
            // a super set of theses, we register interest for it.
            //注册容器分配事件分发器
            addService(oppContainerAllocEventDispatcher);
            //将给事件分发器对象注册到总的事件分发器对象中
            rmDispatcher.register(SchedulerEventType.class, oppContainerAllocEventDispatcher);
            //设置容器队列大小限制的计算对象
            this.rmContext.setContainerQueueLimitCalculator(
                oppContainerAllocatingAMService.getNodeManagerQueueLimitCalculator());
            return oppContainerAllocatingAMService;
        }
        //返回应用主节点服务对象
        return new ApplicationMasterService(this.rmContext, scheduler);
    }

    // TODO: 17/3/23 by zmyer
    protected AdminService createAdminService() {
        return new AdminService(this, rmContext);
    }

    // TODO: 17/3/23 by zmyer
    protected RMSecretManagerService createRMSecretManagerService() {
        return new RMSecretManagerService(conf, rmContext);
    }

    /**
     * Create RMDelegatedNodeLabelsUpdater based on configuration.
     */
    // TODO: 17/3/23 by zmyer
    protected RMDelegatedNodeLabelsUpdater createRMDelegatedNodeLabelsUpdater() {
        if (conf.getBoolean(YarnConfiguration.NODE_LABELS_ENABLED,
            YarnConfiguration.DEFAULT_NODE_LABELS_ENABLED)
            && YarnConfiguration.isDelegatedCentralizedNodeLabelConfiguration(
            conf)) {
            return new RMDelegatedNodeLabelsUpdater(rmContext);
        } else {
            return null;
        }
    }

    // TODO: 17/3/23 by zmyer
    @Private
    public ClientRMService getClientRMService() {
        return this.clientRM;
    }

    /**
     * return the scheduler.
     *
     * @return the scheduler for the Resource Manager.
     */
    // TODO: 17/3/23 by zmyer
    @Private
    public ResourceScheduler getResourceScheduler() {
        return this.scheduler;
    }

    /**
     * return the resource tracking component.
     *
     * @return the resource tracking component.
     */
    // TODO: 17/3/23 by zmyer
    @Private
    public ResourceTrackerService getResourceTrackerService() {
        return this.resourceTracker;
    }

    // TODO: 17/3/23 by zmyer
    @Private
    public ApplicationMasterService getApplicationMasterService() {
        return this.masterService;
    }

    // TODO: 17/3/23 by zmyer
    @Private
    public ApplicationACLsManager getApplicationACLsManager() {
        return this.applicationACLsManager;
    }

    // TODO: 17/3/23 by zmyer
    @Private
    public QueueACLsManager getQueueACLsManager() {
        return this.queueACLsManager;
    }

    // TODO: 17/3/23 by zmyer
    @Private
    WebApp getWebapp() {
        return this.webApp;
    }

    // TODO: 17/3/23 by zmyer
    @Override
    public void recover(RMState state) throws Exception {
        // recover RMdelegationTokenSecretManager
        //恢复tokenSecret管理器
        rmContext.getRMDelegationTokenSecretManager().recover(state);

        // recover AMRMTokenSecretManager
        rmContext.getAMRMTokenSecretManager().recover(state);

        // recover reservations
        if (reservationSystem != null) {
            //恢复资源存留系统状态
            reservationSystem.recover(state);
        }
        // recover applications
        //恢复应用管理器对象
        rmAppManager.recover(state);
        //设置调度服务恢复到开始时间间隔
        setSchedulerRecoveryStartAndWaitTime(state, conf);
    }

    // TODO: 17/2/18 by zmyer
    public static void main(String argv[]) {
        //设置线程的未捕获异常的处理句柄
        Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
        //打印资源管理器启动以及关闭日志
        StringUtils.startupShutdownMessage(ResourceManager.class, argv, LOG);
        try {
            //读取yarn配置
            Configuration conf = new YarnConfiguration();
            //创建解析配置对象
            GenericOptionsParser hParser = new GenericOptionsParser(conf, argv);
            //解析参数
            argv = hParser.getRemainingArgs();
            // If -format-state-store, then delete RMStateStore; else startup normally
            if (argv.length >= 1) {
                if (argv[0].equals("-format-state-store")) {
                    deleteRMStateStore(conf);
                } else if (argv[0].equals("-remove-application-from-state-store") && argv.length == 2) {
                    removeApplication(conf, argv[1]);
                } else {
                    printUsage(System.err);
                }
            } else {
                //创建资源管理器对象
                ResourceManager resourceManager = new ResourceManager();
                //设置资源管理器关闭函数
                ShutdownHookManager.get().addShutdownHook(
                    new CompositeServiceShutdownHook(resourceManager),
                    SHUTDOWN_HOOK_PRIORITY);
                //初始化资源管理器
                resourceManager.init(conf);
                //启动资源管理器
                resourceManager.start();
            }
        } catch (Throwable t) {
            LOG.fatal("Error starting ResourceManager", t);
            System.exit(-1);
        }
    }

    /**
     * Register the handlers for alwaysOn services
     */
    // TODO: 17/3/23 by zmyer
    private Dispatcher setupDispatcher() {
        //创建事件分发器对象
        Dispatcher dispatcher = createDispatcher();
        //注册错误消息处理对象
        dispatcher.register(RMFatalEventType.class, new ResourceManager.RMFatalEventDispatcher());
        return dispatcher;
    }

    // TODO: 17/3/23 by zmyer
    private void resetDispatcher() {
        //重新创建事件分发器对象
        Dispatcher dispatcher = setupDispatcher();
        //初始化事件分发器对象
        ((Service) dispatcher).init(this.conf);
        //启动事件分发器
        ((Service) dispatcher).start();
        //从服务列表中删除该事件分发器
        removeService((Service) rmDispatcher);
        // Need to stop previous rmDispatcher before assigning new dispatcher
        // otherwise causes "AsyncDispatcher event handler" thread leak
        //停止事件分发器
        ((Service) rmDispatcher).stop();
        //设置新的事件分发器
        rmDispatcher = dispatcher;
        //将新的事件分发器注册到服务列表中
        addIfService(rmDispatcher);
        //重新注册新的事件分发器
        rmContext.setDispatcher(rmDispatcher);
    }

    // TODO: 17/3/23 by zmyer
    private void setSchedulerRecoveryStartAndWaitTime(RMState state, Configuration conf) {
        if (!state.getApplicationState().isEmpty()) {
            //读取等待时间
            long waitTime = conf.getLong(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
                YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS);
            //设置调度服务恢复到开始的时间间隔
            rmContext.setSchedulerRecoveryStartAndWaitTime(waitTime);
        }
    }

    /**
     * Retrieve RM bind address from configuration
     *
     * @param conf
     * @return InetSocketAddress
     */
    // TODO: 17/3/23 by zmyer
    public static InetSocketAddress getBindAddress(Configuration conf) {
        return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
            YarnConfiguration.DEFAULT_RM_ADDRESS, YarnConfiguration.DEFAULT_RM_PORT);
    }

    /**
     * Deletes the RMStateStore
     *
     * @param conf
     * @throws Exception
     */
    // TODO: 17/3/23 by zmyer
    @VisibleForTesting
    static void deleteRMStateStore(Configuration conf) throws Exception {
        //创建状态存储对象
        RMStateStore rmStore = RMStateStoreFactory.getStore(conf);
        //设置资源管理器
        rmStore.setResourceManager(new ResourceManager());
        //初始化状态存储对象
        rmStore.init(conf);
        //启动状态存储对象
        rmStore.start();
        try {
            LOG.info("Deleting ResourceManager state store...");
            //删除存储对象
            rmStore.deleteStore();
            LOG.info("State store deleted");
        } finally {
            rmStore.stop();
        }
    }

    // TODO: 17/3/23 by zmyer
    @VisibleForTesting
    static void removeApplication(Configuration conf, String applicationId) throws Exception {
        //首先根据配置读取状态存储对象
        RMStateStore rmStore = RMStateStoreFactory.getStore(conf);
        //设置资源管理器
        rmStore.setResourceManager(new ResourceManager());
        //初始化状态存储对象
        rmStore.init(conf);
        //启动状态存储对象
        rmStore.start();
        try {
            //读取应用id
            ApplicationId removeAppId = ApplicationId.fromString(applicationId);
            LOG.info("Deleting application " + removeAppId + " from state store");
            //从存储对象中删除应用
            rmStore.removeApplication(removeAppId);
            LOG.info("Application is deleted from state store");
        } finally {
            //关闭存储对象
            rmStore.stop();
        }
    }

    // TODO: 17/3/23 by zmyer
    private static void printUsage(PrintStream out) {
        out.println("Usage: yarn resourcemanager [-format-state-store]");
        out.println("                            "
            + "[-remove-application-from-state-store <appId>]" + "\n");
    }

    // TODO: 17/3/23 by zmyer
    protected RMAppLifetimeMonitor createRMAppLifetimeMonitor() {
        return new RMAppLifetimeMonitor(this.rmContext);
    }
}
