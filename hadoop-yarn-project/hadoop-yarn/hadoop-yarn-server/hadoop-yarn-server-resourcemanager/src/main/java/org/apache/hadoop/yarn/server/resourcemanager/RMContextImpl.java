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
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMDelegatedNodeLabelsUpdater;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.monitor.RMAppLifetimeMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed.QueueLimitCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;
import org.apache.hadoop.yarn.util.Clock;

// TODO: 17/2/19 by zmyer
public class RMContextImpl implements RMContext {
    //事件分发对象
    private Dispatcher rmDispatcher;
    //是否开启HA
    private boolean isHAEnabled;
    //HA状态服务对象
    private HAServiceState haServiceState = HAServiceProtocol.HAServiceState.INITIALIZING;
    //管理服务对象
    private AdminService adminService;
    //配置提供者
    private ConfigurationProvider configurationProvider;
    //激活服务上下文对象
    private RMActiveServiceContext activeServiceContext;
    //yarn配置对象
    private Configuration yarnConfiguration;
    private RMApplicationHistoryWriter rmApplicationHistoryWriter;
    private SystemMetricsPublisher systemMetricsPublisher;
    //嵌入式选择器对象
    private EmbeddedElector elector;
    private QueueLimitCalculator queueLimitCalculator;
    private final Object haServiceStateLock = new Object();

    /**
     * Default constructor. To be used in conjunction with setter methods for
     * individual fields.
     */
    public RMContextImpl() {
    }

    // TODO: 17/2/20 by zmyer
    @VisibleForTesting
    // helper constructor for tests
    public RMContextImpl(Dispatcher rmDispatcher,
        ContainerAllocationExpirer containerAllocationExpirer,
        AMLivelinessMonitor amLivelinessMonitor,
        AMLivelinessMonitor amFinishingMonitor,
        DelegationTokenRenewer delegationTokenRenewer,
        AMRMTokenSecretManager appTokenSecretManager,
        RMContainerTokenSecretManager containerTokenSecretManager,
        NMTokenSecretManagerInRM nmTokenSecretManager,
        ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager,
        ResourceScheduler scheduler) {
        this();
        this.setDispatcher(rmDispatcher);
        setActiveServiceContext(new RMActiveServiceContext(rmDispatcher,
            containerAllocationExpirer, amLivelinessMonitor, amFinishingMonitor,
            delegationTokenRenewer, appTokenSecretManager,
            containerTokenSecretManager, nmTokenSecretManager,
            clientToAMTokenSecretManager,
            scheduler));

        ConfigurationProvider provider = new LocalConfigurationProvider();
        setConfigurationProvider(provider);
    }

    // TODO: 17/2/21 by zmyer
    @VisibleForTesting
    // helper constructor for tests
    public RMContextImpl(Dispatcher rmDispatcher,
        ContainerAllocationExpirer containerAllocationExpirer,
        AMLivelinessMonitor amLivelinessMonitor,
        AMLivelinessMonitor amFinishingMonitor,
        DelegationTokenRenewer delegationTokenRenewer,
        AMRMTokenSecretManager appTokenSecretManager,
        RMContainerTokenSecretManager containerTokenSecretManager,
        NMTokenSecretManagerInRM nmTokenSecretManager,
        ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager) {
        this(rmDispatcher,
            containerAllocationExpirer,
            amLivelinessMonitor,
            amFinishingMonitor,
            delegationTokenRenewer,
            appTokenSecretManager,
            containerTokenSecretManager,
            nmTokenSecretManager,
            clientToAMTokenSecretManager, null);
    }

    // TODO: 17/2/22 by zmyer
    @Override
    public Dispatcher getDispatcher() {
        return this.rmDispatcher;
    }

    // TODO: 17/2/23 by zmyer
    @Override
    public void setLeaderElectorService(EmbeddedElector elector) {
        this.elector = elector;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public EmbeddedElector getLeaderElectorService() {
        return this.elector;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public RMStateStore getStateStore() {
        return activeServiceContext.getStateStore();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public ConcurrentMap<ApplicationId, RMApp> getRMApps() {
        return activeServiceContext.getRMApps();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public ConcurrentMap<NodeId, RMNode> getRMNodes() {
        return activeServiceContext.getRMNodes();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public ConcurrentMap<NodeId, RMNode> getInactiveRMNodes() {
        return activeServiceContext.getInactiveRMNodes();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public ContainerAllocationExpirer getContainerAllocationExpirer() {
        return activeServiceContext.getContainerAllocationExpirer();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public AMLivelinessMonitor getAMLivelinessMonitor() {
        return activeServiceContext.getAMLivelinessMonitor();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public AMLivelinessMonitor getAMFinishingMonitor() {
        return activeServiceContext.getAMFinishingMonitor();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public DelegationTokenRenewer getDelegationTokenRenewer() {
        return activeServiceContext.getDelegationTokenRenewer();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public AMRMTokenSecretManager getAMRMTokenSecretManager() {
        return activeServiceContext.getAMRMTokenSecretManager();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public RMContainerTokenSecretManager getContainerTokenSecretManager() {
        return activeServiceContext.getContainerTokenSecretManager();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public NMTokenSecretManagerInRM getNMTokenSecretManager() {
        return activeServiceContext.getNMTokenSecretManager();
    }

    @Override
    // TODO: 17/4/3 by zmyer
    public ResourceScheduler getScheduler() {
        return activeServiceContext.getScheduler();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public ReservationSystem getReservationSystem() {
        return activeServiceContext.getReservationSystem();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public NodesListManager getNodesListManager() {
        return activeServiceContext.getNodesListManager();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager() {
        return activeServiceContext.getClientToAMTokenSecretManager();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public AdminService getRMAdminService() {
        return this.adminService;
    }

    // TODO: 17/4/3 by zmyer
    @VisibleForTesting
    public void setStateStore(RMStateStore store) {
        activeServiceContext.setStateStore(store);
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public ClientRMService getClientRMService() {
        return activeServiceContext.getClientRMService();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public ApplicationMasterService getApplicationMasterService() {
        return activeServiceContext.getApplicationMasterService();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public ResourceTrackerService getResourceTrackerService() {
        return activeServiceContext.getResourceTrackerService();
    }

    // TODO: 17/3/23 by zmyer
    void setHAEnabled(boolean isHAEnabled) {
        this.isHAEnabled = isHAEnabled;
    }

    // TODO: 17/4/3 by zmyer
    void setHAServiceState(HAServiceState serviceState) {
        synchronized (haServiceStateLock) {
            this.haServiceState = serviceState;
        }
    }

    // TODO: 17/4/3 by zmyer
    void setDispatcher(Dispatcher dispatcher) {
        this.rmDispatcher = dispatcher;
    }

    // TODO: 17/4/3 by zmyer
    void setRMAdminService(AdminService adminService) {
        this.adminService = adminService;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public void setClientRMService(ClientRMService clientRMService) {
        activeServiceContext.setClientRMService(clientRMService);
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public RMDelegationTokenSecretManager getRMDelegationTokenSecretManager() {
        return activeServiceContext.getRMDelegationTokenSecretManager();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public void setRMDelegationTokenSecretManager(
        RMDelegationTokenSecretManager delegationTokenSecretManager) {
        activeServiceContext.setRMDelegationTokenSecretManager(delegationTokenSecretManager);
    }

    // TODO: 17/4/3 by zmyer
    void setContainerAllocationExpirer(ContainerAllocationExpirer containerAllocationExpirer) {
        activeServiceContext.setContainerAllocationExpirer(containerAllocationExpirer);
    }

    // TODO: 17/4/3 by zmyer
    void setAMLivelinessMonitor(AMLivelinessMonitor amLivelinessMonitor) {
        activeServiceContext.setAMLivelinessMonitor(amLivelinessMonitor);
    }

    // TODO: 17/3/22 by zmyer
    void setAMFinishingMonitor(AMLivelinessMonitor amFinishingMonitor) {
        activeServiceContext.setAMFinishingMonitor(amFinishingMonitor);
    }

    // TODO: 17/4/3 by zmyer
    void setContainerTokenSecretManager(RMContainerTokenSecretManager containerTokenSecretManager) {
        activeServiceContext.setContainerTokenSecretManager(containerTokenSecretManager);
    }

    // TODO: 17/4/3 by zmyer
    void setNMTokenSecretManager(NMTokenSecretManagerInRM nmTokenSecretManager) {
        activeServiceContext.setNMTokenSecretManager(nmTokenSecretManager);
    }

    // TODO: 17/4/3 by zmyer
    @VisibleForTesting
    public void setScheduler(ResourceScheduler scheduler) {
        activeServiceContext.setScheduler(scheduler);
    }

    // TODO: 17/4/3 by zmyer
    void setReservationSystem(ReservationSystem reservationSystem) {
        activeServiceContext.setReservationSystem(reservationSystem);
    }

    // TODO: 17/4/3 by zmyer
    void setDelegationTokenRenewer(DelegationTokenRenewer delegationTokenRenewer) {
        activeServiceContext.setDelegationTokenRenewer(delegationTokenRenewer);
    }

    // TODO: 17/4/3 by zmyer
    void setClientToAMTokenSecretManager(ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager) {
        activeServiceContext.setClientToAMTokenSecretManager(clientToAMTokenSecretManager);
    }

    // TODO: 17/4/3 by zmyer
    void setAMRMTokenSecretManager(AMRMTokenSecretManager amRMTokenSecretManager) {
        activeServiceContext.setAMRMTokenSecretManager(amRMTokenSecretManager);
    }

    // TODO: 17/4/3 by zmyer
    void setNodesListManager(NodesListManager nodesListManager) {
        activeServiceContext.setNodesListManager(nodesListManager);
    }

    // TODO: 17/4/3 by zmyer
    void setApplicationMasterService(
        ApplicationMasterService applicationMasterService) {
        activeServiceContext.setApplicationMasterService(applicationMasterService);
    }

    // TODO: 17/4/3 by zmyer
    void setResourceTrackerService(ResourceTrackerService resourceTrackerService) {
        activeServiceContext.setResourceTrackerService(resourceTrackerService);
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public boolean isHAEnabled() {
        return isHAEnabled;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public HAServiceState getHAServiceState() {
        synchronized (haServiceStateLock) {
            return haServiceState;
        }
    }

    // TODO: 17/4/3 by zmyer
    public void setWorkPreservingRecoveryEnabled(boolean enabled) {
        activeServiceContext.setWorkPreservingRecoveryEnabled(enabled);
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public boolean isWorkPreservingRecoveryEnabled() {
        return activeServiceContext.isWorkPreservingRecoveryEnabled();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public RMApplicationHistoryWriter getRMApplicationHistoryWriter() {
        return this.rmApplicationHistoryWriter;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public void setRMTimelineCollectorManager(RMTimelineCollectorManager timelineCollectorManager) {
        activeServiceContext.setRMTimelineCollectorManager(timelineCollectorManager);
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public RMTimelineCollectorManager getRMTimelineCollectorManager() {
        return activeServiceContext.getRMTimelineCollectorManager();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public void setSystemMetricsPublisher(SystemMetricsPublisher metricsPublisher) {
        this.systemMetricsPublisher = metricsPublisher;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public SystemMetricsPublisher getSystemMetricsPublisher() {
        return this.systemMetricsPublisher;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public void setRMApplicationHistoryWriter(RMApplicationHistoryWriter rmApplicationHistoryWriter) {
        this.rmApplicationHistoryWriter = rmApplicationHistoryWriter;

    }

    // TODO: 17/4/3 by zmyer
    @Override
    public ConfigurationProvider getConfigurationProvider() {
        return this.configurationProvider;
    }

    // TODO: 17/3/23 by zmyer
    public void setConfigurationProvider(ConfigurationProvider configurationProvider) {
        //设置配置提供者对象
        this.configurationProvider = configurationProvider;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public long getEpoch() {
        return activeServiceContext.getEpoch();
    }

    // TODO: 17/4/3 by zmyer
    void setEpoch(long epoch) {
        activeServiceContext.setEpoch(epoch);
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public RMNodeLabelsManager getNodeLabelManager() {
        return activeServiceContext.getNodeLabelManager();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public void setNodeLabelManager(RMNodeLabelsManager mgr) {
        activeServiceContext.setNodeLabelManager(mgr);
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public RMDelegatedNodeLabelsUpdater getRMDelegatedNodeLabelsUpdater() {
        return activeServiceContext.getRMDelegatedNodeLabelsUpdater();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public void setRMDelegatedNodeLabelsUpdater(RMDelegatedNodeLabelsUpdater delegatedNodeLabelsUpdater) {
        activeServiceContext.setRMDelegatedNodeLabelsUpdater(delegatedNodeLabelsUpdater);
    }

    // TODO: 17/4/3 by zmyer
    public void setSchedulerRecoveryStartAndWaitTime(long waitTime) {
        activeServiceContext.setSchedulerRecoveryStartAndWaitTime(waitTime);
    }

    // TODO: 17/4/3 by zmyer
    public boolean isSchedulerReadyForAllocatingContainers() {
        return activeServiceContext.isSchedulerReadyForAllocatingContainers();
    }

    // TODO: 17/4/3 by zmyer
    @Private
    @VisibleForTesting
    public void setSystemClock(Clock clock) {
        activeServiceContext.setSystemClock(clock);
    }

    // TODO: 17/4/3 by zmyer
    public ConcurrentMap<ApplicationId, ByteBuffer> getSystemCredentialsForApps() {
        return activeServiceContext.getSystemCredentialsForApps();
    }

    // TODO: 17/4/3 by zmyer
    @Private
    @Unstable
    public RMActiveServiceContext getActiveServiceContext() {
        return activeServiceContext;
    }

    // TODO: 17/4/3 by zmyer
    @Private
    @Unstable
    void setActiveServiceContext(RMActiveServiceContext activeServiceContext) {
        this.activeServiceContext = activeServiceContext;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public Configuration getYarnConfiguration() {
        return this.yarnConfiguration;
    }

    // TODO: 17/3/24 by zmyer
    public void setYarnConfiguration(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public PlacementManager getQueuePlacementManager() {
        return this.activeServiceContext.getQueuePlacementManager();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public void setQueuePlacementManager(PlacementManager placementMgr) {
        this.activeServiceContext.setQueuePlacementManager(placementMgr);
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public QueueLimitCalculator getNodeManagerQueueLimitCalculator() {
        return this.queueLimitCalculator;
    }

    // TODO: 17/3/23 by zmyer
    public void setContainerQueueLimitCalculator(QueueLimitCalculator limitCalculator) {
        this.queueLimitCalculator = limitCalculator;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public void setRMAppLifetimeMonitor(RMAppLifetimeMonitor rmAppLifetimeMonitor) {
        this.activeServiceContext.setRMAppLifetimeMonitor(rmAppLifetimeMonitor);
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public RMAppLifetimeMonitor getRMAppLifetimeMonitor() {
        return this.activeServiceContext.getRMAppLifetimeMonitor();
    }

    // TODO: 17/4/3 by zmyer
    public String getHAZookeeperConnectionState() {
        if (elector == null) {
            return "Could not find leader elector. Verify both HA and automatic " +
                "failover are enabled.";
        } else {
            return elector.getZookeeperConnectionState();
        }
    }
}
