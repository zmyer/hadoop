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

package org.apache.hadoop.yarn.client.api.async;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.async.impl.AMRMClientAsyncImpl;
import org.apache.hadoop.yarn.client.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * <code>AMRMClientAsync</code> handles communication with the ResourceManager
 * and provides asynchronous updates on events such as container allocations and
 * completions.  It contains a thread that sends periodic heartbeats to the
 * ResourceManager.
 *
 * It should be used by implementing a CallbackHandler:
 * <pre>
 * {@code
 * class MyCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {
 *   public void onContainersAllocated(List<Container> containers) {
 *     [run tasks on the containers]
 *   }
 *
 *   public void onContainersUpdated(List<Container> containers) {
 *     [determine if resource allocation of containers have been increased in
 *      the ResourceManager, and if so, inform the NodeManagers to increase the
 *      resource monitor/enforcement on the containers]
 *   }
 *
 *   public void onContainersCompleted(List<ContainerStatus> statuses) {
 *     [update progress, check whether app is done]
 *   }
 *
 *   public void onNodesUpdated(List<NodeReport> updated) {}
 *
 *   public void onReboot() {}
 * }
 * }
 * </pre>
 *
 * The client's lifecycle should be managed similarly to the following:
 *
 * <pre>
 * {@code
 * AMRMClientAsync asyncClient =
 *     createAMRMClientAsync(appAttId, 1000, new MyCallbackhandler());
 * asyncClient.init(conf);
 * asyncClient.start();
 * RegisterApplicationMasterResponse response = asyncClient
 *    .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
 *       appMasterTrackingUrl);
 * asyncClient.addContainerRequest(containerRequest);
 * [... wait for application to complete]
 * asyncClient.unregisterApplicationMaster(status, appMsg, trackingUrl);
 * asyncClient.stop();
 * }
 * </pre>
 */
@Public
@Stable
// TODO: 17/3/25 by zmyer
public abstract class AMRMClientAsync<T extends ContainerRequest>
    extends AbstractService {
    private static final Log LOG = LogFactory.getLog(AMRMClientAsync.class);
    //客户端对象
    protected final AMRMClient<T> client;
    //回调函数对象
    protected final CallbackHandler handler;
    //心跳时间间隔
    protected final AtomicInteger heartbeatIntervalMs = new AtomicInteger();

    /**
     * <p>Create a new instance of AMRMClientAsync.</p>
     *
     * @param intervalMs heartbeat interval in milliseconds between AM and RM
     * @param callbackHandler callback handler that processes responses from the <code>ResourceManager</code>
     */
    // TODO: 17/3/25 by zmyer
    public static <T extends ContainerRequest> AMRMClientAsync<T>
    createAMRMClientAsync(int intervalMs, AbstractCallbackHandler callbackHandler) {
        //创建异步模式下的amClient对象
        return new AMRMClientAsyncImpl<T>(intervalMs, callbackHandler);
    }

    /**
     * <p>Create a new instance of AMRMClientAsync.</p>
     *
     * @param client the AMRMClient instance
     * @param intervalMs heartbeat interval in milliseconds between AM and RM
     * @param callbackHandler callback handler that processes responses from the <code>ResourceManager</code>
     */
    // TODO: 17/3/26 by zmyer
    public static <T extends ContainerRequest> AMRMClientAsync<T>
    createAMRMClientAsync(AMRMClient<T> client, int intervalMs,
        AbstractCallbackHandler callbackHandler) {
        //创建异步模式下的amClient对象
        return new AMRMClientAsyncImpl<T>(client, intervalMs, callbackHandler);
    }

    // TODO: 17/3/26 by zmyer
    protected AMRMClientAsync(int intervalMs, AbstractCallbackHandler callbackHandler) {
        this(new AMRMClientImpl<T>(), intervalMs, callbackHandler);
    }

    // TODO: 17/3/26 by zmyer
    @Private
    @VisibleForTesting
    protected AMRMClientAsync(AMRMClient<T> client, int intervalMs,
        AbstractCallbackHandler callbackHandler) {
        super(AMRMClientAsync.class.getName());
        this.client = client;
        this.heartbeatIntervalMs.set(intervalMs);
        this.handler = callbackHandler;
    }

    /**
     * @deprecated Use {@link #createAMRMClientAsync(int, AMRMClientAsync.AbstractCallbackHandler)} instead.
     */
    // TODO: 17/3/26 by zmyer
    @Deprecated
    public static <T extends ContainerRequest> AMRMClientAsync<T>
    createAMRMClientAsync(int intervalMs, CallbackHandler callbackHandler) {
        return new AMRMClientAsyncImpl<T>(intervalMs, callbackHandler);
    }

    /**
     * @deprecated Use {@link #createAMRMClientAsync(AMRMClient, int, AMRMClientAsync.AbstractCallbackHandler)} instead.
     */
    // TODO: 17/3/26 by zmyer
    @Deprecated
    public static <T extends ContainerRequest> AMRMClientAsync<T>
    createAMRMClientAsync(AMRMClient<T> client, int intervalMs,
        CallbackHandler callbackHandler) {
        return new AMRMClientAsyncImpl<T>(client, intervalMs, callbackHandler);
    }

    // TODO: 17/3/26 by zmyer
    @Deprecated
    protected AMRMClientAsync(int intervalMs, CallbackHandler callbackHandler) {
        this(new AMRMClientImpl<T>(), intervalMs, callbackHandler);
    }

    @Private
    @VisibleForTesting
    @Deprecated
    // TODO: 17/3/26 by zmyer
    protected AMRMClientAsync(AMRMClient<T> client, int intervalMs,
        CallbackHandler callbackHandler) {
        super(AMRMClientAsync.class.getName());
        this.client = client;
        this.heartbeatIntervalMs.set(intervalMs);
        this.handler = callbackHandler;
    }

    // TODO: 17/3/26 by zmyer
    public void setHeartbeatInterval(int interval) {
        heartbeatIntervalMs.set(interval);
    }

    // TODO: 17/3/26 by zmyer
    public abstract List<? extends Collection<T>> getMatchingRequests(
        Priority priority,
        String resourceName,
        Resource capability);

    /**
     * Returns all matching ContainerRequests that match the given Priority,
     * ResourceName, ExecutionType and Capability.
     *
     * NOTE: This matches only requests that were made by the client WITHOUT the
     * allocationRequestId specified.
     *
     * @param priority Priority.
     * @param resourceName Location.
     * @param executionType ExecutionType.
     * @param capability Capability.
     * @return All matching ContainerRequests
     */
    // TODO: 17/3/26 by zmyer
    public List<? extends Collection<T>> getMatchingRequests(
        Priority priority, String resourceName, ExecutionType executionType,
        Resource capability) {
        //返回匹配的请求列表
        return client.getMatchingRequests(priority, resourceName,
            executionType, capability);
    }

    /**
     * Returns all matching ContainerRequests that match the given
     * AllocationRequestId.
     *
     * NOTE: This matches only requests that were made by the client WITH the
     * allocationRequestId specified.
     *
     * @param allocationRequestId AllocationRequestId.
     * @return All matching ContainerRequests
     */
    // TODO: 17/3/26 by zmyer
    public Collection<T> getMatchingRequests(long allocationRequestId) {
        return client.getMatchingRequests(allocationRequestId);
    }

    /**
     * Registers this application master with the resource manager. On successful
     * registration, starts the heartbeating thread.
     *
     * @throws YarnException
     * @throws IOException
     */
    // TODO: 17/3/26 by zmyer
    public abstract RegisterApplicationMasterResponse registerApplicationMaster(
        String appHostName, int appHostPort, String appTrackingUrl)
        throws YarnException, IOException;

    /**
     * Unregister the application master. This must be called in the end.
     *
     * @param appStatus Success/Failure status of the master
     * @param appMessage Diagnostics message on failure
     * @param appTrackingUrl New URL to get master info
     * @throws YarnException
     * @throws IOException
     */
    // TODO: 17/3/26 by zmyer
    public abstract void unregisterApplicationMaster(
        FinalApplicationStatus appStatus, String appMessage, String appTrackingUrl)
        throws YarnException, IOException;

    /**
     * Request containers for resources before calling <code>allocate</code>
     *
     * @param req Resource request
     */
    // TODO: 17/3/26 by zmyer
    public abstract void addContainerRequest(T req);

    /**
     * Remove previous container request. The previous container request may have
     * already been sent to the ResourceManager. So even after the remove request
     * the app must be prepared to receive an allocation for the previous request
     * even after the remove request
     *
     * @param req Resource request
     */
    // TODO: 17/3/26 by zmyer
    public abstract void removeContainerRequest(T req);

    /**
     * Request container resource change before calling <code>allocate</code>.
     * Any previous pending resource change request of the same container will be
     * removed.
     *
     * Application that calls this method is expected to maintain the
     * <code>Container</code>s that are returned from previous successful
     * allocations or resource changes. By passing in the existing container and a
     * target resource capability to this method, the application requests the
     * ResourceManager to change the existing resource allocation to the target
     * resource allocation.
     *
     * @param container The container returned from the last successful resource allocation or resource change
     * @param capability The target resource capability of the container
     */
    // TODO: 17/3/26 by zmyer
    public abstract void requestContainerResourceChange(
        Container container, Resource capability);

    /**
     * Release containers assigned by the Resource Manager. If the app cannot use
     * the container or wants to give up the container then it can release them.
     * The app needs to make new requests for the released resource capability if
     * it still needs it. eg. it released non-local resources
     *
     * @param containerId
     */
    // TODO: 17/3/26 by zmyer
    public abstract void releaseAssignedContainer(ContainerId containerId);

    /**
     * Get the currently available resources in the cluster.
     * A valid value is available after a call to allocate has been made
     *
     * @return Currently available resources
     */
    // TODO: 17/3/26 by zmyer
    public abstract Resource getAvailableResources();

    /**
     * Get the current number of nodes in the cluster.
     * A valid values is available after a call to allocate has been made
     *
     * @return Current number of nodes in the cluster
     */
    // TODO: 17/3/26 by zmyer
    public abstract int getClusterNodeCount();

    /**
     * Register TimelineClient to AMRMClient.
     *
     * @param timelineClient
     */
    // TODO: 17/3/26 by zmyer
    public void registerTimelineClient(TimelineClient timelineClient) {
        client.registerTimelineClient(timelineClient);
    }

    /**
     * Get registered timeline client.
     *
     * @return the registered timeline client
     */
    // TODO: 17/3/26 by zmyer
    public TimelineClient getRegisteredTimelineClient() {
        return client.getRegisteredTimelineClient();
    }

    /**
     * Update application's blacklist with addition or removal resources.
     *
     * @param blacklistAdditions list of resources which should be added to the application blacklist
     * @param blacklistRemovals list of resources which should be removed from the application blacklist
     */
    // TODO: 17/3/26 by zmyer
    public abstract void updateBlacklist(List<String> blacklistAdditions,
        List<String> blacklistRemovals);

    /**
     * Wait for <code>check</code> to return true for each 1000 ms.
     * See also {@link #waitFor(com.google.common.base.Supplier, int)}
     * and {@link #waitFor(com.google.common.base.Supplier, int, int)}
     *
     * @param check the condition for which it should wait
     */
    // TODO: 17/3/26 by zmyer
    public void waitFor(Supplier<Boolean> check) throws InterruptedException {
        waitFor(check, 1000);
    }

    /**
     * Wait for <code>check</code> to return true for each
     * <code>checkEveryMillis</code> ms.
     * See also {@link #waitFor(com.google.common.base.Supplier, int, int)}
     *
     * @param check user defined checker
     * @param checkEveryMillis interval to call <code>check</code>
     */
    // TODO: 17/3/26 by zmyer
    public void waitFor(Supplier<Boolean> check, int checkEveryMillis)
        throws InterruptedException {
        waitFor(check, checkEveryMillis, 1);
    }

    ;

    /**
     * Wait for <code>check</code> to return true for each
     * <code>checkEveryMillis</code> ms. In the main loop, this method will log
     * the message "waiting in main loop" for each <code>logInterval</code> times
     * iteration to confirm the thread is alive.
     *
     * @param check user defined checker
     * @param checkEveryMillis interval to call <code>check</code>
     * @param logInterval interval to log for each
     */
    // TODO: 17/3/26 by zmyer
    public void waitFor(Supplier<Boolean> check, int checkEveryMillis,
        int logInterval) throws InterruptedException {
        Preconditions.checkNotNull(check, "check should not be null");
        Preconditions.checkArgument(checkEveryMillis >= 0,
            "checkEveryMillis should be positive value");
        Preconditions.checkArgument(logInterval >= 0,
            "logInterval should be positive value");

        int loggingCounter = logInterval;
        do {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Check the condition for main loop.");
            }

            boolean result = check.get();
            if (result) {
                LOG.info("Exits the main loop.");
                return;
            }
            if (--loggingCounter <= 0) {
                LOG.info("Waiting in main loop.");
                loggingCounter = logInterval;
            }

            Thread.sleep(checkEveryMillis);
        }
        while (true);
    }

    /**
     * <p>
     * The callback abstract class. The callback functions need to be implemented
     * by {@link AMRMClientAsync} users. The APIs are called when responses from
     * the <code>ResourceManager</code> are available.
     * </p>
     */
    // TODO: 17/3/25 by zmyer
    public abstract static class AbstractCallbackHandler implements CallbackHandler {

        /**
         * Called when the ResourceManager responds to a heartbeat with completed
         * containers. If the response contains both completed containers and
         * allocated containers, this will be called before containersAllocated.
         */
        // TODO: 17/3/25 by zmyer
        public abstract void onContainersCompleted(List<ContainerStatus> statuses);

        /**
         * Called when the ResourceManager responds to a heartbeat with allocated
         * containers. If the response containers both completed containers and
         * allocated containers, this will be called after containersCompleted.
         */
        // TODO: 17/3/25 by zmyer
        public abstract void onContainersAllocated(List<Container> containers);

        /**
         * Called when the ResourceManager responds to a heartbeat with containers
         * whose resource allocation has been changed.
         */
        @Public
        @Unstable
        // TODO: 17/3/25 by zmyer
        public abstract void onContainersUpdated(List<UpdatedContainer> containers);

        /**
         * Called when the ResourceManager wants the ApplicationMaster to shutdown
         * for being out of sync etc. The ApplicationMaster should not unregister
         * with the RM unless the ApplicationMaster wants to be the last attempt.
         */
        // TODO: 17/3/25 by zmyer
        public abstract void onShutdownRequest();

        /**
         * Called when nodes tracked by the ResourceManager have changed in health,
         * availability etc.
         */
        // TODO: 17/3/25 by zmyer
        public abstract void onNodesUpdated(List<NodeReport> updatedNodes);

        // TODO: 17/3/25 by zmyer
        public abstract float getProgress();

        /**
         * Called when error comes from RM communications as well as from errors in
         * the callback itself from the app. Calling
         * stop() is the recommended action.
         */
        // TODO: 17/3/25 by zmyer
        public abstract void onError(Throwable e);
    }

    /**
     * @deprecated Use {@link AMRMClientAsync.AbstractCallbackHandler} instead.
     */
    // TODO: 17/3/26 by zmyer
    @Deprecated
    public interface CallbackHandler {

        /**
         * Called when the ResourceManager responds to a heartbeat with completed
         * containers. If the response contains both completed containers and
         * allocated containers, this will be called before containersAllocated.
         */
        // TODO: 17/3/26 by zmyer
        void onContainersCompleted(List<ContainerStatus> statuses);

        /**
         * Called when the ResourceManager responds to a heartbeat with allocated
         * containers. If the response containers both completed containers and
         * allocated containers, this will be called after containersCompleted.
         */
        // TODO: 17/3/26 by zmyer
        void onContainersAllocated(List<Container> containers);

        /**
         * Called when the ResourceManager wants the ApplicationMaster to shutdown
         * for being out of sync etc. The ApplicationMaster should not unregister
         * with the RM unless the ApplicationMaster wants to be the last attempt.
         */
        // TODO: 17/3/26 by zmyer
        void onShutdownRequest();

        /**
         * Called when nodes tracked by the ResourceManager have changed in health,
         * availability etc.
         */
        // TODO: 17/3/26 by zmyer
        void onNodesUpdated(List<NodeReport> updatedNodes);

        // TODO: 17/3/26 by zmyer
        float getProgress();

        /**
         * Called when error comes from RM communications as well as from errors in
         * the callback itself from the app. Calling
         * stop() is the recommended action.
         *
         * @param e
         */
        // TODO: 17/3/26 by zmyer
        void onError(Throwable e);
    }
}
