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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import javax.crypto.SecretKey;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.blacklist.BlacklistManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;

/**
 * Interface to an Application Attempt in the Resource Manager.
 * A {@link RMApp} can have multiple app attempts based on
 * {@link YarnConfiguration#RM_AM_MAX_ATTEMPTS}. For specific
 * implementation take a look at {@link RMAppAttemptImpl}.
 */
// TODO: 17/3/23 by zmyer
public interface RMAppAttempt extends EventHandler<RMAppAttemptEvent> {

    /**
     * Get the application attempt id for this {@link RMAppAttempt}.
     *
     * @return the {@link ApplicationAttemptId} for this RM attempt.
     */
    // TODO: 17/4/3 by zmyer
    ApplicationAttemptId getAppAttemptId();

    /**
     * The state of the {@link RMAppAttempt}.
     *
     * @return the state {@link RMAppAttemptState} of this {@link RMAppAttempt}
     */
    // TODO: 17/4/3 by zmyer
    RMAppAttemptState getAppAttemptState();

    /**
     * The host on which the {@link RMAppAttempt} is running/ran on.
     *
     * @return the host on which the {@link RMAppAttempt} ran/is running on.
     */
    // TODO: 17/4/3 by zmyer
    String getHost();

    /**
     * The rpc port of the {@link RMAppAttempt}.
     *
     * @return the rpc port of the {@link RMAppAttempt} to which the clients can connect to.
     */
    // TODO: 17/4/3 by zmyer
    int getRpcPort();

    /**
     * The url at which the status of the application attempt can be accessed.
     *
     * @return the url at which the status of the attempt can be accessed.
     */
    // TODO: 17/4/3 by zmyer
    String getTrackingUrl();

    /**
     * The original url at which the status of the application attempt can be
     * accessed. This url is not fronted by a proxy. This is only intended to be
     * used by the proxy.
     *
     * @return the url at which the status of the attempt can be accessed and is not fronted by a proxy.
     */
    // TODO: 17/4/3 by zmyer
    String getOriginalTrackingUrl();

    /**
     * The base to be prepended to web URLs that are not relative, and the user
     * has been checked.
     *
     * @return the base URL to be prepended to web URLs that are not relative.
     */
    // TODO: 17/4/3 by zmyer
    String getWebProxyBase();

    /**
     * Diagnostics information for the application attempt.
     *
     * @return diagnostics information for the application attempt.
     */
    // TODO: 17/4/3 by zmyer
    String getDiagnostics();

    /**
     * Progress for the application attempt.
     *
     * @return the progress for this {@link RMAppAttempt}
     */
    // TODO: 17/4/3 by zmyer
    float getProgress();

    /**
     * The final status set by the AM.
     *
     * @return the final status that is set by the AM when unregistering itself. Can return a null if the AM has not
     * unregistered itself.
     */
    // TODO: 17/4/3 by zmyer
    FinalApplicationStatus getFinalApplicationStatus();

    /**
     * Return a list of the last set of finished containers, resetting the
     * finished containers to empty.
     *
     * @return the list of just finished containers, re setting the finished containers.
     */
    // TODO: 17/4/3 by zmyer
    List<ContainerStatus> pullJustFinishedContainers();

    /**
     * Returns a reference to the map of last set of finished containers to the
     * corresponding node. This does not reset the finished containers.
     *
     * @return the list of just finished containers, this does not reset the finished containers.
     */
    // TODO: 17/4/3 by zmyer
    ConcurrentMap<NodeId, List<ContainerStatus>>
    getJustFinishedContainersReference();

    /**
     * Return the list of last set of finished containers. This does not reset
     * the finished containers.
     *
     * @return the list of just finished containers
     */
    // TODO: 17/4/3 by zmyer
    List<ContainerStatus> getJustFinishedContainers();

    /**
     * The map of conatiners per Node that are already sent to the AM.
     *
     * @return map of per node list of finished container status sent to AM
     */
    // TODO: 17/4/3 by zmyer
    ConcurrentMap<NodeId, List<ContainerStatus>>
    getFinishedContainersSentToAMReference();

    /**
     * The container on which the Application Master is running.
     *
     * @return the {@link Container} on which the application master is running.
     */
    // TODO: 17/4/3 by zmyer
    Container getMasterContainer();

    /**
     * The application submission context for this {@link RMAppAttempt}.
     *
     * @return the application submission context for this Application.
     */
    // TODO: 17/4/3 by zmyer
    ApplicationSubmissionContext getSubmissionContext();

    /**
     * The AMRMToken belonging to this app attempt
     *
     * @return The AMRMToken belonging to this app attempt
     */
    // TODO: 17/4/3 by zmyer
    Token<AMRMTokenIdentifier> getAMRMToken();

    /**
     * The master key for client-to-AM tokens for this app attempt. This is only
     * used for RMStateStore. Normal operation must invoke the secret manager to
     * get the key and not use the local key directly.
     *
     * @return The master key for client-to-AM tokens for this app attempt
     */
    // TODO: 17/4/3 by zmyer
    @LimitedPrivate("RMStateStore")
    SecretKey getClientTokenMasterKey();

    /**
     * Create a token for authenticating a client connection to the app attempt
     *
     * @param clientName the name of the client requesting the token
     * @return the token or null if the attempt is not running
     */
    // TODO: 17/4/3 by zmyer
    Token<ClientToAMTokenIdentifier> createClientToken(String clientName);

    /**
     * Get application container and resource usage information.
     *
     * @return an ApplicationResourceUsageReport object.
     */
    // TODO: 17/4/3 by zmyer
    ApplicationResourceUsageReport getApplicationResourceUsageReport();

    /**
     * Get the {@link BlacklistManager} that manages blacklists for AM failures
     *
     * @return the {@link BlacklistManager} that tracks AM failures.
     */
    // TODO: 17/4/3 by zmyer
    BlacklistManager getAMBlacklistManager();

    /**
     * the start time of the application.
     *
     * @return the start time of the application.
     */
    // TODO: 17/4/3 by zmyer
    long getStartTime();

    /**
     * The current state of the {@link RMAppAttempt}.
     *
     * @return the current state {@link RMAppAttemptState} for this application attempt.
     */
    // TODO: 17/4/3 by zmyer
    RMAppAttemptState getState();

    /**
     * Create the external user-facing state of the attempt of ApplicationMaster
     * from the current state of the {@link RMAppAttempt}.
     *
     * @return the external user-facing state of the attempt ApplicationMaster.
     */
    // TODO: 17/4/3 by zmyer
    YarnApplicationAttemptState createApplicationAttemptState();

    /**
     * Create the Application attempt report from the {@link RMAppAttempt}
     *
     * @return {@link ApplicationAttemptReport}
     */
    // TODO: 17/4/3 by zmyer
    ApplicationAttemptReport createApplicationAttemptReport();

    /**
     * Return the flag which indicates whether the attempt failure should be
     * counted to attempt retry count.
     * <p>
     * There failure types should not be counted to attempt retry count:
     * <ul>
     * <li>preempted by the scheduler.</li>
     * <li>
     * hardware failures, such as NM failing, lost NM and NM disk errors.
     * </li>
     * <li>killed by RM because of RM restart or failover.</li>
     * </ul>
     */
    // TODO: 17/4/3 by zmyer
    boolean shouldCountTowardsMaxAttemptRetry();

    /**
     * Get metrics from the {@link RMAppAttempt}
     *
     * @return metrics
     */
    // TODO: 17/4/3 by zmyer
    RMAppAttemptMetrics getRMAppAttemptMetrics();

    /**
     * the finish time of the application attempt.
     *
     * @return the finish time of the application attempt.
     */
    // TODO: 17/4/3 by zmyer
    long getFinishTime();

    /**
     * To capture Launch diagnostics of the app.
     *
     * @param amLaunchDiagnostics
     */
    // TODO: 17/4/3 by zmyer
    void updateAMLaunchDiagnostics(String amLaunchDiagnostics);

    /**
     * @return Set of nodes which are blacklisted by the application
     */
    // TODO: 17/4/3 by zmyer
    Set<String> getBlacklistedNodes();
}
