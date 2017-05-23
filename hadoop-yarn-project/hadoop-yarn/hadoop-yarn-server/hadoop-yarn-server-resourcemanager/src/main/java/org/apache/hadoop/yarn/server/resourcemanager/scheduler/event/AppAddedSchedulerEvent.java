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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;

// TODO: 17/4/4 by zmyer
public class AppAddedSchedulerEvent extends SchedulerEvent {
    //应用id
    private final ApplicationId applicationId;
    //队列名称
    private final String queue;
    private final String user;
    private final ReservationId reservationID;
    private final boolean isAppRecovering;
    private final Priority appPriority;

    // TODO: 17/4/4 by zmyer
    public AppAddedSchedulerEvent(ApplicationId applicationId, String queue, String user) {
        this(applicationId, queue, user, false, null, Priority.newInstance(0));
    }

    // TODO: 17/4/4 by zmyer
    public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
        String user, ReservationId reservationID, Priority appPriority) {
        this(applicationId, queue, user, false, reservationID, appPriority);
    }

    // TODO: 17/4/4 by zmyer
    public AppAddedSchedulerEvent(String user, ApplicationSubmissionContext submissionContext,
        boolean isAppRecovering, Priority appPriority) {
        this(submissionContext.getApplicationId(), submissionContext.getQueue(),
            user, isAppRecovering, submissionContext.getReservationID(),
            appPriority);
    }

    // TODO: 17/4/4 by zmyer
    public AppAddedSchedulerEvent(ApplicationId applicationId, String queue,
        String user, boolean isAppRecovering, ReservationId reservationID, Priority appPriority) {
        super(SchedulerEventType.APP_ADDED);
        this.applicationId = applicationId;
        this.queue = queue;
        this.user = user;
        this.reservationID = reservationID;
        this.isAppRecovering = isAppRecovering;
        this.appPriority = appPriority;
    }

    // TODO: 17/4/4 by zmyer
    public ApplicationId getApplicationId() {
        return applicationId;
    }

    // TODO: 17/4/4 by zmyer
    public String getQueue() {
        return queue;
    }

    // TODO: 17/4/4 by zmyer
    public String getUser() {
        return user;
    }

    // TODO: 17/4/4 by zmyer
    public boolean getIsAppRecovering() {
        return isAppRecovering;
    }

    // TODO: 17/4/4 by zmyer
    public ReservationId getReservationID() {
        return reservationID;
    }

    // TODO: 17/4/4 by zmyer
    public Priority getApplicatonPriority() {
        return appPriority;
    }
}
