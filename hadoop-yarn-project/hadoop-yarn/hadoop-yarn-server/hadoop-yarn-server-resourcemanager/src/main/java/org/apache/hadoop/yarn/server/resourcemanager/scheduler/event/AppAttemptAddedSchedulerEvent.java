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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;

// TODO: 17/4/3 by zmyer
public class AppAttemptAddedSchedulerEvent extends SchedulerEvent {
    //应用执行对象id
    private final ApplicationAttemptId applicationAttemptId;
    //是否是从上一次执行对象中转移的
    private final boolean transferStateFromPreviousAttempt;
    private final boolean isAttemptRecovering;

    // TODO: 17/4/3 by zmyer
    public AppAttemptAddedSchedulerEvent(ApplicationAttemptId applicationAttemptId,
        boolean transferStateFromPreviousAttempt) {
        this(applicationAttemptId, transferStateFromPreviousAttempt, false);
    }

    // TODO: 17/4/3 by zmyer
    public AppAttemptAddedSchedulerEvent(ApplicationAttemptId applicationAttemptId,
        boolean transferStateFromPreviousAttempt,
        boolean isAttemptRecovering) {
        super(SchedulerEventType.APP_ATTEMPT_ADDED);
        this.applicationAttemptId = applicationAttemptId;
        this.transferStateFromPreviousAttempt = transferStateFromPreviousAttempt;
        this.isAttemptRecovering = isAttemptRecovering;
    }

    // TODO: 17/4/3 by zmyer
    public ApplicationAttemptId getApplicationAttemptId() {
        return applicationAttemptId;
    }

    // TODO: 17/4/3 by zmyer
    public boolean getTransferStateFromPreviousAttempt() {
        return transferStateFromPreviousAttempt;
    }

    // TODO: 17/4/3 by zmyer
    public boolean getIsAttemptRecovering() {
        return isAttemptRecovering;
    }
}
