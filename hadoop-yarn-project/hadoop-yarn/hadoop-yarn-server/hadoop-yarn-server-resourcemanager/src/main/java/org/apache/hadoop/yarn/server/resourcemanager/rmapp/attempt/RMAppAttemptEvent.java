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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.event.AbstractEvent;

// TODO: 17/3/23 by zmyer
public class RMAppAttemptEvent extends AbstractEvent<RMAppAttemptEventType> {
    //应用执行id
    private final ApplicationAttemptId appAttemptId;
    //诊断消息
    private final String diagnosticMsg;

    // TODO: 17/4/3 by zmyer
    public RMAppAttemptEvent(ApplicationAttemptId appAttemptId, RMAppAttemptEventType type) {
        this(appAttemptId, type, "");
    }

    // TODO: 17/4/3 by zmyer
    public RMAppAttemptEvent(ApplicationAttemptId appAttemptId,
        RMAppAttemptEventType type, String diagnostics) {
        super(type);
        this.appAttemptId = appAttemptId;
        this.diagnosticMsg = diagnostics;
    }

    // TODO: 17/4/3 by zmyer
    public ApplicationAttemptId getApplicationAttemptId() {
        return this.appAttemptId;
    }

    // TODO: 17/4/3 by zmyer
    public String getDiagnosticMsg() {
        return diagnosticMsg;
    }
}
