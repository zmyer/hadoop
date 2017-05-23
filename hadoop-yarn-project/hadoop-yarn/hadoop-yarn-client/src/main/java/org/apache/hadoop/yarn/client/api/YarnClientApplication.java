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

package org.apache.hadoop.yarn.client.api;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;

@InterfaceAudience.Public
@InterfaceStability.Stable

/**
 * Holder for the {@link GetNewApplicationResponse} and {@link
 * ApplicationSubmissionContext} objects created via {@link org.apache.hadoop
 * .yarn.client.api.YarnClient#createApplication()}
 */
// TODO: 17/3/25 by zmyer
public class YarnClientApplication {
    //新的应用应答请求对象
    private final GetNewApplicationResponse newAppResponse;
    //应用提交上下文对象
    private final ApplicationSubmissionContext appSubmissionContext;

    // TODO: 17/3/25 by zmyer
    public YarnClientApplication(GetNewApplicationResponse newAppResponse, ApplicationSubmissionContext appContext) {
        this.newAppResponse = newAppResponse;
        this.appSubmissionContext = appContext;
    }

    // TODO: 17/3/25 by zmyer
    public GetNewApplicationResponse getNewApplicationResponse() {
        return newAppResponse;
    }

    // TODO: 17/3/25 by zmyer
    public ApplicationSubmissionContext getApplicationSubmissionContext() {
        return appSubmissionContext;
    }
}

