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

package org.apache.hadoop.yarn.server.resourcemanager.ahs;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationStartData;

// TODO: 17/4/3 by zmyer
public class WritingApplicationStartEvent extends WritingApplicationHistoryEvent {
    //应用id
    private ApplicationId appId;
    //应用启动数据
    private ApplicationStartData appStart;

    // TODO: 17/4/3 by zmyer
    public WritingApplicationStartEvent(ApplicationId appId,
        ApplicationStartData appStart) {
        super(WritingHistoryEventType.APP_START);
        this.appId = appId;
        this.appStart = appStart;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public int hashCode() {
        return appId.hashCode();
    }

    // TODO: 17/4/3 by zmyer
    public ApplicationId getApplicationId() {
        return appId;
    }

    // TODO: 17/4/3 by zmyer
    public ApplicationStartData getApplicationStartData() {
        return appStart;
    }

}
