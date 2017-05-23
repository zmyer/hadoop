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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.AbstractEvent;

// TODO: 17/3/22 by zmyer
public class RMAppManagerEvent extends AbstractEvent<RMAppManagerEventType> {
    //应用id
    private final ApplicationId appId;
    private final String targetQueueForMove;

    // TODO: 17/4/3 by zmyer
    public RMAppManagerEvent(ApplicationId appId, RMAppManagerEventType type) {
        this(appId, "", type);
    }

    // TODO: 17/4/3 by zmyer
    public RMAppManagerEvent(ApplicationId appId, String targetQueueForMove, RMAppManagerEventType type) {
        super(type);
        this.appId = appId;
        this.targetQueueForMove = targetQueueForMove;
    }

    // TODO: 17/4/3 by zmyer
    public ApplicationId getApplicationId() {
        return this.appId;
    }

    // TODO: 17/4/3 by zmyer
    public String getTargetQueueForMove() {
        return this.targetQueueForMove;
    }
}
