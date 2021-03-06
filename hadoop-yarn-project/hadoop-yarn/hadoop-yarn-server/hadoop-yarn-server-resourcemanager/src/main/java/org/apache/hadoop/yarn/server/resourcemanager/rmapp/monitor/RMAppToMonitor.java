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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.monitor;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;

/**
 * This class used for monitor application with applicationId+appTimeoutType.
 */
// TODO: 17/4/3 by zmyer
public class RMAppToMonitor {
    //应用id
    private ApplicationId applicationId;
    //应用超时类型
    private ApplicationTimeoutType appTimeoutType;

    // TODO: 17/4/3 by zmyer
    RMAppToMonitor(ApplicationId appId, ApplicationTimeoutType timeoutType) {
        this.applicationId = appId;
        this.appTimeoutType = timeoutType;
    }

    // TODO: 17/4/3 by zmyer
    public ApplicationId getApplicationId() {
        return applicationId;
    }

    // TODO: 17/4/3 by zmyer
    public ApplicationTimeoutType getAppTimeoutType() {
        return appTimeoutType;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public int hashCode() {
        return applicationId.hashCode() + appTimeoutType.hashCode();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RMAppToMonitor other = (RMAppToMonitor) obj;
        if (!this.applicationId.equals(other.getApplicationId())) {
            return false;
        }
        if (this.appTimeoutType != other.getAppTimeoutType()) {
            return false;
        }
        return true;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(applicationId.toString()).append("_").append(appTimeoutType);
        return sb.toString();
    }
}