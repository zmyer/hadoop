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

package org.apache.hadoop.yarn.server.applicationhistoryservice.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;

/**
 * The class contains the fields that can be determined when <code>RMApp</code>
 * starts, and that need to be stored persistently.
 */
@Public
@Unstable
// TODO: 17/4/3 by zmyer
public abstract class ApplicationStartData {

    @Public
    @Unstable
    // TODO: 17/4/3 by zmyer
    public static ApplicationStartData newInstance(ApplicationId applicationId,
        String applicationName, String applicationType, String queue,
        String user, long submitTime, long startTime) {
        ApplicationStartData appSD = Records.newRecord(ApplicationStartData.class);
        appSD.setApplicationId(applicationId);
        appSD.setApplicationName(applicationName);
        appSD.setApplicationType(applicationType);
        appSD.setQueue(queue);
        appSD.setUser(user);
        appSD.setSubmitTime(submitTime);
        appSD.setStartTime(startTime);
        return appSD;
    }

    @Public
    @Unstable
    // TODO: 17/4/3 by zmyer
    public abstract ApplicationId getApplicationId();

    @Public
    @Unstable
    // TODO: 17/4/3 by zmyer
    public abstract void setApplicationId(ApplicationId applicationId);

    @Public
    @Unstable
    // TODO: 17/4/3 by zmyer
    public abstract String getApplicationName();

    @Public
    @Unstable
    // TODO: 17/4/3 by zmyer
    public abstract void setApplicationName(String applicationName);

    @Public
    @Unstable
    // TODO: 17/4/3 by zmyer
    public abstract String getApplicationType();

    @Public
    @Unstable
    // TODO: 17/4/3 by zmyer
    public abstract void setApplicationType(String applicationType);

    @Public
    @Unstable
    // TODO: 17/4/3 by zmyer
    public abstract String getUser();

    @Public
    @Unstable
    // TODO: 17/4/3 by zmyer
    public abstract void setUser(String user);

    @Public
    @Unstable
    // TODO: 17/4/3 by zmyer
    public abstract String getQueue();

    @Public
    @Unstable
    // TODO: 17/4/3 by zmyer
    public abstract void setQueue(String queue);

    @Public
    @Unstable
    // TODO: 17/4/3 by zmyer
    public abstract long getSubmitTime();

    @Public
    @Unstable
    // TODO: 17/4/3 by zmyer
    public abstract void setSubmitTime(long submitTime);

    @Public
    @Unstable
    // TODO: 17/4/3 by zmyer
    public abstract long getStartTime();

    @Public
    @Unstable
    // TODO: 17/4/3 by zmyer
    public abstract void setStartTime(long startTime);

}
