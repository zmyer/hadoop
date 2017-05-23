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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;

// TODO: 17/4/4 by zmyer
@Private
@Unstable
public class SchedulerApplication<T extends SchedulerApplicationAttempt> {
    //app可调度队列
    private Queue queue;
    private final String user;
    //当前的app执行对象
    private volatile T currentAttempt;
    //优先级
    private volatile Priority priority;

    // TODO: 17/4/4 by zmyer
    public SchedulerApplication(Queue queue, String user) {
        this.queue = queue;
        this.user = user;
        this.priority = null;
    }

    // TODO: 17/4/4 by zmyer
    public SchedulerApplication(Queue queue, String user, Priority priority) {
        this.queue = queue;
        this.user = user;
        this.priority = priority;
    }

    // TODO: 17/4/4 by zmyer
    public Queue getQueue() {
        return queue;
    }

    // TODO: 17/4/4 by zmyer
    public void setQueue(Queue queue) {
        this.queue = queue;
    }

    // TODO: 17/4/4 by zmyer
    public String getUser() {
        return user;
    }

    // TODO: 17/4/4 by zmyer
    public T getCurrentAppAttempt() {
        return currentAttempt;
    }

    // TODO: 17/4/4 by zmyer
    public void setCurrentAppAttempt(T currentAttempt) {
        this.currentAttempt = currentAttempt;
    }

    // TODO: 17/4/4 by zmyer
    public void stop(RMAppState rmAppFinalState) {
        queue.getMetrics().finishApp(user, rmAppFinalState);
    }

    // TODO: 17/4/4 by zmyer
    public Priority getPriority() {
        return priority;
    }

    // TODO: 17/4/4 by zmyer
    public void setPriority(Priority priority) {
        this.priority = priority;

        // Also set priority in current running attempt
        if (null != currentAttempt) {
            currentAttempt.setPriority(priority);
        }
    }
}
