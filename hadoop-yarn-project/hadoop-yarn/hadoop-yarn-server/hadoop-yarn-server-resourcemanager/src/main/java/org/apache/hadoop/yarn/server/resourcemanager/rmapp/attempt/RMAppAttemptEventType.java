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

// TODO: 17/4/3 by zmyer
public enum RMAppAttemptEventType {
    // Source: RMApp
    //启动app
    START,
    //杀死app
    KILL,
    //app执行失败
    FAIL,
    // Source: AMLauncher
    //启动app
    LAUNCHED,
    //启动app失败
    LAUNCH_FAILED,
    // Source: AMLivelinessMonitor
    //超时app
    EXPIRE,

    // Source: ApplicationMasterService
    //app已注册
    REGISTERED,
    //app状态变更
    STATUS_UPDATE,
    //注销app
    UNREGISTERED,
    // Source: Containers
    //分配容器
    CONTAINER_ALLOCATED,
    //容器执行完毕
    CONTAINER_FINISHED,
    // Source: RMStateStore
    ATTEMPT_NEW_SAVED,
    ATTEMPT_UPDATE_SAVED,
    // Source: Scheduler
    ATTEMPT_ADDED,

    // Source: RMAttemptImpl.recover
    RECOVER

}
