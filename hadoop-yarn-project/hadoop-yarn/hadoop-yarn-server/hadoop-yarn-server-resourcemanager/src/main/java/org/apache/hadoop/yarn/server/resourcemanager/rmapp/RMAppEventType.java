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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

// TODO: 17/3/22 by zmyer
public enum RMAppEventType {
    // Source: ClientRMService
    //启动app
    START,
    //恢复app
    RECOVER,
    //杀死app
    KILL,
    // Source: Scheduler and RMAppManager
    //app拒绝执行
    APP_REJECTED,
    // Source: Scheduler
    //app接受
    APP_ACCEPTED,
    // TODO add source later
    //更新收集器
    COLLECTOR_UPDATE,
    // Source: RMAppAttempt
    //注册执行对象
    ATTEMPT_REGISTERED,
    //注销执行对象
    ATTEMPT_UNREGISTERED,
    //执行对象执行完毕
    ATTEMPT_FINISHED, // Will send the final state
    //执行对象执行失败
    ATTEMPT_FAILED,
    //杀死执行对象
    ATTEMPT_KILLED,
    //更新节点
    NODE_UPDATE,
    // Source: Container and ResourceTracker
    //app开始在节点上执行
    APP_RUNNING_ON_NODE,
    // Source: RMStateStore
    APP_NEW_SAVED,
    APP_UPDATE_SAVED,
}
