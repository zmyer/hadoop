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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;

// TODO: 17/3/22 by zmyer
@SuppressWarnings({"unchecked", "rawtypes"})
public class ContainerAllocationExpirer extends AbstractLivelinessMonitor<AllocationExpirationInfo> {
    //事件处理对象
    private EventHandler dispatcher;
    // TODO: 17/4/3 by zmyer
    public ContainerAllocationExpirer(Dispatcher d) {
        super(ContainerAllocationExpirer.class.getName());
        this.dispatcher = d.getEventHandler();
    }

    // TODO: 17/4/3 by zmyer
    public void serviceInit(Configuration conf) throws Exception {
        //读取超时时间间隔
        int expireIntvl = conf.getInt(YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS);
        //设置超时时间间隔
        setExpireInterval(expireIntvl);
        //设置监视时间间隔
        setMonitorInterval(expireIntvl / 3);
        super.serviceInit(conf);
    }

    // TODO: 17/4/3 by zmyer
    @Override
    protected void expire(AllocationExpirationInfo allocationExpirationInfo) {
        dispatcher.handle(new ContainerExpiredSchedulerEvent(allocationExpirationInfo.getContainerId(),
            allocationExpirationInfo.isIncrease()));
    }
}
