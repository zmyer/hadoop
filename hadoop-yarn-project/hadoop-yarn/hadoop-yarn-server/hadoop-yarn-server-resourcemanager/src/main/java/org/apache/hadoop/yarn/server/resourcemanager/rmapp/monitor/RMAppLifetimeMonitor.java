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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.SystemClock;

/**
 * This service will monitor the applications against the lifetime value given.
 * The applications will be killed if it running beyond the given time.
 */
// TODO: 17/3/23 by zmyer
public class RMAppLifetimeMonitor extends AbstractLivelinessMonitor<RMAppToMonitor> {
    private static final Log LOG = LogFactory.getLog(RMAppLifetimeMonitor.class);
    //rm上下文对象
    private RMContext rmContext;

    // TODO: 17/4/3 by zmyer
    public RMAppLifetimeMonitor(RMContext rmContext) {
        super(RMAppLifetimeMonitor.class.getName(), SystemClock.getInstance());
        this.rmContext = rmContext;
    }

    // TODO: 17/4/3 by zmyer
    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        //监视时间间隔
        long monitorInterval = conf.getLong(YarnConfiguration.RM_APPLICATION_MONITOR_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_APPLICATION_MONITOR_INTERVAL_MS);
        if (monitorInterval <= 0) {
            monitorInterval = YarnConfiguration.DEFAULT_RM_APPLICATION_MONITOR_INTERVAL_MS;
        }
        //设置监视时间间隔
        setMonitorInterval(monitorInterval);
        setExpireInterval(0); // No need of expire interval for App.
        setResetTimeOnStart(false); // do not reset expire time on restart
        LOG.info("Application lifelime monitor interval set to " + monitorInterval + " ms.");
        super.serviceInit(conf);
    }

    // TODO: 17/4/3 by zmyer
    @SuppressWarnings("unchecked")
    @Override
    protected synchronized void expire(RMAppToMonitor monitoredAppKey) {
        //读取应用id
        ApplicationId appId = monitoredAppKey.getApplicationId();
        //根据应用id,获取app对象
        RMApp app = rmContext.getRMApps().get(appId);
        if (app == null) {
            return;
        }
        String diagnostics = "Application is killed by ResourceManager as it"
            + " has exceeded the lifetime period.";
        //如果出现了超时情况,则需要及时杀死该app
        rmContext.getDispatcher().getEventHandler().handle(new RMAppEvent(appId, RMAppEventType.KILL, diagnostics));
    }

    // TODO: 17/4/3 by zmyer
    public void registerApp(ApplicationId appId, ApplicationTimeoutType timeoutType, long expireTime) {
        //创建app监视对象
        RMAppToMonitor appToMonitor = new RMAppToMonitor(appId, timeoutType);
        //将该监视对象注册到监视器列表中
        register(appToMonitor, expireTime);
    }

    // TODO: 17/4/3 by zmyer
    public void unregisterApp(ApplicationId appId, ApplicationTimeoutType timeoutType) {
        //创建app监视器对象
        RMAppToMonitor remove = new RMAppToMonitor(appId, timeoutType);
        //注销监视器
        unregister(remove);
    }

    // TODO: 17/4/3 by zmyer
    public void unregisterApp(ApplicationId appId, Set<ApplicationTimeoutType> timeoutTypes) {
        for (ApplicationTimeoutType timeoutType : timeoutTypes) {
            //注册监视器
            unregisterApp(appId, timeoutType);
        }
    }

    // TODO: 17/4/3 by zmyer
    public void updateApplicationTimeouts(ApplicationId appId, Map<ApplicationTimeoutType, Long> timeouts) {
        for (Entry<ApplicationTimeoutType, Long> entry : timeouts.entrySet()) {
            ApplicationTimeoutType timeoutType = entry.getKey();
            RMAppToMonitor update = new RMAppToMonitor(appId, timeoutType);
            register(update, entry.getValue());
        }
    }
}