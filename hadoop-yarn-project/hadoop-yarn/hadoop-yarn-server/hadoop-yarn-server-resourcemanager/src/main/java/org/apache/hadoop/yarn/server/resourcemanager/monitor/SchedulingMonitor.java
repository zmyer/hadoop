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
package org.apache.hadoop.yarn.server.resourcemanager.monitor;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;

// TODO: 17/3/23 by zmyer
public class SchedulingMonitor extends AbstractService {
    //调度策略
    private final SchedulingEditPolicy scheduleEditPolicy;
    private static final Log LOG = LogFactory.getLog(SchedulingMonitor.class);

    //thread which runs periodically to see the last time since a heartbeat is
    //received.
    //检查线程
    private Thread checkerThread;
    //是否暂停
    private volatile boolean stopped;
    //监视时间间隔
    private long monitorInterval;
    //rm执行上下文对象
    private RMContext rmContext;

    // TODO: 17/4/2 by zmyer
    public SchedulingMonitor(RMContext rmContext, SchedulingEditPolicy scheduleEditPolicy) {
        super("SchedulingMonitor (" + scheduleEditPolicy.getPolicyName() + ")");
        this.scheduleEditPolicy = scheduleEditPolicy;
        this.rmContext = rmContext;
    }

    // TODO: 17/4/2 by zmyer
    @VisibleForTesting
    public synchronized SchedulingEditPolicy getSchedulingEditPolicy() {
        return scheduleEditPolicy;
    }

    // TODO: 17/4/2 by zmyer
    public void serviceInit(Configuration conf) throws Exception {
        //初始化调度策略对象
        scheduleEditPolicy.init(conf, rmContext, (PreemptableResourceScheduler) rmContext.getScheduler());
        this.monitorInterval = scheduleEditPolicy.getMonitoringInterval();
        //初始化父类服务
        super.serviceInit(conf);
    }

    // TODO: 17/4/2 by zmyer
    @Override
    public void serviceStart() throws Exception {
        assert !stopped : "starting when already stopped";
        //创建检查线程对象
        checkerThread = new Thread(new PreemptionChecker());
        checkerThread.setName(getName());
        //启动检查线程
        checkerThread.start();
        //启动父类
        super.serviceStart();
    }

    // TODO: 17/4/2 by zmyer
    @Override
    public void serviceStop() throws Exception {
        stopped = true;
        if (checkerThread != null) {
            //关闭检查线程
            checkerThread.interrupt();
        }
        super.serviceStop();
    }

    // TODO: 17/4/2 by zmyer
    @VisibleForTesting
    public void invokePolicy() {
        scheduleEditPolicy.editSchedule();
    }

    // TODO: 17/4/2 by zmyer
    private class PreemptionChecker implements Runnable {
        @Override
        public void run() {
            //线程未退出
            while (!stopped && !Thread.currentThread().isInterrupted()) {
                try {
                    //invoke the preemption policy at a regular pace
                    //the policy will generate preemption or kill events
                    //managed by the dispatcher
                    //开始执行策略
                    invokePolicy();
                } catch (YarnRuntimeException e) {
                    LOG.error("YarnRuntimeException raised while executing preemption"
                        + " checker, skip this run..., exception=", e);
                }

                // Wait before next run
                try {
                    //等待片刻
                    Thread.sleep(monitorInterval);
                } catch (InterruptedException e) {
                    LOG.info(getName() + " thread interrupted");
                    break;
                }
            }
        }
    }
}
