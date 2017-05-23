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

package org.apache.hadoop.yarn.server.resourcemanager.amlauncher;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;

// TODO: 17/3/22 by zmyer
public class ApplicationMasterLauncher extends AbstractService implements EventHandler<AMLauncherEvent> {
    private static final Log LOG = LogFactory.getLog(ApplicationMasterLauncher.class);
    //加载线程池对象
    private ThreadPoolExecutor launcherPool;
    //加载线程对象
    private LauncherThread launcherHandlingThread;
    //任务队列
    private final BlockingQueue<Runnable> masterEvents = new LinkedBlockingQueue<>();
    //rm执行上下文对象
    protected final RMContext context;

    // TODO: 17/3/24 by zmyer
    public ApplicationMasterLauncher(RMContext context) {
        super(ApplicationMasterLauncher.class.getName());
        this.context = context;
        this.launcherHandlingThread = new LauncherThread();
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        //读取线程数目
        int threadCount = conf.getInt(YarnConfiguration.RM_AMLAUNCHER_THREAD_COUNT,
            YarnConfiguration.DEFAULT_RM_AMLAUNCHER_THREAD_COUNT);

        //创建线程池对象
        ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("ApplicationMasterLauncher #%d").build();
        //创建加载线程池对象
        launcherPool = new ThreadPoolExecutor(threadCount, threadCount, 1, TimeUnit.HOURS, new LinkedBlockingQueue<>());
        //设置底层处理线程池对象
        launcherPool.setThreadFactory(tf);

        //创建yarn配置
        Configuration newConf = new YarnConfiguration(conf);
        //设置连接最大重试次数
        newConf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
            conf.getInt(YarnConfiguration.RM_NODEMANAGER_CONNECT_RETRIES,
                YarnConfiguration.DEFAULT_RM_NODEMANAGER_CONNECT_RETRIES));
        setConfig(newConf);
        //父类服务初始化
        super.serviceInit(newConf);
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected void serviceStart() throws Exception {
        //启动加载服务线程
        launcherHandlingThread.start();
        //启动父类服务
        super.serviceStart();
    }

    // TODO: 17/3/24 by zmyer
    protected Runnable createRunnableLauncher(RMAppAttempt application, AMLauncherEventType event) {
        //创建加载AM任务对象
        return new AMLauncher(context, application, event, getConfig());
    }

    // TODO: 17/3/24 by zmyer
    private void launch(RMAppAttempt application) {
        //首先创建加载AM任务对象
        Runnable launcher = createRunnableLauncher(application, AMLauncherEventType.LAUNCH);
        //将该任务对象插入到任务队列中
        masterEvents.add(launcher);
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected void serviceStop() throws Exception {
        //停止加载线程
        launcherHandlingThread.interrupt();
        try {
            //等待加载线程结束
            launcherHandlingThread.join();
        } catch (InterruptedException ie) {
            LOG.info(launcherHandlingThread.getName() + " interrupted during join ", ie);
        }
        //停止加载线程池对象
        launcherPool.shutdown();
    }

    // TODO: 17/3/24 by zmyer
    private class LauncherThread extends Thread {

        // TODO: 17/3/24 by zmyer
        public LauncherThread() {
            super("ApplicationMaster Launcher");
        }

        // TODO: 17/3/24 by zmyer
        @Override
        public void run() {
            while (!this.isInterrupted()) {
                Runnable toLaunch;
                try {
                    //读取任务
                    toLaunch = masterEvents.take();
                    //将该任务放入线程池中处理
                    launcherPool.execute(toLaunch);
                } catch (InterruptedException e) {
                    LOG.warn(this.getClass().getName() + " interrupted. Returning.");
                    return;
                }
            }
        }
    }

    // TODO: 17/3/24 by zmyer
    private void cleanup(RMAppAttempt application) {
        //创建清理加载服务任务
        Runnable launcher = createRunnableLauncher(application, AMLauncherEventType.CLEANUP);
        //将任务插入到任务队列中
        masterEvents.add(launcher);
    }

    // TODO: 17/3/24 by zmyer
    @Override
    public synchronized void handle(AMLauncherEvent appEvent) {
        //读取AM加载事件类型
        AMLauncherEventType event = appEvent.getType();
        //读取本地执行的app对象
        RMAppAttempt application = appEvent.getAppAttempt();
        switch (event) {
            case LAUNCH:
                //开始加载AM任务
                launch(application);
                break;
            case CLEANUP:
                //清理AM
                cleanup(application);
                break;
            default:
                break;
        }
    }
}
