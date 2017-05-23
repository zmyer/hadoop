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

package org.apache.hadoop.yarn.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.service.AbstractService;

/**
 * A simple liveliness monitor with which clients can register, trust the
 * component to monitor liveliness, get a call-back on expiry and then finally
 * unregister.
 */
// TODO: 17/3/23 by zmyer
@Public
@Evolving
public abstract class AbstractLivelinessMonitor<O> extends AbstractService {
    private static final Log LOG = LogFactory.getLog(AbstractLivelinessMonitor.class);
    //thread which runs periodically to see the last time since a heartbeat is
    //received.
    //检查线程对象
    private Thread checkerThread;
    //是否暂停标记
    private volatile boolean stopped;
    //默认超时时间
    public static final int DEFAULT_EXPIRE = 5 * 60 * 1000;//5 mins
    //超时时间间隔
    private long expireInterval = DEFAULT_EXPIRE;
    //监视间隔
    private long monitorInterval = expireInterval / 3;
    private volatile boolean resetTimerOnStart = true;
    //时钟对象
    private final Clock clock;
    //监视器列表
    private Map<O, Long> running = new HashMap<O, Long>();

    // TODO: 17/4/3 by zmyer
    public AbstractLivelinessMonitor(String name, Clock clock) {
        super(name);
        this.clock = clock;
    }

    // TODO: 17/4/3 by zmyer
    public AbstractLivelinessMonitor(String name) {
        this(name, new MonotonicClock());
    }

    // TODO: 17/4/3 by zmyer
    @Override
    protected void serviceStart() throws Exception {
        assert !stopped : "starting when already stopped";
        //重置定时器
        resetTimer();
        //创建检查线程对象
        checkerThread = new Thread(new PingChecker());
        //设置线程名称
        checkerThread.setName("Ping Checker");
        //启动线程对象
        checkerThread.start();
        //启动父类服务
        super.serviceStart();
    }

    // TODO: 17/4/3 by zmyer
    @Override
    protected void serviceStop() throws Exception {
        //设置暂停标记
        stopped = true;
        if (checkerThread != null) {
            //停止检查线程
            checkerThread.interrupt();
        }
        //停止父类服务
        super.serviceStop();
    }

    // TODO: 17/4/3 by zmyer
    protected abstract void expire(O ob);

    // TODO: 17/4/3 by zmyer
    protected void setExpireInterval(int expireInterval) {
        this.expireInterval = expireInterval;
    }

    // TODO: 17/4/3 by zmyer
    protected long getExpireInterval(O o) {
        // by-default return for all the registered object interval.
        return this.expireInterval;
    }

    // TODO: 17/4/3 by zmyer
    protected void setMonitorInterval(long monitorInterval) {
        this.monitorInterval = monitorInterval;
    }

    // TODO: 17/4/3 by zmyer
    public synchronized void receivedPing(O ob) {
        //only put for the registered objects
        if (running.containsKey(ob)) {
            //如果当前的监视器存在于列表中,则直接更新当前监视器
            running.put(ob, clock.getTime());
        }
    }

    // TODO: 17/4/3 by zmyer
    public synchronized void register(O ob) {
        register(ob, clock.getTime());
    }

    // TODO: 17/4/3 by zmyer
    public synchronized void register(O ob, long expireTime) {
        running.put(ob, expireTime);
    }

    // TODO: 17/4/3 by zmyer
    public synchronized void unregister(O ob) {
        running.remove(ob);
    }

    // TODO: 17/4/3 by zmyer
    public synchronized void resetTimer() {
        if (resetTimerOnStart) {
            long time = clock.getTime();
            for (O ob : running.keySet()) {
                running.put(ob, time);
            }
        }
    }

    // TODO: 17/4/3 by zmyer
    protected void setResetTimeOnStart(boolean resetTimeOnStart) {
        this.resetTimerOnStart = resetTimeOnStart;
    }

    // TODO: 17/3/24 by zmyer
    private class PingChecker implements Runnable {

        // TODO: 17/4/3 by zmyer
        @Override
        public void run() {
            while (!stopped && !Thread.currentThread().isInterrupted()) {
                synchronized (AbstractLivelinessMonitor.this) {
                    //读取监视器列表迭代器
                    Iterator<Map.Entry<O, Long>> iterator = running.entrySet().iterator();

                    // avoid calculating current time everytime in loop
                    long currentTime = clock.getTime();
                    while (iterator.hasNext()) {
                        Map.Entry<O, Long> entry = iterator.next();
                        //读取键值
                        O key = entry.getKey();
                        long interval = getExpireInterval(key);
                        if (currentTime > entry.getValue() + interval) {
                            //如果超时了,则从列表中删除
                            iterator.remove();
                            //超时处理
                            expire(key);
                            LOG.info("Expired:" + entry.getKey().toString()
                                + " Timed out after " + interval / 1000 + " secs");
                        }
                    }
                }
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
