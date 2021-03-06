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

package org.apache.hadoop.yarn.event;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * This is a specialized EventHandler to be used by Services that are expected
 * handle a large number of events efficiently by ensuring that the caller
 * thread is not blocked. Events are immediately stored in a BlockingQueue and
 * a separate dedicated Thread consumes events from the queue and handles
 * appropriately
 *
 * @param <T> Type of Event
 */
// TODO: 17/3/24 by zmyer
public class EventDispatcher<T extends Event> extends AbstractService implements EventHandler<T> {
    //事件处理对象
    private final EventHandler<T> handler;
    //事件队列
    private final BlockingQueue<T> eventQueue = new LinkedBlockingDeque<>();
    //事件处理线程
    private final Thread eventProcessor;
    //是否停止
    private volatile boolean stopped = false;
    //错误出现是否退出
    private boolean shouldExitOnError = false;

    private static final Log LOG = LogFactory.getLog(EventDispatcher.class);

    // TODO: 17/3/24 by zmyer
    private final class EventProcessor implements Runnable {
        @Override
        public void run() {
            //事件
            T event;
            //线程还未退出
            while (!stopped && !Thread.currentThread().isInterrupted()) {
                try {
                    //从事件队列中读取事件
                    event = eventQueue.take();
                } catch (InterruptedException e) {
                    LOG.error("Returning, interrupted : " + e);
                    return; // TODO: Kill RM.
                }

                try {
                    //处理事件
                    handler.handle(event);
                } catch (Throwable t) {
                    // An error occurred, but we are shutting down anyway.
                    // If it was an InterruptedException, the very act of
                    // shutdown could have caused it and is probably harmless.
                    if (stopped) {
                        LOG.warn("Exception during shutdown: ", t);
                        break;
                    }
                    LOG.fatal("Error in handling event type " + event.getType()
                        + " to the Event Dispatcher", t);
                    if (shouldExitOnError && !ShutdownHookManager.get().isShutdownInProgress()) {
                        LOG.info("Exiting, bbye..");
                        //System.exit(-1);
                    }
                }
            }
        }
    }

    // TODO: 17/3/24 by zmyer
    public EventDispatcher(EventHandler<T> handler, String name) {
        super(name);
        //设置事件处理句柄
        this.handler = handler;
        //创建事件处理线程
        this.eventProcessor = new Thread(new EventProcessor());
        //设置线程名
        this.eventProcessor.setName(getName() + ":Event Processor");
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        this.shouldExitOnError = conf.getBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY,
                Dispatcher.DEFAULT_DISPATCHER_EXIT_ON_ERROR);
        //初始化父类服务
        super.serviceInit(conf);
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected void serviceStart() throws Exception {
        //启动事件处理线程
        this.eventProcessor.start();
        //启动父类服务
        super.serviceStart();
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected void serviceStop() throws Exception {
        //停止服务
        this.stopped = true;
        //暂停事件处理线程
        this.eventProcessor.interrupt();
        try {
            //等待线程退出
            this.eventProcessor.join();
        } catch (InterruptedException e) {
            throw new YarnRuntimeException(e);
        }
        //停止父类服务
        super.serviceStop();
    }

    // TODO: 17/3/24 by zmyer
    @Override
    public void handle(T event) {
        try {
            //事件队列长度
            int qSize = eventQueue.size();
            if (qSize != 0 && qSize % 1000 == 0) {
                LOG.info("Size of " + getName() + " event-queue is " + qSize);
            }
            //事件队列剩余空间
            int remCapacity = eventQueue.remainingCapacity();
            if (remCapacity < 1000) {
                LOG.info("Very low remaining capacity on " + getName() + "" + "event queue: " + remCapacity);
            }
            //将事件插入到事件队列中
            this.eventQueue.put(event);
        } catch (InterruptedException e) {
            LOG.info("Interrupted. Trying to exit gracefully.");
        }
    }
}
