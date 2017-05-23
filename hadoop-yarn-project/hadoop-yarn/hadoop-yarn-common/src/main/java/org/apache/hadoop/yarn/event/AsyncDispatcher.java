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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 * Dispatches {@link Event}s in a separate thread. Currently only single thread
 * does that. Potentially there could be multiple channels for each event type
 * class and a thread pool can be used to dispatch the events.
 */
// TODO: 17/3/22 by zmyer
@SuppressWarnings("rawtypes")
@Public
@Evolving
public class AsyncDispatcher extends AbstractService implements Dispatcher {
    private static final Log LOG = LogFactory.getLog(AsyncDispatcher.class);
    //事件阻塞队列
    private final BlockingQueue<Event> eventQueue;
    //事件阻塞队列长度
    private volatile int lastEventQueueSizeLogged = 0;
    //停止标记
    private volatile boolean stopped = false;

    // Configuration flag for enabling/disabling draining dispatcher's events on
    // stop functionality.
    private volatile boolean drainEventsOnStop = false;

    // Indicates all the remaining dispatcher's events on stop have been drained
    // and processed.
    // Race condition happens if dispatcher thread sets drained to true between
    // handler setting drained to false and enqueueing event. YARN-3878 decided
    // to ignore it because of its tiny impact. Also see YARN-5436.
    private volatile boolean drained = true;
    private final Object waitForDrained = new Object();

    // For drainEventsOnStop enabled only, block newly coming events into the
    // queue while stopping.
    private volatile boolean blockNewEvents = false;
    //处理事件句柄对象
    private final EventHandler<Event> handlerInstance = new GenericEventHandler();
    //事件处理线程
    private Thread eventHandlingThread;
    //事件处理句柄映射表
    protected final Map<Class<? extends Enum>, EventHandler> eventDispatchers;
    //事件分发异常对象
    private boolean exitOnDispatchException;

    // TODO: 17/3/22 by zmyer
    public AsyncDispatcher() {
        this(new LinkedBlockingQueue<>());
    }

    // TODO: 17/3/24 by zmyer
    public AsyncDispatcher(BlockingQueue<Event> eventQueue) {
        super("Dispatcher");
        this.eventQueue = eventQueue;
        this.eventDispatchers = new HashMap<>();
    }

    // TODO: 17/3/24 by zmyer
    Runnable createThread() {
        return new Runnable() {
            @Override
            public void run() {
                while (!stopped && !Thread.currentThread().isInterrupted()) {
                    //当前事件队列是否为空
                    drained = eventQueue.isEmpty();
                    // blockNewEvents is only set when dispatcher is draining to stop,
                    // adding this check is to avoid the overhead of acquiring the lock
                    // and calling notify every time in the normal run of the loop.
                    if (blockNewEvents) {
                        synchronized (waitForDrained) {
                            if (drained) {
                                //等待读取事件
                                waitForDrained.notify();
                            }
                        }
                    }
                    Event event;
                    try {
                        //从事件列表中读取事件对象
                        event = eventQueue.take();
                    } catch (InterruptedException ie) {
                        if (!stopped) {
                            LOG.warn("AsyncDispatcher thread interrupted", ie);
                        }
                        return;
                    }
                    if (event != null) {
                        //开始分发事件对象
                        dispatch(event);
                    }
                }
            }
        };
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        this.exitOnDispatchException = conf.getBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY,
            Dispatcher.DEFAULT_DISPATCHER_EXIT_ON_ERROR);
        //初始化服务
        super.serviceInit(conf);
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected void serviceStart() throws Exception {
        //start all the components
        //启动父类服务
        super.serviceStart();
        //创建事件处理线程
        eventHandlingThread = new Thread(createThread());
        //设置线程名称
        eventHandlingThread.setName("AsyncDispatcher event handler");
        //启动事件处理线程
        eventHandlingThread.start();
    }

    // TODO: 17/3/24 by zmyer
    public void setDrainEventsOnStop() {
        drainEventsOnStop = true;
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected void serviceStop() throws Exception {
        if (drainEventsOnStop) {
            blockNewEvents = true;
            LOG.info("AsyncDispatcher is draining to stop, ignoring any new events.");
            long endTime = System.currentTimeMillis() + getConfig()
                .getLong(YarnConfiguration.DISPATCHER_DRAIN_EVENTS_TIMEOUT,
                    YarnConfiguration.DEFAULT_DISPATCHER_DRAIN_EVENTS_TIMEOUT);

            synchronized (waitForDrained) {
                while (!isDrained() && eventHandlingThread != null && eventHandlingThread.isAlive()
                    && System.currentTimeMillis() < endTime) {
                    waitForDrained.wait(100);
                    LOG.info("Waiting for AsyncDispatcher to drain. Thread state is :" +
                        eventHandlingThread.getState());
                }
            }
        }
        stopped = true;
        if (eventHandlingThread != null) {
            //关闭事件处理线程
            eventHandlingThread.interrupt();
            try {
                //等待线程结束
                eventHandlingThread.join();
            } catch (InterruptedException ie) {
                LOG.warn("Interrupted Exception while stopping", ie);
            }
        }

        // stop all the components
        //父类服务停止
        super.serviceStop();
    }

    // TODO: 17/3/24 by zmyer
    @SuppressWarnings("unchecked")
    protected void dispatch(Event event) {
        //all events go thru this loop
        if (LOG.isDebugEnabled()) {
            LOG.debug("Dispatching the event " + event.getClass().getName() + "." + event.toString());
        }

        //读取事件类型
        Class<? extends Enum> type = event.getType().getDeclaringClass();
        try {
            //从事件处理器映射表中读取指定的事件处理器对象
            EventHandler handler = eventDispatchers.get(type);
            if (handler != null) {
                //开始处理事件
                handler.handle(event);
            } else {
                throw new Exception("No handler for registered for " + type);
            }
        } catch (Throwable t) {
            //TODO Maybe log the state of the queue
            LOG.fatal("Error in dispatcher thread", t);
            // If serviceStop is called, we should exit this thread gracefully.
            if (exitOnDispatchException
                && !(ShutdownHookManager.get().isShutdownInProgress())
                && !stopped) {
                //设置停止标记
                stopped = true;
                //创建关闭线程
                Thread shutDownThread = new Thread(createShutDownThread());
                //设置线程名称
                shutDownThread.setName("AsyncDispatcher ShutDown handler");
                //启动关闭线程
                shutDownThread.start();
            }
        }
    }

    // TODO: 17/3/24 by zmyer
    @SuppressWarnings("unchecked")
    @Override
    public void register(Class<? extends Enum> eventType, EventHandler handler) {
    /* check to see if we have a listener registered */
        //根据事件类型,读取对应的处理对象
        EventHandler<Event> registeredHandler = (EventHandler<Event>) eventDispatchers.get(eventType);
        LOG.info("Registering " + eventType + " for " + handler.getClass());
        if (registeredHandler == null) {
            //直接将该事件处理器注册到映射表中
            eventDispatchers.put(eventType, handler);
        } else if (!(registeredHandler instanceof MultiListenerHandler)) {
      /* for multiple listeners of an event add the multiple listener handler */
            //创建多路监听处理器
            MultiListenerHandler multiHandler = new MultiListenerHandler();
            //将该注册的处理器注册到多路监听处理器中
            multiHandler.addHandler(registeredHandler);
            //将该新的处理器注册到多路监听处理器中
            multiHandler.addHandler(handler);
            //将该多路监听器注册到事件分发器对象中
            eventDispatchers.put(eventType, multiHandler);
        } else {
      /* already a multilistener, just add to it */
            //如果之前的处理器是多路的,则直接将该事件处理器注册到多路监听器中
            MultiListenerHandler multiHandler = (MultiListenerHandler) registeredHandler;
            //将当前的事件处理器插入到列表中
            multiHandler.addHandler(handler);
        }
    }

    // TODO: 17/3/24 by zmyer
    @Override
    public EventHandler<Event> getEventHandler() {
        return handlerInstance;
    }

    // TODO: 17/3/24 by zmyer
    class GenericEventHandler implements EventHandler<Event> {
        // TODO: 17/3/24 by zmyer
        public void handle(Event event) {
            if (blockNewEvents) {
                return;
            }
            drained = false;

      /* all this method does is enqueue all the events onto the queue */
            //读取队列长度
            int qSize = eventQueue.size();
            if (qSize != 0 && qSize % 1000 == 0 && lastEventQueueSizeLogged != qSize) {
                lastEventQueueSizeLogged = qSize;
                LOG.info("Size of event-queue is " + qSize);
            }
            //读取事件队列中保留的容量大小
            int remCapacity = eventQueue.remainingCapacity();
            if (remCapacity < 1000) {
                LOG.warn("Very low remaining capacity in the event-queue: " + remCapacity);
            }
            try {
                //将事件对象插入到事件队列中
                eventQueue.put(event);
            } catch (InterruptedException e) {
                if (!stopped) {
                    LOG.warn("AsyncDispatcher thread interrupted", e);
                }
                // Need to reset drained flag to true if event queue is empty,
                // otherwise dispatcher will hang on stop.
                drained = eventQueue.isEmpty();
                throw new YarnRuntimeException(e);
            }
        }
    }

    /**
     * Multiplexing an event. Sending it to different handlers that
     * are interested in the event.
     *
     * @param <T> the type of event these multiple handlers are interested in.
     */
    // TODO: 17/3/24 by zmyer
    static class MultiListenerHandler implements EventHandler<Event> {
        //事件处理器列表
        List<EventHandler<Event>> listofHandlers;

        // TODO: 17/3/24 by zmyer
        public MultiListenerHandler() {
            listofHandlers = new ArrayList<>();
        }

        // TODO: 17/3/24 by zmyer
        @Override
        public void handle(Event event) {
            for (EventHandler<Event> handler : listofHandlers) {
                //开始处理事件对象
                handler.handle(event);
            }
        }

        // TODO: 17/3/24 by zmyer
        void addHandler(EventHandler<Event> handler) {
            listofHandlers.add(handler);
        }

    }

    // TODO: 17/3/24 by zmyer
    Runnable createShutDownThread() {
        return new Runnable() {
            @Override
            public void run() {
                LOG.info("Exiting, bbye..");
                //System.exit(-1);
            }
        };
    }

    // TODO: 17/3/24 by zmyer
    @VisibleForTesting
    protected boolean isEventThreadWaiting() {
        return eventHandlingThread.getState() == Thread.State.WAITING;
    }

    // TODO: 17/3/24 by zmyer
    protected boolean isDrained() {
        return drained;
    }

    // TODO: 17/3/24 by zmyer
    protected boolean isStopped() {
        return stopped;
    }
}
