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

package org.apache.hadoop.yarn.client.api.async.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.impl.NMClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

@Private
@Unstable
// TODO: 17/3/27 by zmyer
public class NMClientAsyncImpl extends NMClientAsync {
    private static final Log LOG = LogFactory.getLog(NMClientAsyncImpl.class);
    //线程池默认大小
    protected static final int INITIAL_THREAD_POOL_SIZE = 10;
    //线程池对象
    protected ThreadPoolExecutor threadPool;
    //线程池最大的线程数目
    protected int maxThreadPoolSize;
    //事件分发线程对象
    protected Thread eventDispatcherThread;
    //是否停止客户端标记
    protected AtomicBoolean stopped = new AtomicBoolean(false);
    //容器事件队列
    protected BlockingQueue<ContainerEvent> events = new LinkedBlockingQueue<ContainerEvent>();
    //容器映射表
    protected ConcurrentMap<ContainerId, StatefulContainer> containers =
        new ConcurrentHashMap<ContainerId, StatefulContainer>();

    // TODO: 17/3/27 by zmyer
    public NMClientAsyncImpl(AbstractCallbackHandler callbackHandler) {
        this(NMClientAsync.class.getName(), callbackHandler);
    }

    // TODO: 17/3/27 by zmyer
    public NMClientAsyncImpl(
        String name, AbstractCallbackHandler callbackHandler) {
        this(name, new NMClientImpl(), callbackHandler);
    }

    @Private
    @VisibleForTesting
    // TODO: 17/3/27 by zmyer
    protected NMClientAsyncImpl(String name, NMClient client,
        AbstractCallbackHandler callbackHandler) {
        super(name, client, callbackHandler);
        this.client = client;
        this.callbackHandler = callbackHandler;
    }

    /**
     * @deprecated Use {@link #NMClientAsyncImpl(NMClientAsync.AbstractCallbackHandler)} instead.
     */
    // TODO: 17/3/27 by zmyer
    @Deprecated
    public NMClientAsyncImpl(CallbackHandler callbackHandler) {
        this(NMClientAsync.class.getName(), callbackHandler);
    }

    /**
     * @deprecated Use {@link #NMClientAsyncImpl(String, NMClientAsync.AbstractCallbackHandler)} instead.
     */
    @Deprecated
    // TODO: 17/3/27 by zmyer
    public NMClientAsyncImpl(String name, CallbackHandler callbackHandler) {
        this(name, new NMClientImpl(), callbackHandler);
    }

    @Private
    @VisibleForTesting
    @Deprecated
    // TODO: 17/3/27 by zmyer
    protected NMClientAsyncImpl(String name, NMClient client,
        CallbackHandler callbackHandler) {
        super(name, client, callbackHandler);
        this.client = client;
        this.callbackHandler = callbackHandler;
    }

    @Override
    // TODO: 17/3/27 by zmyer
    protected void serviceInit(Configuration conf) throws Exception {
        //读取线程池最大线程数量
        this.maxThreadPoolSize = conf.getInt(
            YarnConfiguration.NM_CLIENT_ASYNC_THREAD_POOL_MAX_SIZE,
            YarnConfiguration.DEFAULT_NM_CLIENT_ASYNC_THREAD_POOL_MAX_SIZE);
        LOG.info("Upper bound of the thread pool size is " + maxThreadPoolSize);
        //初始化客户端
        client.init(conf);
        //初始化父类服务
        super.serviceInit(conf);
    }

    @Override
    // TODO: 17/3/27 by zmyer
    protected void serviceStart() throws Exception {
        //启动客户端对象
        client.start();
        //创建线程池对象
        ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat(
            this.getClass().getName() + " #%d").setDaemon(true).build();

        // Start with a default core-pool size and change it dynamically.
        //线程池线程初始化数量
        int initSize = Math.min(INITIAL_THREAD_POOL_SIZE, maxThreadPoolSize);
        //创建线程池对象
        threadPool = new ThreadPoolExecutor(initSize, Integer.MAX_VALUE, 1,
            TimeUnit.HOURS, new LinkedBlockingQueue<>(), tf);

        //创建事件分发线程
        eventDispatcherThread = new Thread() {
            @Override
            public void run() {
                ContainerEvent event;
                Set<String> allNodes = new HashSet<String>();

                while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
                    try {
                        //从队列中读取事件
                        event = events.take();
                    } catch (InterruptedException e) {
                        if (!stopped.get()) {
                            LOG.error("Returning, thread interrupted", e);
                        }
                        return;
                    }
                    //从事件中读取节点id,并将其插入到列表中
                    allNodes.add(event.getNodeId().toString());

                    //读取线程池中的线程数量
                    int threadPoolSize = threadPool.getCorePoolSize();

                    // We can increase the pool size only if haven't reached the maximum
                    // limit yet.
                    if (threadPoolSize != maxThreadPoolSize) {

                        // nodes where containers will run at *this* point of time. This is
                        // *not* the cluster size and doesn't need to be.
                        //读取节点列表的长度
                        int nodeNum = allNodes.size();
                        //计算到目前为止,线程池中还有的空闲线程数量
                        int idealThreadPoolSize = Math.min(maxThreadPoolSize, nodeNum);

                        if (threadPoolSize < idealThreadPoolSize) {
                            // Bump up the pool size to idealThreadPoolSize +
                            // INITIAL_POOL_SIZE, the later is just a buffer so we are not
                            // always increasing the pool-size
                            //计算新的线程池的线程数量
                            int newThreadPoolSize = Math.min(maxThreadPoolSize,
                                idealThreadPoolSize + INITIAL_THREAD_POOL_SIZE);
                            LOG.info("Set NMClientAsync thread pool size to " +
                                newThreadPoolSize + " as the number of nodes to talk to is "
                                + nodeNum);
                            //重新设置线程池的线程数量
                            threadPool.setCorePoolSize(newThreadPoolSize);
                        }
                    }

                    // the events from the queue are handled in parallel with a thread
                    // pool
                    //开始在线程池中处理接收到的事件对象
                    threadPool.execute(getContainerEventProcessor(event));

                    // TODO: Group launching of multiple containers to a single
                    // NodeManager into a single connection
                }
            }
        };
        //设置事件分发线程的名称
        eventDispatcherThread.setName("Container  Event Dispatcher");
        eventDispatcherThread.setDaemon(false);
        //启动事件分发线程对象
        eventDispatcherThread.start();
        //启动父类服务
        super.serviceStart();
    }

    // TODO: 17/3/28 by zmyer
    @Override
    protected void serviceStop() throws Exception {
        //如果已经停止了,则直接退出
        if (stopped.getAndSet(true)) {
            // return if already stopped
            return;
        }
        if (eventDispatcherThread != null) {
            //停止线程
            eventDispatcherThread.interrupt();
            try {
                //等待线程结束
                eventDispatcherThread.join();
            } catch (InterruptedException e) {
                LOG.error("The thread of " + eventDispatcherThread.getName() +
                    " didn't finish normally.", e);
            }
        }
        if (threadPool != null) {
            //关闭线程池对象
            threadPool.shutdownNow();
        }
        if (client != null) {
            // If NMClientImpl doesn't stop running containers, the states doesn't
            // need to be cleared.
            if (!(client instanceof NMClientImpl) ||
                ((NMClientImpl) client).getCleanupRunningContainers().get()) {
                if (containers != null) {
                    //清理容器集合
                    containers.clear();
                }
            }
            //关闭客户端
            client.stop();
        }
        //关闭父类服务
        super.serviceStop();
    }

    // TODO: 17/3/28 by zmyer
    public void startContainerAsync(
        Container container, ContainerLaunchContext containerLaunchContext) {
        //将容器首先插入到容器列表中
        if (containers.putIfAbsent(container.getId(),
            new StatefulContainer(this, container.getId())) != null) {
            //如果插入失败,则直接回调传递过来的启动容器失败接口
            callbackHandler.onStartContainerError(container.getId(),
                RPCUtil.getRemoteException("Container " + container.getId() +
                    " is already started or scheduled to start"));
        }
        try {
            //将启动容器的事件插入到事件队列中,等待后续处理
            events.put(new StartContainerEvent(container, containerLaunchContext));
        } catch (InterruptedException e) {
            LOG.warn("Exception when scheduling the event of starting Container " +
                container.getId());
            //执行容器启动失败的回调函数
            callbackHandler.onStartContainerError(container.getId(), e);
        }
    }

    // TODO: 17/3/28 by zmyer
    public void increaseContainerResourceAsync(Container container) {
        if (!(callbackHandler instanceof AbstractCallbackHandler)) {
            LOG.error("Callback handler does not implement container resource "
                + "increase callback methods");
            return;
        }

        AbstractCallbackHandler handler = (AbstractCallbackHandler) callbackHandler;
        if (containers.get(container.getId()) == null) {
            //如果待扩容资源的容器对象不在容器列表中,这说明该容器无效,直接抛出异常
            handler.onIncreaseContainerResourceError(
                container.getId(),
                RPCUtil.getRemoteException(
                    "Container " + container.getId() +
                        " is neither started nor scheduled to start"));
        }
        try {
            //将扩容资源的事件插入到事件队列中
            events.put(new IncreaseContainerResourceEvent(container));
        } catch (InterruptedException e) {
            LOG.warn("Exception when scheduling the event of increasing resource of "
                + "Container " + container.getId());
            //失败回调
            handler.onIncreaseContainerResourceError(container.getId(), e);
        }
    }

    // TODO: 17/3/28 by zmyer
    public void stopContainerAsync(ContainerId containerId, NodeId nodeId) {
        if (containers.get(containerId) == null) {
            //容器不在列表中,直接失败回调
            callbackHandler.onStopContainerError(containerId,
                RPCUtil.getRemoteException("Container " + containerId +
                    " is neither started nor scheduled to start"));
        }
        try {
            //插入事件
            events.put(new ContainerEvent(containerId, nodeId, null,
                ContainerEventType.STOP_CONTAINER));
        } catch (InterruptedException e) {
            LOG.warn("Exception when scheduling the event of stopping Container " +
                containerId);
            //失败回调
            callbackHandler.onStopContainerError(containerId, e);
        }
    }

    // TODO: 17/3/28 by zmyer
    public void getContainerStatusAsync(ContainerId containerId, NodeId nodeId) {
        try {
            //将获取容器状态的事件插入到事件队列中
            events.put(new ContainerEvent(containerId, nodeId, null,
                ContainerEventType.QUERY_CONTAINER));
        } catch (InterruptedException e) {
            LOG.warn("Exception when scheduling the event of querying the status" +
                " of Container " + containerId);
            //失败回调
            callbackHandler.onGetContainerStatusError(containerId, e);
        }
    }

    // TODO: 17/3/27 by zmyer
    protected enum ContainerState {
        PREP, FAILED, RUNNING, DONE,
    }

    // TODO: 17/3/28 by zmyer
    private boolean isCompletelyDone(StatefulContainer container) {
        return container.getState() == ContainerState.DONE ||
            container.getState() == ContainerState.FAILED;
    }

    // TODO: 17/3/27 by zmyer
    protected ContainerEventProcessor getContainerEventProcessor(ContainerEvent event) {
        return new ContainerEventProcessor(event);
    }

    /**
     * The type of the event of interacting with a container
     */
    // TODO: 17/3/27 by zmyer
    protected enum ContainerEventType {
        START_CONTAINER,
        STOP_CONTAINER,
        QUERY_CONTAINER,
        INCREASE_CONTAINER_RESOURCE
    }

    // TODO: 17/3/27 by zmyer
    protected static class ContainerEvent extends AbstractEvent<ContainerEventType> {
        //容器id
        private ContainerId containerId;
        //节点id
        private NodeId nodeId;
        //容器token
        private Token containerToken;

        // TODO: 17/3/28 by zmyer
        ContainerEvent(ContainerId containerId, NodeId nodeId,
            Token containerToken, ContainerEventType type) {
            super(type);
            this.containerId = containerId;
            this.nodeId = nodeId;
            this.containerToken = containerToken;
        }

        // TODO: 17/3/28 by zmyer
        public ContainerId getContainerId() {
            return containerId;
        }

        // TODO: 17/3/28 by zmyer
        public NodeId getNodeId() {
            return nodeId;
        }

        // TODO: 17/3/28 by zmyer
        public Token getContainerToken() {
            return containerToken;
        }
    }

    // TODO: 17/3/28 by zmyer
    protected static class StartContainerEvent extends ContainerEvent {
        //容器对象
        private Container container;
        //容器加载上下文对象
        private ContainerLaunchContext containerLaunchContext;

        // TODO: 17/3/28 by zmyer
        public StartContainerEvent(Container container,
            ContainerLaunchContext containerLaunchContext) {
            super(container.getId(), container.getNodeId(),
                container.getContainerToken(), ContainerEventType.START_CONTAINER);
            this.container = container;
            this.containerLaunchContext = containerLaunchContext;
        }

        // TODO: 17/3/28 by zmyer
        public Container getContainer() {
            return container;
        }

        // TODO: 17/3/28 by zmyer
        ContainerLaunchContext getContainerLaunchContext() {
            return containerLaunchContext;
        }
    }

    // TODO: 17/3/28 by zmyer
    private static class IncreaseContainerResourceEvent extends ContainerEvent {
        //容器对象
        private Container container;

        // TODO: 17/3/28 by zmyer
        IncreaseContainerResourceEvent(Container container) {
            super(container.getId(), container.getNodeId(),
                container.getContainerToken(),
                ContainerEventType.INCREASE_CONTAINER_RESOURCE);
            this.container = container;
        }

        // TODO: 17/3/28 by zmyer
        public Container getContainer() {
            return container;
        }
    }

    // TODO: 17/3/27 by zmyer
    protected static class StatefulContainer implements EventHandler<ContainerEvent> {

        // TODO: 17/3/28 by zmyer
        final static StateMachineFactory<StatefulContainer,
            ContainerState, ContainerEventType, ContainerEvent> stateMachineFactory
            = new StateMachineFactory<StatefulContainer, ContainerState,
            ContainerEventType, ContainerEvent>(ContainerState.PREP)

            // Transitions from PREP state
            .addTransition(ContainerState.PREP,
                EnumSet.of(ContainerState.RUNNING, ContainerState.FAILED),
                ContainerEventType.START_CONTAINER,
                new StartContainerTransition())
            .addTransition(ContainerState.PREP, ContainerState.DONE,
                ContainerEventType.STOP_CONTAINER, new OutOfOrderTransition())

            // Transitions from RUNNING state
            .addTransition(ContainerState.RUNNING, ContainerState.RUNNING,
                ContainerEventType.INCREASE_CONTAINER_RESOURCE,
                new IncreaseContainerResourceTransition())
            .addTransition(ContainerState.RUNNING,
                EnumSet.of(ContainerState.DONE, ContainerState.FAILED),
                ContainerEventType.STOP_CONTAINER,
                new StopContainerTransition())

            // Transition from DONE state
            .addTransition(ContainerState.DONE, ContainerState.DONE,
                EnumSet.of(ContainerEventType.START_CONTAINER,
                    ContainerEventType.STOP_CONTAINER,
                    ContainerEventType.INCREASE_CONTAINER_RESOURCE))

            // Transition from FAILED state
            .addTransition(ContainerState.FAILED, ContainerState.FAILED,
                EnumSet.of(ContainerEventType.START_CONTAINER,
                    ContainerEventType.STOP_CONTAINER,
                    ContainerEventType.INCREASE_CONTAINER_RESOURCE));

        // TODO: 17/3/27 by zmyer
        protected static class StartContainerTransition implements
            MultipleArcTransition<StatefulContainer, ContainerEvent, ContainerState> {

            // TODO: 17/3/27 by zmyer
            @Override
            public ContainerState transition(
                StatefulContainer container, ContainerEvent event) {
                ContainerId containerId = event.getContainerId();
                try {
                    //启动容器事件
                    StartContainerEvent scEvent = null;
                    if (event instanceof StartContainerEvent) {
                        scEvent = (StartContainerEvent) event;
                    }
                    assert scEvent != null;
                    //开始启动容器
                    Map<String, ByteBuffer> allServiceResponse =
                        container.nmClientAsync.getClient().startContainer(
                            scEvent.getContainer(), scEvent.getContainerLaunchContext());
                    try {
                        //启动容器成功回调
                        container.nmClientAsync.getCallbackHandler().onContainerStarted(
                            containerId, allServiceResponse);
                    } catch (Throwable thr) {
                        // Don't process user created unchecked exception
                        LOG.info("Unchecked exception is thrown from onContainerStarted for "
                            + "Container " + containerId, thr);
                    }
                    //返回容器启动成功
                    return ContainerState.RUNNING;
                } catch (YarnException e) {
                    return onExceptionRaised(container, event, e);
                } catch (IOException e) {
                    return onExceptionRaised(container, event, e);
                } catch (Throwable t) {
                    return onExceptionRaised(container, event, t);
                }
            }

            // TODO: 17/3/28 by zmyer
            private ContainerState onExceptionRaised(StatefulContainer container,
                ContainerEvent event, Throwable t) {
                try {
                    //启动失败回调
                    container.nmClientAsync.getCallbackHandler().onStartContainerError(
                        event.getContainerId(), t);
                } catch (Throwable thr) {
                    // Don't process user created unchecked exception
                    LOG.info(
                        "Unchecked exception is thrown from onStartContainerError for " +
                            "Container " + event.getContainerId(), thr);
                }
                //返回启动失败状态
                return ContainerState.FAILED;
            }
        }

        // TODO: 17/3/28 by zmyer
        static class IncreaseContainerResourceTransition implements
            SingleArcTransition<StatefulContainer, ContainerEvent> {
            // TODO: 17/3/28 by zmyer
            @Override
            public void transition(
                StatefulContainer container, ContainerEvent event) {
                if (!(container.nmClientAsync.getCallbackHandler()
                    instanceof AbstractCallbackHandler)) {
                    LOG.error("Callback handler does not implement container resource "
                        + "increase callback methods");
                    return;
                }
                AbstractCallbackHandler handler =
                    (AbstractCallbackHandler) container.nmClientAsync.getCallbackHandler();
                try {
                    if (!(event instanceof IncreaseContainerResourceEvent)) {
                        throw new AssertionError("Unexpected event type. Expecting:"
                            + "IncreaseContainerResourceEvent. Got:" + event);
                    }
                    //创建扩容容器资源事件
                    IncreaseContainerResourceEvent increaseEvent =
                        (IncreaseContainerResourceEvent) event;
                    //请求扩容容器资源
                    container.nmClientAsync.getClient().increaseContainerResource(
                        increaseEvent.getContainer());
                    try {
                        //资源扩容成功回调
                        handler.onContainerResourceIncreased(
                            increaseEvent.getContainerId(), increaseEvent.getContainer()
                                .getResource());
                    } catch (Throwable thr) {
                        // Don't process user created unchecked exception
                        LOG.info("Unchecked exception is thrown from "
                            + "onContainerResourceIncreased for Container "
                            + event.getContainerId(), thr);
                    }
                } catch (Exception e) {
                    try {
                        //资源扩容失败回调
                        handler.onIncreaseContainerResourceError(event.getContainerId(), e);
                    } catch (Throwable thr) {
                        // Don't process user created unchecked exception
                        LOG.info("Unchecked exception is thrown from "
                            + "onIncreaseContainerResourceError for Container "
                            + event.getContainerId(), thr);
                    }
                }
            }
        }

        // TODO: 17/3/28 by zmyer
        protected static class StopContainerTransition implements
            MultipleArcTransition<StatefulContainer, ContainerEvent, ContainerState> {

            // TODO: 17/3/28 by zmyer
            @Override
            public ContainerState transition(
                StatefulContainer container, ContainerEvent event) {
                ContainerId containerId = event.getContainerId();
                try {
                    container.nmClientAsync.getClient().stopContainer(
                        containerId, event.getNodeId());
                    try {
                        //停止指定容器运行
                        container.nmClientAsync.getCallbackHandler().onContainerStopped(
                            event.getContainerId());
                    } catch (Throwable thr) {
                        // Don't process user created unchecked exception
                        LOG.info("Unchecked exception is thrown from onContainerStopped for "
                            + "Container " + event.getContainerId(), thr);
                    }
                    //返回操作完成
                    return ContainerState.DONE;
                } catch (YarnException e) {
                    return onExceptionRaised(container, event, e);
                } catch (IOException e) {
                    return onExceptionRaised(container, event, e);
                } catch (Throwable t) {
                    return onExceptionRaised(container, event, t);
                }
            }

            // TODO: 17/3/28 by zmyer
            private ContainerState onExceptionRaised(StatefulContainer container,
                ContainerEvent event, Throwable t) {
                try {
                    //停止容器失败回调
                    container.nmClientAsync.getCallbackHandler().onStopContainerError(
                        event.getContainerId(), t);
                } catch (Throwable thr) {
                    // Don't process user created unchecked exception
                    LOG.info("Unchecked exception is thrown from onStopContainerError for "
                        + "Container " + event.getContainerId(), thr);
                }
                //返回停止失败状态
                return ContainerState.FAILED;
            }
        }

        // TODO: 17/3/28 by zmyer
        protected static class OutOfOrderTransition implements
            SingleArcTransition<StatefulContainer, ContainerEvent> {

            static final String STOP_BEFORE_START_ERROR_MSG =
                "Container was killed before it was launched";

            // TODO: 17/3/28 by zmyer
            @Override
            public void transition(StatefulContainer container, ContainerEvent event) {
                try {
                    //失败回调
                    container.nmClientAsync.getCallbackHandler().onStartContainerError(
                        event.getContainerId(),
                        RPCUtil.getRemoteException(STOP_BEFORE_START_ERROR_MSG));
                } catch (Throwable thr) {
                    // Don't process user created unchecked exception
                    LOG.info(
                        "Unchecked exception is thrown from onStartContainerError for " +
                            "Container " + event.getContainerId(), thr);
                }
            }
        }

        //nm客户端异步对象
        private final NMClientAsync nmClientAsync;
        //容器id
        private final ContainerId containerId;
        //状态机对象
        private final StateMachine<ContainerState, ContainerEventType,
            ContainerEvent> stateMachine;
        //读取锁对象
        private final ReadLock readLock;
        //写入锁对象
        private final WriteLock writeLock;

        // TODO: 17/3/28 by zmyer
        public StatefulContainer(NMClientAsync client, ContainerId containerId) {
            this.nmClientAsync = client;
            this.containerId = containerId;
            stateMachine = stateMachineFactory.make(this);
            ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
            readLock = lock.readLock();
            writeLock = lock.writeLock();
        }

        // TODO: 17/3/28 by zmyer
        @Override
        public void handle(ContainerEvent event) {
            writeLock.lock();
            try {
                try {
                    //开始进行状态机状态转移
                    this.stateMachine.doTransition(event.getType(), event);
                } catch (InvalidStateTransitionException e) {
                    LOG.error("Can't handle this event at current state", e);
                }
            } finally {
                writeLock.unlock();
            }
        }

        // TODO: 17/3/28 by zmyer
        public ContainerId getContainerId() {
            return containerId;
        }

        // TODO: 17/3/28 by zmyer
        public ContainerState getState() {
            readLock.lock();
            try {
                //返回状态机的当前状态信息
                return stateMachine.getCurrentState();
            } finally {
                readLock.unlock();
            }
        }
    }

    // TODO: 17/3/27 by zmyer
    protected class ContainerEventProcessor implements Runnable {
        //容器事件对象
        protected ContainerEvent event;

        // TODO: 17/3/27 by zmyer
        public ContainerEventProcessor(ContainerEvent event) {
            this.event = event;
        }

        // TODO: 17/3/27 by zmyer
        @Override
        public void run() {
            //读取容器id
            ContainerId containerId = event.getContainerId();
            LOG.info("Processing Event " + event + " for Container " + containerId);
            if (event.getType() == ContainerEventType.QUERY_CONTAINER)
                try {
                    //如果是查询容器运行状态信息,直接发送查询消息
                    ContainerStatus containerStatus = client.getContainerStatus(
                        containerId, event.getNodeId());
                    try {
                        //开始进行异步应答回调处理
                        callbackHandler.onContainerStatusReceived(
                            containerId, containerStatus);
                    } catch (Throwable thr) {
                        // Don't process user created unchecked exception
                        LOG.info(
                            "Unchecked exception is thrown from onContainerStatusReceived" +
                                " for Container " + event.getContainerId(), thr);
                    }
                } catch (Throwable t) {
                    //失败处理
                    onExceptionRaised(containerId, t);
                }
            else {
                //如果不是查询指令,则首先读取相应的容器对象
                StatefulContainer container = containers.get(containerId);
                if (container == null) {
                    LOG.info("Container " + containerId + " is already stopped or failed");
                } else {
                    //进入容器状态机进行事件处理
                    container.handle(event);
                    if (isCompletelyDone(container)) {
                        //如果当前的容器处理完毕,则直接从容器列表中删除该容器
                        containers.remove(containerId);
                    }
                }
            }
        }

        // TODO: 17/3/27 by zmyer
        private void onExceptionRaised(ContainerId containerId, Throwable t) {
            try {
                //失败回调
                callbackHandler.onGetContainerStatusError(containerId, t);
            } catch (Throwable thr) {
                // Don't process user created unchecked exception
                LOG.info("Unchecked exception is thrown from onGetContainerStatusError" +
                    " for Container " + containerId, thr);
            }
        }
    }
}
