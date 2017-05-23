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

package org.apache.hadoop.ipc;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;

/**
 * Abstracts queue operations for different blocking queues.
 */
// TODO: 17/3/18 by zmyer
public class CallQueueManager<E> {
    public static final Log LOG = LogFactory.getLog(CallQueueManager.class);
    // Number of checkpoints for empty queue.
    //检查点数量
    private static final int CHECKPOINT_NUM = 20;
    // Interval to check empty queue.
    //检查点时间间隔
    private static final long CHECKPOINT_INTERVAL_MS = 10;

    // TODO: 17/3/18 by zmyer
    @SuppressWarnings("unchecked")
    static <E> Class<? extends BlockingQueue<E>> convertQueueClass(
        Class<?> queueClass, Class<E> elementClass) {
        //队列类型转换
        return (Class<? extends BlockingQueue<E>>) queueClass;
    }

    // TODO: 17/3/15 by zmyer
    @SuppressWarnings("unchecked")
    static Class<? extends RpcScheduler> convertSchedulerClass(
        Class<?> schedulerClass) {
        return (Class<? extends RpcScheduler>) schedulerClass;
    }

    private volatile boolean clientBackOffEnabled;

    // Atomic refs point to active callQueue
    // We have two so we can better control swapping
    //写入队列
    private final AtomicReference<BlockingQueue<E>> putRef;
    //提取队列
    private final AtomicReference<BlockingQueue<E>> takeRef;

    //rpc调度器
    private RpcScheduler scheduler;

    // TODO: 17/3/18 by zmyer
    public CallQueueManager(Class<? extends BlockingQueue<E>> backingClass,
        Class<? extends RpcScheduler> schedulerClass,
        boolean clientBackOffEnabled, int maxQueueSize, String namespace,
        Configuration conf) {
        //优先等级
        int priorityLevels = parseNumLevels(namespace, conf);
        //创建调度器
        this.scheduler = createScheduler(schedulerClass, priorityLevels,
            namespace, conf);
        //创建call队列
        BlockingQueue<E> bq = createCallQueueInstance(backingClass,
            priorityLevels, maxQueueSize, namespace, conf);
        //
        this.clientBackOffEnabled = clientBackOffEnabled;
        //创建写入队列
        this.putRef = new AtomicReference<>(bq);
        //创建提取队列
        this.takeRef = new AtomicReference<>(bq);
        LOG.info("Using callQueue: " + backingClass + " queueCapacity: " +
            maxQueueSize + " scheduler: " + schedulerClass);
    }

    // TODO: 17/3/18 by zmyer
    private static <T extends RpcScheduler> T createScheduler(
        Class<T> theClass, int priorityLevels, String ns, Configuration conf) {
        // Used for custom, configurable scheduler
        try {
            //根据提供的类对象,获取指定的构造函数
            Constructor<T> ctor = theClass.getDeclaredConstructor(int.class,
                String.class, Configuration.class);
            //根据构造函数,创建实例
            return ctor.newInstance(priorityLevels, ns, conf);
        } catch (RuntimeException e) {
            throw e;
        } catch (InvocationTargetException e) {
            throw new RuntimeException(theClass.getName()
                + " could not be constructed.", e.getCause());
        } catch (Exception e) {
        }

        try {
            //构造函数
            Constructor<T> ctor = theClass.getDeclaredConstructor(int.class);
            //实例化对象
            return ctor.newInstance(priorityLevels);
        } catch (RuntimeException e) {
            throw e;
        } catch (InvocationTargetException e) {
            throw new RuntimeException(theClass.getName()
                + " could not be constructed.", e.getCause());
        } catch (Exception e) {
        }

        // Last attempt
        try {
            Constructor<T> ctor = theClass.getDeclaredConstructor();
            return ctor.newInstance();
        } catch (RuntimeException e) {
            throw e;
        } catch (InvocationTargetException e) {
            throw new RuntimeException(theClass.getName()
                + " could not be constructed.", e.getCause());
        } catch (Exception e) {
        }

        // Nothing worked
        throw new RuntimeException(theClass.getName() +
            " could not be constructed.");
    }

    // TODO: 17/3/18 by zmyer
    private <T extends BlockingQueue<E>> T createCallQueueInstance(
        Class<T> theClass, int priorityLevels, int maxLen, String ns, Configuration conf) {

        // Used for custom, configurable callqueues
        try {
            //获取构造函数
            Constructor<T> ctor = theClass.getDeclaredConstructor(int.class,
                int.class, String.class, Configuration.class);
            //实例化对象
            return ctor.newInstance(priorityLevels, maxLen, ns, conf);
        } catch (RuntimeException e) {
            throw e;
        } catch (InvocationTargetException e) {
            throw new RuntimeException(theClass.getName()
                + " could not be constructed.", e.getCause());
        } catch (Exception ignored) {
        }

        // Used for LinkedBlockingQueue, ArrayBlockingQueue, etc
        try {
            //构造函数
            Constructor<T> ctor = theClass.getDeclaredConstructor(int.class);
            //实例化
            return ctor.newInstance(maxLen);
        } catch (RuntimeException e) {
            throw e;
        } catch (InvocationTargetException e) {
            throw new RuntimeException(theClass.getName()
                + " could not be constructed.", e.getCause());
        } catch (Exception e) {
        }

        // Last attempt
        try {
            Constructor<T> ctor = theClass.getDeclaredConstructor();
            return ctor.newInstance();
        } catch (RuntimeException e) {
            throw e;
        } catch (InvocationTargetException e) {
            throw new RuntimeException(theClass.getName()
                + " could not be constructed.", e.getCause());
        } catch (Exception e) {
        }

        // Nothing worked
        throw new RuntimeException(theClass.getName() +
            " could not be constructed.");
    }

    // TODO: 17/3/18 by zmyer
    boolean isClientBackoffEnabled() {
        return clientBackOffEnabled;
    }

    // Based on policy to determine back off current call
    // TODO: 17/3/18 by zmyer
    boolean shouldBackOff(Schedulable e) {
        return scheduler.shouldBackOff(e);
    }

    // TODO: 17/3/18 by zmyer
    void addResponseTime(String name, int priorityLevel, int queueTime,
        int processingTime) {
        scheduler.addResponseTime(name, priorityLevel, queueTime, processingTime);
    }

    // This should be only called once per call and cached in the call object
    // each getPriorityLevel call will increment the counter for the caller
    // TODO: 17/3/18 by zmyer
    int getPriorityLevel(Schedulable e) {
        return scheduler.getPriorityLevel(e);
    }

    // TODO: 17/3/18 by zmyer
    void setClientBackoffEnabled(boolean value) {
        clientBackOffEnabled = value;
    }

    /**
     * Insert e into the backing queue or block until we can.
     * If we block and the queue changes on us, we will insert while the
     * queue is drained.
     */
    // TODO: 17/3/18 by zmyer
    public void put(E e) throws InterruptedException {
        putRef.get().put(e);
    }

    /**
     * Insert e into the backing queue.
     * Return true if e is queued.
     * Return false if the queue is full.
     */
    // TODO: 17/3/18 by zmyer
    public boolean offer(E e) throws InterruptedException {
        return putRef.get().offer(e);
    }

    /**
     * Retrieve an E from the backing queue or block until we can.
     * Guaranteed to return an element from the current queue.
     */
    // TODO: 17/3/18 by zmyer
    public E take() throws InterruptedException {
        E e = null;

        while (e == null) {
            //开始从提取队列中读取元素
            e = takeRef.get().poll(1000L, TimeUnit.MILLISECONDS);
        }
        return e;
    }

    // TODO: 17/3/18 by zmyer
    public int size() {
        return takeRef.get().size();
    }

    /**
     * Read the number of levels from the configuration.
     * This will affect the FairCallQueue's overall capacity.
     *
     * @throws IllegalArgumentException on invalid queue count
     */
    // TODO: 17/3/18 by zmyer
    @SuppressWarnings("deprecation")
    private static int parseNumLevels(String ns, Configuration conf) {
        // Fair call queue levels (IPC_CALLQUEUE_PRIORITY_LEVELS_KEY)
        // takes priority over the scheduler level key
        // (IPC_SCHEDULER_PRIORITY_LEVELS_KEY)
        int retval = conf.getInt(ns + "." +
            FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY, 0);
        if (retval == 0) { // No FCQ priority level configured
            retval = conf.getInt(ns + "." +
                    CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_KEY,
                CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_DEFAULT_KEY);
        } else {
            LOG.warn(ns + "." + FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY +
                " is deprecated. Please use " + ns + "." +
                CommonConfigurationKeys.IPC_SCHEDULER_PRIORITY_LEVELS_KEY + ".");
        }
        if (retval < 1) {
            throw new IllegalArgumentException("numLevels must be at least 1");
        }
        return retval;
    }

    /**
     * Replaces active queue with the newly requested one and transfers
     * all calls to the newQ before returning.
     */
    // TODO: 17/3/18 by zmyer
    public synchronized void swapQueue(
        Class<? extends RpcScheduler> schedulerClass,
        Class<? extends BlockingQueue<E>> queueClassToUse, int maxSize,
        String ns, Configuration conf) {
        //读取优先级
        int priorityLevels = parseNumLevels(ns, conf);
        //创建调度器
        RpcScheduler newScheduler = createScheduler(schedulerClass, priorityLevels,
            ns, conf);
        //创建call队列
        BlockingQueue<E> newQ = createCallQueueInstance(queueClassToUse,
            priorityLevels, maxSize, ns, conf);

        // Our current queue becomes the old queue
        //读取写入队列
        BlockingQueue<E> oldQ = putRef.get();

        // Swap putRef first: allow blocked puts() to be unblocked
        //替换新的call队列
        putRef.set(newQ);

        // Wait for handlers to drain the oldQ
        //等待队列为空
        while (!queueIsReallyEmpty(oldQ)) {
        }

        // Swap takeRef to handle new calls
        //替换新的提取call队列
        takeRef.set(newQ);

        //设置新的调度器
        this.scheduler = newScheduler;

        LOG.info("Old Queue: " + stringRepr(oldQ) + ", " + "Replacement: " + stringRepr(newQ));
    }

    /**
     * Checks if queue is empty by checking at CHECKPOINT_NUM points with
     * CHECKPOINT_INTERVAL_MS interval.
     * This doesn't mean the queue might not fill up at some point later, but
     * it should decrease the probability that we lose a call this way.
     */
    // TODO: 17/3/18 by zmyer
    private boolean queueIsReallyEmpty(BlockingQueue<?> q) {
        for (int i = 0; i < CHECKPOINT_NUM; i++) {
            try {
                Thread.sleep(CHECKPOINT_INTERVAL_MS);
            } catch (InterruptedException ie) {
                return false;
            }
            //检查队列是否为空
            if (!q.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private String stringRepr(Object o) {
        return o.getClass().getName() + '@' + Integer.toHexString(o.hashCode());
    }
}
