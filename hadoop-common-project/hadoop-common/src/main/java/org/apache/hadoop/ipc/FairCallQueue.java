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

import com.google.common.annotations.VisibleForTesting;
import java.lang.ref.WeakReference;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.util.MBeans;

/**
 * A queue with multiple levels for each priority.
 */
// TODO: 17/3/19 by zmyer
public class FairCallQueue<E extends Schedulable> extends AbstractQueue<E>
    implements BlockingQueue<E> {
    @Deprecated
    public static final int IPC_CALLQUEUE_PRIORITY_LEVELS_DEFAULT = 4;
    @Deprecated
    public static final String IPC_CALLQUEUE_PRIORITY_LEVELS_KEY =
        "faircallqueue.priority-levels";

    public static final Log LOG = LogFactory.getLog(FairCallQueue.class);

    //call队列列表
    /* The queues */
    private final ArrayList<BlockingQueue<E>> queues;

    /* Read locks */
    //提取call的重入锁对象
    private final ReentrantLock takeLock = new ReentrantLock();
    //call队列的非空条件变量
    private final Condition notEmpty = takeLock.newCondition();

    // TODO: 17/3/19 by zmyer
    private void signalNotEmpty() {
        takeLock.lock();
        try {
            //发送非空条件变量
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }

    /* Multiplexer picks which queue to draw from */
    //rpc多路复用器对象
    private RpcMultiplexer multiplexer;

    /* Statistic tracking */
    private final ArrayList<AtomicLong> overflowedCalls;

    /**
     * Create a FairCallQueue.
     *
     * @param capacity the total size of all sub-queues
     * @param ns the prefix to use for configuration
     * @param conf the configuration to read from Notes: Each sub-queue has a capacity of `capacity / numSubqueues`. The
     * first or the highest priority sub-queue has an excess capacity of `capacity % numSubqueues`
     */
    // TODO: 17/3/19 by zmyer
    public FairCallQueue(int priorityLevels, int capacity, String ns, Configuration conf) {
        if (priorityLevels < 1) {
            throw new IllegalArgumentException("Number of Priority Levels must be " +
                "at least 1");
        }
        //设置优先级
        int numQueues = priorityLevels;
        LOG.info("FairCallQueue is in use with " + numQueues +
            " queues with total capacity of " + capacity);

        //创建call队列
        this.queues = new ArrayList<BlockingQueue<E>>(numQueues);
        //
        this.overflowedCalls = new ArrayList<AtomicLong>(numQueues);
        //计算每个队列的容量
        int queueCapacity = capacity / numQueues;
        //设置第一个call队列的容量
        int capacityForFirstQueue = queueCapacity + (capacity % numQueues);
        for (int i = 0; i < numQueues; i++) {
            if (i == 0) {
                //创建第一个call队列
                this.queues.add(new LinkedBlockingQueue<E>(capacityForFirstQueue));
            } else {
                //创建call队列
                this.queues.add(new LinkedBlockingQueue<E>(queueCapacity));
            }
            this.overflowedCalls.add(new AtomicLong(0));
        }

        //创建多路复用器对象
        this.multiplexer = new WeightedRoundRobinMultiplexer(numQueues, ns, conf);
        // Make this the active source of metrics
        //创建统计代理对象
        MetricsProxy mp = MetricsProxy.getInstance(ns);
        //为统计代理注册该对象
        mp.setDelegate(this);
    }

    /**
     * Returns the first non-empty queue with equal or lesser priority
     * than <i>startIdx</i>. Wraps around, searching a maximum of N
     * queues, where N is this.queues.size().
     *
     * @param startIdx the queue number to start searching at
     * @return the first non-empty queue with less priority, or null if everything was empty
     */
    // TODO: 17/3/19 by zmyer
    private BlockingQueue<E> getFirstNonEmptyQueue(int startIdx) {
        //统计当前的call队列数量
        final int numQueues = this.queues.size();
        for (int i = 0; i < numQueues; i++) {
            //每个call队列的索引
            int idx = (i + startIdx) % numQueues; // offset and wrap around
            //读取call队列
            BlockingQueue<E> queue = this.queues.get(idx);
            if (queue.size() != 0) {
                //如果当前的call队列不为空,则直接返回
                return queue;
            }
        }
        // All queues were empty
        return null;
    }

  /* AbstractQueue and BlockingQueue methods */

    /**
     * Put and offer follow the same pattern:
     * 1. Get the assigned priorityLevel from the call by scheduler
     * 2. Get the nth sub-queue matching this priorityLevel
     * 3. delegate the call to this sub-queue.
     *
     * But differ in how they handle overflow:
     * - Put will move on to the next queue until it lands on the last queue
     * - Offer does not attempt other queues on overflow
     */
    // TODO: 17/3/19 by zmyer
    @Override
    public void put(E e) throws InterruptedException {
        //获取优先级
        int priorityLevel = e.getPriorityLevel();

        //获取call队列的队列数量
        final int numLevels = this.queues.size();
        while (true) {
            //根据优先级,获取对应的call队列
            BlockingQueue<E> q = this.queues.get(priorityLevel);
            //将元素插入到对应的call队列中
            boolean res = q.offer(e);
            if (!res) {
                // Update stats
                //更新统计信息
                this.overflowedCalls.get(priorityLevel).getAndIncrement();

                // If we failed to insert, try again on the next level
                //递增优先级
                priorityLevel++;

                if (priorityLevel == numLevels) {
                    // That was the last one, we will block on put in the last queue
                    // Delete this line to drop the call
                    //将当前的元素直接插入到最后一个call队列中
                    this.queues.get(priorityLevel - 1).put(e);
                    break;
                }
            } else {
                break;
            }
        }
        //发送call队列非空信号
        signalNotEmpty();
    }

    // TODO: 17/3/19 by zmyer
    @Override
    public boolean offer(E e, long timeout, TimeUnit unit)
        throws InterruptedException {
        //读取优先级
        int priorityLevel = e.getPriorityLevel();
        //读取优先级对应的call队列
        BlockingQueue<E> q = this.queues.get(priorityLevel);
        //将当前的元素插入到对应的队列总
        boolean ret = q.offer(e, timeout, unit);
        //发送非空信号
        signalNotEmpty();

        return ret;
    }

    // TODO: 17/3/19 by zmyer
    @Override
    public boolean offer(E e) {
        //读取优先级
        int priorityLevel = e.getPriorityLevel();
        //获取优先级对应的call队列
        BlockingQueue<E> q = this.queues.get(priorityLevel);
        //插入call队列
        boolean ret = q.offer(e);
        //发送非空信号
        signalNotEmpty();

        return ret;
    }

    // TODO: 17/3/19 by zmyer
    @Override
    public E take() throws InterruptedException {
        //读取call队列对应的索引号
        int startIdx = this.multiplexer.getAndAdvanceCurrentIndex();

        takeLock.lockInterruptibly();
        try {
            // Wait while queue is empty
            for (; ; ) {
                //读取第一个非空的call队列
                BlockingQueue<E> q = this.getFirstNonEmptyQueue(startIdx);
                if (q != null) {
                    // Got queue, so return if we can poll out an object
                    //从非空call队列中读取call对象
                    E e = q.poll();
                    if (e != null) {
                        return e;
                    }
                }
                //如果所有的call队列都为空,则直接等待
                notEmpty.await();
            }
        } finally {
            takeLock.unlock();
        }
    }

    // TODO: 17/3/19 by zmyer
    @Override
    public E poll(long timeout, TimeUnit unit)
        throws InterruptedException {

        //读取call队列的开始索引
        int startIdx = this.multiplexer.getAndAdvanceCurrentIndex();

        long nanos = unit.toNanos(timeout);
        takeLock.lockInterruptibly();
        try {
            for (; ; ) {
                //读取第一个非空的call队列
                BlockingQueue<E> q = this.getFirstNonEmptyQueue(startIdx);
                if (q != null) {
                    //从call队列中读取call对象
                    E e = q.poll();
                    if (e != null) {
                        // Escape condition: there might be something available
                        return e;
                    }
                }

                if (nanos <= 0) {
                    // Wait has elapsed
                    return null;
                }

                try {
                    // Now wait on the condition for a bit. If we get
                    // spuriously awoken we'll re-loop
                    //等待非空信号
                    nanos = notEmpty.awaitNanos(nanos);
                } catch (InterruptedException ie) {
                    //如果出现异常了,直接返回非空信号
                    notEmpty.signal(); // propagate to a non-interrupted thread
                    throw ie;
                }
            }
        } finally {
            takeLock.unlock();
        }
    }

    /**
     * poll() provides no strict consistency: it is possible for poll to return
     * null even though an element is in the queue.
     */
    // TODO: 17/3/19 by zmyer
    @Override
    public E poll() {
        //读取call队列的开始索引
        int startIdx = this.multiplexer.getAndAdvanceCurrentIndex();

        //读取第一个非空的call队列
        BlockingQueue<E> q = this.getFirstNonEmptyQueue(startIdx);
        if (q == null) {
            return null; // everything is empty
        }

        // Delegate to the sub-queue's poll, which could still return null
        //开始从call队列中读取call对象
        return q.poll();
    }

    /**
     * Peek, like poll, provides no strict consistency.
     */
    // TODO: 17/3/19 by zmyer
    @Override
    public E peek() {
        //读取第一个call队列
        BlockingQueue<E> q = this.getFirstNonEmptyQueue(0);
        if (q == null) {
            return null;
        } else {
            //返回队列头部
            return q.peek();
        }
    }

    /**
     * Size returns the sum of all sub-queue sizes, so it may be greater than
     * capacity.
     * Note: size provides no strict consistency, and should not be used to
     * control queue IO.
     */
    // TODO: 17/3/19 by zmyer
    @Override
    public int size() {
        int size = 0;
        for (BlockingQueue<E> q : this.queues) {
            size += q.size();
        }
        return size;
    }

    /**
     * Iterator is not implemented, as it is not needed.
     */
    // TODO: 17/3/19 by zmyer
    @Override
    public Iterator<E> iterator() {
        throw new NotImplementedException();
    }

    /**
     * drainTo defers to each sub-queue. Note that draining from a FairCallQueue
     * to another FairCallQueue will likely fail, since the incoming calls
     * may be scheduled differently in the new FairCallQueue. Nonetheless this
     * method is provided for completeness.
     */
    // TODO: 17/3/19 by zmyer
    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        int sum = 0;
        for (BlockingQueue<E> q : this.queues) {
            sum += q.drainTo(c, maxElements);
        }
        return sum;
    }

    // TODO: 17/3/19 by zmyer
    @Override
    public int drainTo(Collection<? super E> c) {
        int sum = 0;
        for (BlockingQueue<E> q : this.queues) {
            sum += q.drainTo(c);
        }
        return sum;
    }

    /**
     * Returns maximum remaining capacity. This does not reflect how much you can
     * ideally fit in this FairCallQueue, as that would depend on the scheduler's
     * decisions.
     */
    // TODO: 17/3/19 by zmyer
    @Override
    public int remainingCapacity() {
        int sum = 0;
        for (BlockingQueue<E> q : this.queues) {
            sum += q.remainingCapacity();
        }
        return sum;
    }

    /**
     * MetricsProxy is a singleton because we may init multiple
     * FairCallQueues, but the metrics system cannot unregister beans cleanly.
     */
    // TODO: 17/3/19 by zmyer
    private static final class MetricsProxy implements FairCallQueueMXBean {
        // One singleton per namespace
        private static final HashMap<String, MetricsProxy> INSTANCES =
            new HashMap<String, MetricsProxy>();

        // Weakref for delegate, so we don't retain it forever if it can be GC'd
        private WeakReference<FairCallQueue<? extends Schedulable>> delegate;

        // Keep track of how many objects we registered
        private int revisionNumber = 0;

        private MetricsProxy(String namespace) {
            MBeans.register(namespace, "FairCallQueue", this);
        }

        // TODO: 17/3/19 by zmyer
        public static synchronized MetricsProxy getInstance(String namespace) {
            MetricsProxy mp = INSTANCES.get(namespace);
            if (mp == null) {
                // We must create one
                mp = new MetricsProxy(namespace);
                INSTANCES.put(namespace, mp);
            }
            return mp;
        }

        // TODO: 17/3/19 by zmyer
        public void setDelegate(FairCallQueue<? extends Schedulable> obj) {
            this.delegate
                = new WeakReference<FairCallQueue<? extends Schedulable>>(obj);
            this.revisionNumber++;
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public int[] getQueueSizes() {
            FairCallQueue<? extends Schedulable> obj = this.delegate.get();
            if (obj == null) {
                return new int[] {};
            }

            return obj.getQueueSizes();
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public long[] getOverflowedCalls() {
            FairCallQueue<? extends Schedulable> obj = this.delegate.get();
            if (obj == null) {
                return new long[] {};
            }

            return obj.getOverflowedCalls();
        }

        // TODO: 17/3/19 by zmyer
        @Override public int getRevision() {
            return revisionNumber;
        }
    }

    // TODO: 17/3/19 by zmyer
    // FairCallQueueMXBean
    public int[] getQueueSizes() {
        //读取队列数
        int numQueues = queues.size();
        //创建队列大小数组
        int[] sizes = new int[numQueues];
        for (int i = 0; i < numQueues; i++) {
            //设置每个队列的大小
            sizes[i] = queues.get(i).size();
        }
        return sizes;
    }

    // TODO: 17/3/19 by zmyer
    public long[] getOverflowedCalls() {
        int numQueues = queues.size();
        long[] calls = new long[numQueues];
        for (int i = 0; i < numQueues; i++) {
            calls[i] = overflowedCalls.get(i).get();
        }
        return calls;
    }

    // TODO: 17/3/19 by zmyer
    @VisibleForTesting
    public void setMultiplexer(RpcMultiplexer newMux) {
        this.multiplexer = newMux;
    }
}
