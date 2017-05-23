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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Determines which queue to start reading from, occasionally drawing from
 * low-priority queues in order to prevent starvation. Given the pull pattern
 * [9, 4, 1] for 3 queues:
 *
 * The cycle is (a minimum of) 9+4+1=14 reads.
 * Queue 0 is read (at least) 9 times
 * Queue 1 is read (at least) 4 times
 * Queue 2 is read (at least) 1 time
 * Repeat
 *
 * There may be more reads than the minimum due to race conditions. This is
 * allowed by design for performance reasons.
 */
// TODO: 17/3/19 by zmyer
public class WeightedRoundRobinMultiplexer implements RpcMultiplexer {
    // Config keys
    public static final String IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY =
        "faircallqueue.multiplexer.weights";

    public static final Log LOG = LogFactory.getLog(WeightedRoundRobinMultiplexer.class);

    //队列数
    private final int numQueues; // The number of queues under our provisioning

    //当前的队列索引
    private final AtomicInteger currentQueueIndex; // Current queue we're serving
    //当前的请求的数量
    private final AtomicInteger requestsLeft; // Number of requests left for this queue

    //队列权重集合
    private int[] queueWeights; // The weights for each queue

    // TODO: 17/3/19 by zmyer
    public WeightedRoundRobinMultiplexer(int aNumQueues, String ns,
        Configuration conf) {
        if (aNumQueues <= 0) {
            throw new IllegalArgumentException("Requested queues (" + aNumQueues +
                ") must be greater than zero.");
        }

        //设置队列数
        this.numQueues = aNumQueues;
        //读取队列权重
        this.queueWeights = conf.getInts(ns + "." + IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY);

        //如果队列权重数组为空,则使用默认
        if (this.queueWeights.length == 0) {
            //读取默认的队列权重
            this.queueWeights = getDefaultQueueWeights(this.numQueues);
        } else if (this.queueWeights.length != this.numQueues) {
            throw new IllegalArgumentException(ns + "." +
                IPC_CALLQUEUE_WRRMUX_WEIGHTS_KEY + " must specify exactly " +
                this.numQueues + " weights: one for each priority level.");
        }

        //设置当前队列索引
        this.currentQueueIndex = new AtomicInteger(0);
        //初始化待处理的请求数量
        this.requestsLeft = new AtomicInteger(this.queueWeights[0]);

        LOG.info("WeightedRoundRobinMultiplexer is being used.");
    }

    /**
     * Creates default weights for each queue. The weights are 2^N.
     */
    // TODO: 17/3/19 by zmyer
    private int[] getDefaultQueueWeights(int aNumQueues) {
        //创建队列权重数组
        int[] weights = new int[aNumQueues];

        int weight = 1; // Start low
        for (int i = aNumQueues - 1; i >= 0; i--) { // Start at lowest queue
            //为每个队列设置权重
            weights[i] = weight;
            weight *= 2; // Double every iteration
        }
        //返回权重列表
        return weights;
    }

    /**
     * Move to the next queue.
     */
    // TODO: 17/3/19 by zmyer
    private void moveToNextQueue() {
        //读取当前的队列索引
        int thisIdx = this.currentQueueIndex.get();

        // Wrap to fit in our bounds
        //计算下一个队列索引
        int nextIdx = (thisIdx + 1) % this.numQueues;

        // Set to next index: once this is called, requests will start being
        // drawn from nextIdx, but requestsLeft will continue to decrement into
        // the negatives
        //更新当前的队列索引
        this.currentQueueIndex.set(nextIdx);

        // Finally, reset requestsLeft. This will enable moveToNextQueue to be
        // called again, for the new currentQueueIndex
        //初始化等待处理的请求数量
        this.requestsLeft.set(this.queueWeights[nextIdx]);
    }

    /**
     * Advances the index, which will change the current index
     * if called enough times.
     */
    // TODO: 17/3/19 by zmyer
    private void advanceIndex() {
        // Since we did read, we should decrement
        //递减等待处理的请求数量
        int requestsLeftVal = this.requestsLeft.decrementAndGet();

        // Strict compare with zero (instead of inequality) so that if another
        // thread decrements requestsLeft, only one thread will be responsible
        // for advancing currentQueueIndex
        if (requestsLeftVal == 0) {
            // This is guaranteed to be called exactly once per currentQueueIndex
            //如果当前的队列处理完了指定的请求数量,则移到下一个队列继续处理
            this.moveToNextQueue();
        }
    }

    /**
     * Gets the current index. Should be accompanied by a call to
     * advanceIndex at some point.
     */
    // TODO: 17/3/19 by zmyer
    private int getCurrentIndex() {
        return this.currentQueueIndex.get();
    }

    /**
     * Use the mux by getting and advancing index.
     */
    // TODO: 17/3/19 by zmyer
    public int getAndAdvanceCurrentIndex() {
        int idx = this.getCurrentIndex();
        this.advanceIndex();
        return idx;
    }

}
