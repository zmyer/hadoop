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
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.metrics.RetryCacheMetrics;
import org.apache.hadoop.util.LightWeightCache;
import org.apache.hadoop.util.LightWeightGSet;
import org.apache.hadoop.util.LightWeightGSet.LinkedElement;

/**
 * Maintains a cache of non-idempotent requests that have been successfully
 * processed by the RPC server implementation, to handle the retries. A request
 * is uniquely identified by the unique client ID + call ID of the RPC request.
 * On receiving retried request, an entry will be found in the
 * {@link RetryCache} and the previous response is sent back to the request.
 * <p>
 * To look an implementation using this cache, see HDFS FSNamesystem class.
 */
// TODO: 17/3/19 by zmyer
@InterfaceAudience.Private
public class RetryCache {
    public static final Log LOG = LogFactory.getLog(RetryCache.class);
    //重试缓存统计对象
    private final RetryCacheMetrics retryCacheMetrics;
    //缓存最大的容量
    private static final int MAX_CAPACITY = 16;

    /**
     * CacheEntry is tracked using unique client ID and callId of the RPC request
     */
    // TODO: 17/3/19 by zmyer
    public static class CacheEntry implements LightWeightCache.Entry {
        /**
         * Processing state of the requests
         */
        //处理中的请求数量
        private static byte INPROGRESS = 0;
        //处理成功的请求数量
        private static byte SUCCESS = 1;
        //处理失败的请求数量
        private static byte FAILED = 2;

        //默认状态为处理中
        private byte state = INPROGRESS;

        // Store uuid as two long for better memory utilization
        private final long clientIdMsb; // Most signficant bytes
        private final long clientIdLsb; // Least significant bytes

        //call id
        private final int callId;
        //超时时间
        private final long expirationTime;
        //下一个缓存元素的地址
        private LightWeightGSet.LinkedElement next;

        // TODO: 17/3/19 by zmyer
        CacheEntry(byte[] clientId, int callId, long expirationTime) {
            // ClientId must be a UUID - that is 16 octets.
            Preconditions.checkArgument(clientId.length == ClientId.BYTE_LENGTH,
                "Invalid clientId - length is " + clientId.length
                    + " expected length " + ClientId.BYTE_LENGTH);
            // Convert UUID bytes to two longs
            clientIdMsb = ClientId.getMsb(clientId);
            clientIdLsb = ClientId.getLsb(clientId);
            this.callId = callId;
            this.expirationTime = expirationTime;
        }

        // TODO: 17/3/19 by zmyer
        CacheEntry(byte[] clientId, int callId, long expirationTime,
            boolean success) {
            this(clientId, callId, expirationTime);
            this.state = success ? SUCCESS : FAILED;
        }

        // TODO: 17/3/19 by zmyer
        private static int hashCode(long value) {
            return (int) (value ^ (value >>> 32));
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public int hashCode() {
            return (hashCode(clientIdMsb) * 31 + hashCode(clientIdLsb)) * 31 + callId;
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof CacheEntry)) {
                return false;
            }
            CacheEntry other = (CacheEntry) obj;
            return callId == other.callId && clientIdMsb == other.clientIdMsb
                && clientIdLsb == other.clientIdLsb;
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public void setNext(LinkedElement next) {
            this.next = next;
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public LinkedElement getNext() {
            return next;
        }

        // TODO: 17/3/19 by zmyer
        synchronized void completed(boolean success) {
            state = success ? SUCCESS : FAILED;
            this.notifyAll();
        }

        // TODO: 17/3/19 by zmyer
        public synchronized boolean isSuccess() {
            return state == SUCCESS;
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public void setExpirationTime(long timeNano) {
            // expiration time does not change
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public long getExpirationTime() {
            return expirationTime;
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public String toString() {
            return (new UUID(this.clientIdMsb, this.clientIdLsb)).toString() + ":"
                + this.callId + ":" + this.state;
        }
    }

    /**
     * CacheEntry with payload that tracks the previous response or parts of
     * previous response to be used for generating response for retried requests.
     */
    // TODO: 17/3/19 by zmyer
    public static class CacheEntryWithPayload extends CacheEntry {
        //缓存负载对象
        private Object payload;

        // TODO: 17/3/19 by zmyer
        CacheEntryWithPayload(byte[] clientId, int callId, Object payload,
            long expirationTime) {
            super(clientId, callId, expirationTime);
            this.payload = payload;
        }

        // TODO: 17/3/19 by zmyer
        CacheEntryWithPayload(byte[] clientId, int callId, Object payload,
            long expirationTime, boolean success) {
            super(clientId, callId, expirationTime, success);
            this.payload = payload;
        }

        /** Override equals to avoid findbugs warnings */
        // TODO: 17/3/19 by zmyer
        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }

        /** Override hashcode to avoid findbugs warnings */
        // TODO: 17/3/19 by zmyer
        @Override
        public int hashCode() {
            return super.hashCode();
        }

        // TODO: 17/3/19 by zmyer
        public Object getPayload() {
            return payload;
        }
    }

    //缓存对象集合
    private final LightWeightGSet<CacheEntry, CacheEntry> set;
    //超时时间
    private final long expirationTime;
    //缓存名称
    private String cacheName;

    //缓存重入锁对象
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Constructor
     *
     * @param cacheName name to identify the cache by
     * @param percentage percentage of total java heap space used by this cache
     * @param expirationTime time for an entry to expire in nanoseconds
     */
    // TODO: 17/3/19 by zmyer
    public RetryCache(String cacheName, double percentage, long expirationTime) {
        //计算缓存集合容量
        int capacity = LightWeightGSet.computeCapacity(percentage, cacheName);
        capacity = capacity > MAX_CAPACITY ? capacity : MAX_CAPACITY;
        //分配缓存集合对象
        this.set = new LightWeightCache<CacheEntry, CacheEntry>(capacity, capacity,
            expirationTime, 0);
        //设置超时时间
        this.expirationTime = expirationTime;
        //设置缓存名称
        this.cacheName = cacheName;
        //设置重试缓存统计对象
        this.retryCacheMetrics = RetryCacheMetrics.create(this);
    }

    // TODO: 17/3/19 by zmyer
    private static boolean skipRetryCache() {
        // Do not track non RPC invocation or RPC requests with
        // invalid callId or clientId in retry cache
        return !Server.isRpcInvocation() || Server.getCallId() < 0
            || Arrays.equals(Server.getClientId(), RpcConstants.DUMMY_CLIENT_ID);
    }

    // TODO: 17/3/19 by zmyer
    public void lock() {
        this.lock.lock();
    }

    // TODO: 17/3/19 by zmyer
    public void unlock() {
        this.lock.unlock();
    }

    // TODO: 17/3/19 by zmyer
    private void incrCacheClearedCounter() {
        retryCacheMetrics.incrCacheCleared();
    }

    // TODO: 17/3/19 by zmyer
    @VisibleForTesting
    public LightWeightGSet<CacheEntry, CacheEntry> getCacheSet() {
        return set;
    }

    // TODO: 17/3/19 by zmyer
    @VisibleForTesting
    public RetryCacheMetrics getMetricsForTests() {
        return retryCacheMetrics;
    }

    /**
     * This method returns cache name for metrics.
     */
    // TODO: 17/3/19 by zmyer
    public String getCacheName() {
        return cacheName;
    }

    /**
     * This method handles the following conditions:
     * <ul>
     * <li>If retry is not to be processed, return null</li>
     * <li>If there is no cache entry, add a new entry {@code newEntry} and return
     * it.</li>
     * <li>If there is an existing entry, wait for its completion. If the
     * completion state is {@link CacheEntry#FAILED}, the expectation is that the
     * thread that waited for completion, retries the request. the
     * {@link CacheEntry} state is set to {@link CacheEntry#INPROGRESS} again.
     * <li>If the completion state is {@link CacheEntry#SUCCESS}, the entry is
     * returned so that the thread that waits for it can can return previous
     * response.</li>
     * <ul>
     *
     * @return {@link CacheEntry}.
     */
    // TODO: 17/3/19 by zmyer
    private CacheEntry waitForCompletion(CacheEntry newEntry) {
        CacheEntry mapEntry = null;
        lock.lock();
        try {
            mapEntry = set.get(newEntry);
            // If an entry in the cache does not exist, add a new one
            if (mapEntry == null) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Adding Rpc request clientId "
                        + newEntry.clientIdMsb + newEntry.clientIdLsb + " callId "
                        + newEntry.callId + " to retryCache");
                }
                set.put(newEntry);
                retryCacheMetrics.incrCacheUpdated();
                return newEntry;
            } else {
                retryCacheMetrics.incrCacheHit();
            }
        } finally {
            lock.unlock();
        }
        // Entry already exists in cache. Wait for completion and return its state
        Preconditions.checkNotNull(mapEntry,
            "Entry from the cache should not be null");
        // Wait for in progress request to complete
        synchronized (mapEntry) {
            while (mapEntry.state == CacheEntry.INPROGRESS) {
                try {
                    mapEntry.wait();
                } catch (InterruptedException ie) {
                    // Restore the interrupted status
                    Thread.currentThread().interrupt();
                }
            }
            // Previous request has failed, the expectation is is that it will be
            // retried again.
            if (mapEntry.state != CacheEntry.SUCCESS) {
                mapEntry.state = CacheEntry.INPROGRESS;
            }
        }
        return mapEntry;
    }

    /**
     * Add a new cache entry into the retry cache. The cache entry consists of
     * clientId and callId extracted from editlog.
     */
    // TODO: 17/3/19 by zmyer
    public void addCacheEntry(byte[] clientId, int callId) {
        //创建缓存实体对象
        CacheEntry newEntry = new CacheEntry(clientId, callId, System.nanoTime()
            + expirationTime, true);
        lock.lock();
        try {
            //将缓存对象插入到集合中
            set.put(newEntry);
        } finally {
            lock.unlock();
        }
        retryCacheMetrics.incrCacheUpdated();
    }

    // TODO: 17/3/19 by zmyer
    public void addCacheEntryWithPayload(byte[] clientId, int callId,
        Object payload) {
        // since the entry is loaded from editlog, we can assume it succeeded.
        //创建缓存对象
        CacheEntry newEntry = new CacheEntryWithPayload(clientId, callId, payload,
            System.nanoTime() + expirationTime, true);
        lock.lock();
        try {
            //将缓存对象插入到集合中
            set.put(newEntry);
        } finally {
            lock.unlock();
        }
        retryCacheMetrics.incrCacheUpdated();
    }

    // TODO: 17/3/19 by zmyer
    private static CacheEntry newEntry(long expirationTime) {
        //创建缓存对象
        return new CacheEntry(Server.getClientId(), Server.getCallId(),
            System.nanoTime() + expirationTime);
    }

    // TODO: 17/3/19 by zmyer
    private static CacheEntryWithPayload newEntry(Object payload,
        long expirationTime) {
        return new CacheEntryWithPayload(Server.getClientId(), Server.getCallId(),
            payload, System.nanoTime() + expirationTime);
    }

    /** Static method that provides null check for retryCache */
    // TODO: 17/3/19 by zmyer
    public static CacheEntry waitForCompletion(RetryCache cache) {
        if (skipRetryCache()) {
            return null;
        }
        return cache != null ? cache
            .waitForCompletion(newEntry(cache.expirationTime)) : null;
    }

    /** Static method that provides null check for retryCache */
    // TODO: 17/3/19 by zmyer
    public static CacheEntryWithPayload waitForCompletion(RetryCache cache,
        Object payload) {
        if (skipRetryCache()) {
            return null;
        }
        return (CacheEntryWithPayload) (cache != null ? cache
            .waitForCompletion(newEntry(payload, cache.expirationTime)) : null);
    }

    // TODO: 17/3/19 by zmyer
    public static void setState(CacheEntry e, boolean success) {
        if (e == null) {
            return;
        }
        e.completed(success);
    }

    // TODO: 17/3/19 by zmyer
    public static void setState(CacheEntryWithPayload e, boolean success,
        Object payload) {
        if (e == null) {
            return;
        }
        e.payload = payload;
        e.completed(success);
    }

    // TODO: 17/3/19 by zmyer
    public static void clear(RetryCache cache) {
        if (cache != null) {
            cache.set.clear();
            cache.incrCacheClearedCounter();
        }
    }
}
