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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.SocketFactory;
import javax.security.sasl.Sasl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.Server.AuthProtocol;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SaslRpcClient;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.apache.htrace.core.Span;
import org.apache.htrace.core.Tracer;

import static org.apache.hadoop.ipc.RpcConstants.CONNECTION_CONTEXT_CALL_ID;
import static org.apache.hadoop.ipc.RpcConstants.PING_CALL_ID;

/**
 * A client for an IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 *
 * @see Server
 */
@Public
@InterfaceStability.Evolving
public class Client implements AutoCloseable {

    public static final Log LOG = LogFactory.getLog(Client.class);

    /** A counter for generating call IDs. */
    //call id生成器
    private static final AtomicInteger callIdCounter = new AtomicInteger();
    //当前callid
    private static final ThreadLocal<Integer> callId = new ThreadLocal<Integer>();
    //重试次数
    private static final ThreadLocal<Integer> retryCount = new ThreadLocal<Integer>();
    //外部call处理对象
    private static final ThreadLocal<Object> EXTERNAL_CALL_HANDLER = new ThreadLocal<>();
    //异步rpc应答对象
    private static final ThreadLocal<AsyncGet<? extends Writable, IOException>>
        ASYNC_RPC_RESPONSE = new ThreadLocal<>();
    //异步模式开关
    private static final ThreadLocal<Boolean> asynchronousMode =
        new ThreadLocal<Boolean>() {
            @Override
            protected Boolean initialValue() {
                return false;
            }
        };

    @SuppressWarnings("unchecked")
    @Unstable
    //获取异步应答
    public static <T extends Writable> AsyncGet<T, IOException>
    getAsyncRpcResponse() {
        return (AsyncGet<T, IOException>) ASYNC_RPC_RESPONSE.get();
    }

    /** Set call id and retry count for the next call. */
    // TODO: 17/3/12 by zmyer
    public static void setCallIdAndRetryCount(int cid, int rc,
        Object externalHandler) {
        Preconditions.checkArgument(cid != RpcConstants.INVALID_CALL_ID);
        Preconditions.checkState(callId.get() == null);
        Preconditions.checkArgument(rc != RpcConstants.INVALID_RETRY_COUNT);
        //设置callid
        callId.set(cid);
        //设置重试次数
        retryCount.set(rc);
        //设置外部call处理对象
        EXTERNAL_CALL_HANDLER.set(externalHandler);
    }

    //连接对象映射表
    private ConcurrentMap<ConnectionId, Connection> connections = new ConcurrentHashMap<>();

    //值类型
    private Class<? extends Writable> valueClass;   // class of call values
    //是否运行标记
    private AtomicBoolean running = new AtomicBoolean(true); // if client runs
    //客户端配置
    final private Configuration conf;
    //客户端套接字工厂对象
    private SocketFactory socketFactory;           // how to create sockets
    //客户端引用计数器
    private int refCount = 1;
    //连接超时时间
    private final int connectionTimeout;

    //回调标记
    private final boolean fallbackAllowed;
    //客户端id列表
    private final byte[] clientId;
    //最大的异步调用次数
    private final int maxAsyncCalls;
    //异步调用计数器对象
    private final AtomicInteger asyncCallCounter = new AtomicInteger(0);

    /**
     * Executor on which IPC calls' parameters are sent.
     * Deferring the sending of parameters to a separate
     * thread isolates them from thread interruptions in the
     * calling code.
     */
    //发送参数执行器
    private final ExecutorService sendParamsExecutor;
    //客户端服务执行工厂对象
    private final static ClientExecutorServiceFactory clientExcecutorFactory =
        new ClientExecutorServiceFactory();

    // TODO: 17/3/12 by zmyer
    private static class ClientExecutorServiceFactory {
        //执行器引用计数器
        private int executorRefCount = 0;
        //客户端执行器对象
        private ExecutorService clientExecutor = null;

        /**
         * Get Executor on which IPC calls' parameters are sent.
         * If the internal reference counter is zero, this method
         * creates the instance of Executor. If not, this method
         * just returns the reference of clientExecutor.
         *
         * @return An ExecutorService instance
         */
        // TODO: 17/3/12 by zmyer
        synchronized ExecutorService refAndGetInstance() {
            if (executorRefCount == 0) {
                //如果引用计数器为0,则需要重新创建执行器对象
                clientExecutor = Executors.newCachedThreadPool(
                    new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("IPC Parameter Sending Thread #%d")
                        .build());
            }
            //递增执行器引用计数器
            executorRefCount++;
            //返回执行器对象
            return clientExecutor;
        }

        /**
         * Cleanup Executor on which IPC calls' parameters are sent.
         * If reference counter is zero, this method discards the
         * instance of the Executor. If not, this method
         * just decrements the internal reference counter.
         *
         * @return An ExecutorService instance if it exists. Null is returned if not.
         */
        // TODO: 17/3/12 by zmyer
        synchronized ExecutorService unrefAndCleanup() {
            //递减引用计数器
            executorRefCount--;
            assert (executorRefCount >= 0);

            if (executorRefCount == 0) {
                //如果引用计数器为0,则直接关闭客户端执行器对象
                clientExecutor.shutdown();
                try {
                    //等待执行器关闭
                    if (!clientExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
                        clientExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while waiting for clientExecutor" +
                        " to stop");
                    clientExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
                clientExecutor = null;
            }
            //返回客户端执行器
            return clientExecutor;
        }
    }


    /**
     * set the ping interval value in configuration
     *
     * @param conf Configuration
     * @param pingInterval the ping interval
     */
    // TODO: 17/3/12 by zmyer
    static final void setPingInterval(Configuration conf, int pingInterval) {
        //设置ping的时间间隔
        conf.setInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, pingInterval);
    }

    /**
     * Get the ping interval from configuration;
     * If not set in the configuration, return the default value.
     *
     * @param conf Configuration
     * @return the ping interval
     */
    // TODO: 17/3/12 by zmyer
    static final int getPingInterval(Configuration conf) {
        //读取ping时间间隔
        return conf.getInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,
            CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);
    }

    /**
     * The time after which a RPC will timeout.
     * If ping is not enabled (via ipc.client.ping), then the timeout value is the
     * same as the pingInterval.
     * If ping is enabled, then there is no timeout value.
     *
     * @param conf Configuration
     * @return the timeout period in milliseconds. -1 if no timeout value is set
     * @deprecated use {@link #getRpcTimeout(Configuration)} instead
     */
    // TODO: 17/3/12 by zmyer
    @Deprecated
    final public static int getTimeout(Configuration conf) {
        //读取rpc超时时间
        int timeout = getRpcTimeout(conf);
        if (timeout > 0) {
            //返回超时时间
            return timeout;
        }

        if (!conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY,
            CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT)) {
            //读取ping超时时间
            return getPingInterval(conf);
        }
        return -1;
    }

    /**
     * The time after which a RPC will timeout.
     *
     * @param conf Configuration
     * @return the timeout period in milliseconds.
     */
    // TODO: 17/3/12 by zmyer
    public static final int getRpcTimeout(Configuration conf) {
        //读取配置中的超时时间
        int timeout =
            conf.getInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY,
                CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT);
        //返回超时时间
        return (timeout < 0) ? 0 : timeout;
    }

    /**
     * set the connection timeout value in configuration
     *
     * @param conf Configuration
     * @param timeout the socket connect timeout value
     */
    // TODO: 17/3/12 by zmyer
    public static final void setConnectTimeout(Configuration conf, int timeout) {
        //设置超时时间
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY, timeout);
    }

    // TODO: 17/3/12 by zmyer
    @VisibleForTesting
    public static final ExecutorService getClientExecutor() {
        return Client.clientExcecutorFactory.clientExecutor;
    }

    /**
     * Increment this client's reference count
     */
    // TODO: 17/3/12 by zmyer
    synchronized void incCount() {
        refCount++;
    }

    /**
     * Decrement this client's reference count
     */
    // TODO: 17/3/12 by zmyer
    synchronized void decCount() {
        refCount--;
    }

    /**
     * Return if this client has no reference
     *
     * @return true if this client has no reference; false otherwise
     */
    // TODO: 17/3/12 by zmyer
    synchronized boolean isZeroReference() {
        return refCount == 0;
    }

    /** Check the rpc response header. */
    // TODO: 17/3/12 by zmyer
    void checkResponse(RpcResponseHeaderProto header) throws IOException {
        if (header == null) {
            throw new EOFException("Response is null.");
        }
        if (header.hasClientId()) {
            //读取应答头部中的客户端id
            // check client IDs
            final byte[] id = header.getClientId().toByteArray();
            if (!Arrays.equals(id, RpcConstants.DUMMY_CLIENT_ID)) {
                if (!Arrays.equals(id, clientId)) {
                    throw new IOException("Client IDs not matched: local ID="
                        + StringUtils.byteToHexString(clientId) + ", ID in response="
                        + StringUtils.byteToHexString(header.getClientId().toByteArray()));
                }
            }
        }
    }

    // TODO: 17/3/12 by zmyer
    Call createCall(RPC.RpcKind rpcKind, Writable rpcRequest) {
        //创建call对象
        return new Call(rpcKind, rpcRequest);
    }

    /**
     * Class that represents an RPC call
     */
    // TODO: 17/3/12 by zmyer
    static class Call {
        //call id
        final int id;               // call id
        //重试次数
        final int retry;           // retry count
        //rpc请求对象
        final Writable rpcRequest;  // the serialized rpc request
        //rpc应答对象
        Writable rpcResponse;       // null if rpc has error
        //rpc错误对象
        IOException error;          // exception, null if success
        //rpc请求类型
        final RPC.RpcKind rpcKind;      // Rpc EngineKind
        //是否完成标记
        boolean done;               // true when call is done
        //附带的处理器对象
        private final Object externalHandler;

        // TODO: 17/3/12 by zmyer
        private Call(RPC.RpcKind rpcKind, Writable param) {
            //设置rpc类型
            this.rpcKind = rpcKind;
            //设置rpc请求对象
            this.rpcRequest = param;

            //设置call id
            final Integer id = callId.get();
            if (id == null) {
                this.id = nextCallId();
            } else {
                callId.set(null);
                this.id = id;
            }

            //读取重试次数
            final Integer rc = retryCount.get();
            if (rc == null) {
                this.retry = 0;
            } else {
                this.retry = rc;
            }

            //设置附加的处理器对象
            this.externalHandler = EXTERNAL_CALL_HANDLER.get();
        }

        // TODO: 17/3/12 by zmyer
        @Override
        public String toString() {
            return getClass().getSimpleName() + id;
        }

        /**
         * Indicate when the call is complete and the
         * value or error are available.  Notifies by default.
         */
        // TODO: 17/3/12 by zmyer
        protected synchronized void callComplete() {
            //设置完成标记
            this.done = true;
            //发送公告通知
            notify();                                 // notify caller
            if (externalHandler != null) {
                synchronized (externalHandler) {
                    //如果设置了附加的处理器,则执行附加流程
                    externalHandler.notify();
                }
            }
        }

        /**
         * Set the exception when there is an error.
         * Notify the caller the call is done.
         *
         * @param error exception thrown by the call; either local or remote
         */
        // TODO: 17/3/12 by zmyer
        public synchronized void setException(IOException error) {
            //设置错误信息
            this.error = error;
            //设置完成标记
            callComplete();
        }

        /**
         * Set the return value when there is no error.
         * Notify the caller the call is done.
         *
         * @param rpcResponse return value of the rpc call.
         */
        // TODO: 17/3/12 by zmyer
        public synchronized void setRpcResponse(Writable rpcResponse) {
            //设置rpc应答消息
            this.rpcResponse = rpcResponse;
            //设置完成标记
            callComplete();
        }

        // TODO: 17/3/12 by zmyer
        public synchronized Writable getRpcResponse() {
            return rpcResponse;
        }
    }

    /**
     * Thread that reads responses and notifies callers.  Each connection owns a
     * socket connected to a remote address.  Calls are multiplexed through this
     * socket: responses may be delivered out of order.
     */
    // TODO: 17/3/12 by zmyer
    private class Connection extends Thread {
        //服务器的地址信息
        private InetSocketAddress server;             // server ip:port
        //连接对象id
        private final ConnectionId remoteId;                // connection id
        //认证函数对象
        private AuthMethod authMethod; // authentication method
        //认证协议对象
        private AuthProtocol authProtocol;
        //服务类对象
        private int serviceClass;
        //sasl认证客户端对象
        private SaslRpcClient saslRpcClient;

        //连接套接字对象
        private Socket socket = null;                 // connected socket
        //rpc流式对象
        private IpcStreams ipcStreams;
        //最大的应答长度
        private final int maxResponseLength;
        //rpc超时时间
        private final int rpcTimeout;
        //连接最大的空闲时间
        private int maxIdleTime; //connections will be culled if it was idle for
        //maxIdleTime msecs
        //连接重试策略对象
        private final RetryPolicy connectionRetryPolicy;
        //sasl最大重试次数
        private final int maxRetriesOnSasl;
        //套接字连接超时最大重试次数
        private int maxRetriesOnSocketTimeouts;
        //
        private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
        //低延时开关
        private final boolean tcpLowLatency; // if T then use low-delay QoS
        //ping开关
        private final boolean doPing; //do we need to send ping message
        //ping间隔时间
        private final int pingInterval; // how often sends ping to the server
        //超时时间
        private final int soTimeout; // used by ipc ping and rpc timeout
        //ping消息
        private byte[] pingRequest; // ping message

        // currently active calls
        //call列表
        private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();
        //最近一次IO激活时间
        private AtomicLong lastActivity = new AtomicLong();// last I/O activity time
        //是否关闭连接开关
        private AtomicBoolean shouldCloseConnection = new AtomicBoolean();  // indicate if the connection is closed
        //关闭异常对象
        private IOException closeException; // close reason

        //发送rpc请求锁对象
        private final Object sendRpcRequestLock = new Object();

        // TODO: 17/3/12 by zmyer
        public Connection(ConnectionId remoteId, int serviceClass) throws IOException {
            this.remoteId = remoteId;
            this.server = remoteId.getAddress();
            if (server.isUnresolved()) {
                throw NetUtils.wrapException(server.getHostName(),
                    server.getPort(),
                    null,
                    0,
                    new UnknownHostException());
            }
            this.maxResponseLength = remoteId.conf.getInt(
                CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH,
                CommonConfigurationKeys.IPC_MAXIMUM_RESPONSE_LENGTH_DEFAULT);
            this.rpcTimeout = remoteId.getRpcTimeout();
            this.maxIdleTime = remoteId.getMaxIdleTime();
            this.connectionRetryPolicy = remoteId.connectionRetryPolicy;
            this.maxRetriesOnSasl = remoteId.getMaxRetriesOnSasl();
            this.maxRetriesOnSocketTimeouts = remoteId.getMaxRetriesOnSocketTimeouts();
            this.tcpNoDelay = remoteId.getTcpNoDelay();
            this.tcpLowLatency = remoteId.getTcpLowLatency();
            this.doPing = remoteId.getDoPing();
            if (doPing) {
                // construct a RPC header with the callId as the ping callId
                ResponseBuffer buf = new ResponseBuffer();
                RpcRequestHeaderProto pingHeader = ProtoUtil
                    .makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
                        OperationProto.RPC_FINAL_PACKET, PING_CALL_ID,
                        RpcConstants.INVALID_RETRY_COUNT, clientId);
                pingHeader.writeDelimitedTo(buf);
                pingRequest = buf.toByteArray();
            }
            this.pingInterval = remoteId.getPingInterval();
            if (rpcTimeout > 0) {
                // effective rpc timeout is rounded up to multiple of pingInterval
                // if pingInterval < rpcTimeout.
                this.soTimeout = (doPing && pingInterval < rpcTimeout) ?
                    pingInterval : rpcTimeout;
            } else {
                this.soTimeout = pingInterval;
            }
            this.serviceClass = serviceClass;
            if (LOG.isDebugEnabled()) {
                LOG.debug("The ping interval is " + this.pingInterval + " ms.");
            }

            UserGroupInformation ticket = remoteId.getTicket();
            // try SASL if security is enabled or if the ugi contains tokens.
            // this causes a SIMPLE client with tokens to attempt SASL
            boolean trySasl = UserGroupInformation.isSecurityEnabled() ||
                (ticket != null && !ticket.getTokens().isEmpty());
            this.authProtocol = trySasl ? AuthProtocol.SASL : AuthProtocol.NONE;

            this.setName("IPC Client (" + socketFactory.hashCode() + ") connection to " +
                server.toString() +
                " from " + ((ticket == null) ? "an unknown user" : ticket.getUserName()));
            this.setDaemon(true);
        }

        /** Update lastActivity with the current time. */
        // TODO: 17/3/12 by zmyer
        private void touch() {
            lastActivity.set(Time.now());
        }

        /**
         * Add a call to this connection's call queue and notify
         * a listener; synchronized.
         * Returns false if called during shutdown.
         *
         * @param call to add
         * @return true if the call was added.
         */
        // TODO: 17/3/12 by zmyer
        private synchronized boolean addCall(Call call) {
            if (shouldCloseConnection.get())
                return false;
            //向call列表中插入call对象
            calls.put(call.id, call);
            //发送call插入公告
            notify();
            //返回
            return true;
        }

        /**
         * This class sends a ping to the remote side when timeout on
         * reading. If no failure is detected, it retries until at least
         * a byte is read.
         */
        // TODO: 17/3/13 by zmyer
        private class PingInputStream extends FilterInputStream {
            /* constructor */
            // TODO: 17/3/13 by zmyer
            protected PingInputStream(InputStream in) {
                super(in);
            }

            /* Process timeout exception
             * if the connection is not going to be closed or
             * the RPC is not timed out yet, send a ping.
             */
            // TODO: 17/3/13 by zmyer
            private void handleTimeout(SocketTimeoutException e, int waiting)
                throws IOException {
                if (shouldCloseConnection.get() || !running.get() ||
                    (0 < rpcTimeout && rpcTimeout <= waiting)) {
                    throw e;
                } else {
                    //发送ping消息
                    sendPing();
                }
            }

            /**
             * Read a byte from the stream.
             * Send a ping if timeout on read. Retries if no failure is detected
             * until a byte is read.
             *
             * @throws IOException for any IO problem other than socket timeout
             */
            // TODO: 17/3/13 by zmyer
            @Override
            public int read() throws IOException {
                int waiting = 0;
                do {
                    try {
                        //读取输入流对象
                        return super.read();
                    } catch (SocketTimeoutException e) {
                        //更新超时时间
                        waiting += soTimeout;
                        //超时处理
                        handleTimeout(e, waiting);
                    }
                }
                while (true);
            }

            /**
             * Read bytes into a buffer starting from offset <code>off</code>
             * Send a ping if timeout on read. Retries if no failure is detected
             * until a byte is read.
             *
             * @return the total number of bytes read; -1 if the connection is closed.
             */
            // TODO: 17/3/13 by zmyer
            @Override
            public int read(byte[] buf, int off, int len) throws IOException {
                int waiting = 0;
                do {
                    try {
                        //读取数据
                        return super.read(buf, off, len);
                    } catch (SocketTimeoutException e) {
                        //更新超时时间
                        waiting += soTimeout;
                        //超时处理
                        handleTimeout(e, waiting);
                    }
                }
                while (true);
            }
        }

        // TODO: 17/3/13 by zmyer
        private synchronized void disposeSasl() {
            if (saslRpcClient != null) {
                try {
                    saslRpcClient.dispose();
                    saslRpcClient = null;
                } catch (IOException ignored) {
                }
            }
        }

        // TODO: 17/3/13 by zmyer
        private synchronized boolean shouldAuthenticateOverKrb() throws IOException {
            UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            UserGroupInformation realUser = currentUser.getRealUser();
            return authMethod == AuthMethod.KERBEROS && loginUser != null &&
                // Make sure user logged in using Kerberos either keytab or TGT
                loginUser.hasKerberosCredentials() &&
                // relogin only in case it is the login user (e.g. JT)
                // or superuser (like oozie).
                (loginUser.equals(currentUser) || loginUser.equals(realUser));
        }

        // TODO: 17/3/13 by zmyer
        private synchronized AuthMethod setupSaslConnection(IpcStreams streams)
            throws IOException {
            // Do not use Client.conf here! We must use ConnectionId.conf, since the
            // Client object is cached and shared between all RPC clients, even those
            // for separate services.
            //创建sasl客户端对象
            saslRpcClient = new SaslRpcClient(remoteId.getTicket(),
                remoteId.getProtocol(), remoteId.getAddress(), remoteId.conf);
            //开始连接服务器
            return saslRpcClient.saslConnect(streams);
        }

        /**
         * Update the server address if the address corresponding to the host
         * name has changed.
         *
         * @return true if an addr change was detected.
         * @throws IOException when the hostname cannot be resolved.
         */
        // TODO: 17/3/13 by zmyer
        private synchronized boolean updateAddress() throws IOException {
            // Do a fresh lookup with the old host name.
            //创建套接字对象
            InetSocketAddress currentAddr = NetUtils.createSocketAddrForHost(
                server.getHostName(), server.getPort());

            if (!server.equals(currentAddr)) {
                LOG.warn("Address change detected. Old: " + server.toString() +
                    " New: " + currentAddr.toString());
                //更新服务器的套接字对象
                server = currentAddr;
                return true;
            }
            return false;
        }

        // TODO: 17/3/14 by zmyer
        private synchronized void setupConnection() throws IOException {
            short ioFailures = 0;
            short timeoutFailures = 0;
            while (true) {
                try {
                    //创建套接字对象
                    this.socket = socketFactory.createSocket();
                    //设置套接字非延时模式
                    this.socket.setTcpNoDelay(tcpNoDelay);
                    //保持连接
                    this.socket.setKeepAlive(true);

                    //低延迟优化
                    if (tcpLowLatency) {
            /*
             * This allows intermediate switches to shape IPC traffic
             * differently from Shuffle/HDFS DataStreamer traffic.
             *
             * IPTOS_RELIABILITY (0x04) | IPTOS_LOWDELAY (0x10)
             *
             * Prefer to optimize connect() speed & response latency over net
             * throughput.
             */
                        this.socket.setTrafficClass(0x04 | 0x10);
                        this.socket.setPerformancePreferences(1, 2, 0);
                    }

          /*
           * Bind the socket to the host specified in the principal name of the
           * client, to ensure Server matching address of the client connection
           * to host name in principal passed.
           */
                    UserGroupInformation ticket = remoteId.getTicket();
                    if (ticket != null && ticket.hasKerberosCredentials()) {
                        KerberosInfo krbInfo =
                            remoteId.getProtocol().getAnnotation(KerberosInfo.class);
                        if (krbInfo != null && krbInfo.clientPrincipal() != null) {
                            String host =
                                SecurityUtil.getHostFromPrincipal(remoteId.getTicket().getUserName());

                            // If host name is a valid local address then bind socket to it
                            InetAddress localAddr = NetUtils.getLocalInetAddress(host);
                            if (localAddr != null) {
                                //设置地址复用标记
                                this.socket.setReuseAddress(true);
                                //绑定本地地址
                                this.socket.bind(new InetSocketAddress(localAddr, 0));
                            }
                        }
                    }

                    //开始连接服务器
                    NetUtils.connect(this.socket, server, connectionTimeout);
                    //设置套接字超时时间
                    this.socket.setSoTimeout(soTimeout);
                    return;
                } catch (ConnectTimeoutException toe) {
          /* Check for an address change and update the local reference.
           * Reset the failure counter if the address was changed
           */
                    if (updateAddress()) {
                        timeoutFailures = ioFailures = 0;
                    }
                    //处理连接超时
                    handleConnectionTimeout(timeoutFailures++, maxRetriesOnSocketTimeouts, toe);
                } catch (IOException ie) {
                    if (updateAddress()) {
                        timeoutFailures = ioFailures = 0;
                    }
                    //处理连接失败
                    handleConnectionFailure(ioFailures++, ie);
                }
            }
        }

        /**
         * If multiple clients with the same principal try to connect to the same
         * server at the same time, the server assumes a replay attack is in
         * progress. This is a feature of kerberos. In order to work around this,
         * what is done is that the client backs off randomly and tries to initiate
         * the connection again. The other problem is to do with ticket expiry. To
         * handle that, a relogin is attempted.
         */
        // TODO: 17/3/14 by zmyer
        private synchronized void handleSaslConnectionFailure(
            final int currRetries, final int maxRetries, final Exception ex,
            final Random rand, final UserGroupInformation ugi) throws IOException,
            InterruptedException {
            ugi.doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws IOException, InterruptedException {
                    final short MAX_BACKOFF = 5000;
                    closeConnection();
                    disposeSasl();
                    if (shouldAuthenticateOverKrb()) {
                        if (currRetries < maxRetries) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Exception encountered while connecting to "
                                    + "the server : " + ex);
                            }
                            // try re-login
                            if (UserGroupInformation.isLoginKeytabBased()) {
                                UserGroupInformation.getLoginUser().reloginFromKeytab();
                            } else if (UserGroupInformation.isLoginTicketBased()) {
                                UserGroupInformation.getLoginUser().reloginFromTicketCache();
                            }
                            // have granularity of milliseconds
                            //we are sleeping with the Connection lock held but since this
                            //connection instance is being used for connecting to the server
                            //in question, it is okay
                            Thread.sleep((rand.nextInt(MAX_BACKOFF) + 1));
                            return null;
                        } else {
                            String msg = "Couldn't setup connection for "
                                + UserGroupInformation.getLoginUser().getUserName() + " to "
                                + remoteId;
                            LOG.warn(msg, ex);
                            throw (IOException) new IOException(msg).initCause(ex);
                        }
                    } else {
                        LOG.warn("Exception encountered while connecting to "
                            + "the server : " + ex);
                    }
                    if (ex instanceof RemoteException)
                        throw (RemoteException) ex;
                    throw new IOException(ex);
                }
            });
        }

        /**
         * Connect to the server and set up the I/O streams. It then sends
         * a header to the server and starts
         * the connection thread that waits for responses.
         */
        // TODO: 17/3/14 by zmyer
        private synchronized void setupIOstreams(
            AtomicBoolean fallbackToSimpleAuth) {
            //如果套接字对象不为空或者当前的需要关闭连接,则直接退出
            if (socket != null || shouldCloseConnection.get()) {
                return;
            }
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Connecting to " + server);
                }
                Span span = Tracer.getCurrentSpan();
                if (span != null) {
                    span.addTimelineAnnotation("IPC client connecting to " + server);
                }
                //重试次数
                short numRetries = 0;
                Random rand = null;
                while (true) {
                    //首先建立服务器端连接
                    setupConnection();
                    //根据创建的套接字,创建stream对象
                    ipcStreams = new IpcStreams(socket, maxResponseLength);
                    //向stream对象中写入连接头部对象
                    writeConnectionHeader(ipcStreams);
                    if (authProtocol == AuthProtocol.SASL) {
                        UserGroupInformation ticket = remoteId.getTicket();
                        if (ticket.getRealUser() != null) {
                            ticket = ticket.getRealUser();
                        }
                        try {
                            authMethod = ticket
                                .doAs(new PrivilegedExceptionAction<AuthMethod>() {
                                    @Override
                                    public AuthMethod run()
                                        throws IOException, InterruptedException {
                                        //建立sasl连接
                                        return setupSaslConnection(ipcStreams);
                                    }
                                });
                        } catch (IOException ex) {
                            if (saslRpcClient == null) {
                                // whatever happened -it can't be handled, so rethrow
                                throw ex;
                            }
                            // otherwise, assume a connection problem
                            authMethod = saslRpcClient.getAuthMethod();
                            if (rand == null) {
                                rand = new Random();
                            }
                            handleSaslConnectionFailure(numRetries++, maxRetriesOnSasl, ex,
                                rand, ticket);
                            continue;
                        }
                        if (authMethod != AuthMethod.SIMPLE) {
                            // Sasl connect is successful. Let's set up Sasl i/o streams.
                            ipcStreams.setSaslClient(saslRpcClient);
                            // for testing
                            remoteId.saslQop =
                                (String) saslRpcClient.getNegotiatedProperty(Sasl.QOP);
                            LOG.debug("Negotiated QOP is :" + remoteId.saslQop);
                            if (fallbackToSimpleAuth != null) {
                                fallbackToSimpleAuth.set(false);
                            }
                        } else if (UserGroupInformation.isSecurityEnabled()) {
                            if (!fallbackAllowed) {
                                throw new IOException("Server asks us to fall back to SIMPLE " +
                                    "auth, but this client is configured to only allow secure " +
                                    "connections.");
                            }
                            if (fallbackToSimpleAuth != null) {
                                fallbackToSimpleAuth.set(true);
                            }
                        }
                    }

                    if (doPing) {
                        //如果发送ping消息,则为stream对象设置ping输入流对象
                        ipcStreams.setInputStream(new PingInputStream(ipcStreams.in));
                    }
                    //开始写入正文对象
                    writeConnectionContext(remoteId, authMethod);

                    // update last activity time
                    touch();

                    span = Tracer.getCurrentSpan();
                    if (span != null) {
                        span.addTimelineAnnotation("IPC client connected to " + server);
                    }

                    // start the receiver thread after the socket connection has been set
                    // up
                    //启动接收线程
                    start();
                    return;
                }
            } catch (Throwable t) {
                if (t instanceof IOException) {
                    markClosed((IOException) t);
                } else {
                    markClosed(new IOException("Couldn't set up IO streams: " + t, t));
                }
                close();
            }
        }

        // TODO: 17/3/14 by zmyer
        private void closeConnection() {
            if (socket == null) {
                return;
            }
            // close the current connection
            try {
                //关闭套接字
                socket.close();
            } catch (IOException e) {
                LOG.warn("Not able to close a socket", e);
            }
            // set socket to null so that the next call to setupIOstreams
            // can start the process of connect all over again.
            socket = null;
        }

        /* Handle connection failures due to timeout on connect
         *
         * If the current number of retries is equal to the max number of retries,
         * stop retrying and throw the exception; Otherwise backoff 1 second and
         * try connecting again.
         *
         * This Method is only called from inside setupIOstreams(), which is
         * synchronized. Hence the sleep is synchronized; the locks will be retained.
         *
         * @param curRetries current number of retries
         * @param maxRetries max number of retries allowed
         * @param ioe failure reason
         * @throws IOException if max number of retries is reached
         */
        // TODO: 17/3/14 by zmyer
        private void handleConnectionTimeout(
            int curRetries, int maxRetries, IOException ioe) throws IOException {
                //首先关闭连接
            closeConnection();

            // throw the exception if the maximum number of retries is reached
            //如果当前的重试次数超过了最大次数,则抛出异常
            if (curRetries >= maxRetries) {
                throw ioe;
            }
            LOG.info("Retrying connect to server: " + server + ". Already tried "
                + curRetries + " time(s); maxRetries=" + maxRetries);
        }

        // TODO: 17/3/14 by zmyer
        private void handleConnectionFailure(int curRetries, IOException ioe) throws IOException {
            //关闭连接
            closeConnection();

            final RetryAction action;
            try {
                //读取连接重试对象
                action = connectionRetryPolicy.shouldRetry(ioe, curRetries, 0, true);
            } catch (Exception e) {
                throw e instanceof IOException ? (IOException) e : new IOException(e);
            }
            if (action.action == RetryAction.RetryDecision.FAIL) {
                if (action.reason != null) {
                    LOG.warn("Failed to connect to server: " + server + ": "
                        + action.reason, ioe);
                }
                throw ioe;
            }

            // Throw the exception if the thread is interrupted
            if (Thread.currentThread().isInterrupted()) {
                LOG.warn("Interrupted while trying for connection");
                throw ioe;
            }

            try {
                //等待一段时间
                Thread.sleep(action.delayMillis);
            } catch (InterruptedException e) {
                throw (IOException) new InterruptedIOException("Interrupted: action="
                    + action + ", retry policy=" + connectionRetryPolicy).initCause(e);
            }
            LOG.info("Retrying connect to server: " + server + ". Already tried "
                + curRetries + " time(s); retry policy is " + connectionRetryPolicy);
        }

        /**
         * Write the connection header - this is sent when connection is established
         * +----------------------------------+
         * |  "hrpc" 4 bytes                  |
         * +----------------------------------+
         * |  Version (1 byte)                |
         * +----------------------------------+
         * |  Service Class (1 byte)          |
         * +----------------------------------+
         * |  AuthProtocol (1 byte)           |
         * +----------------------------------+
         */
        // TODO: 17/3/14 by zmyer
        private void writeConnectionHeader(IpcStreams streams)
            throws IOException {
            // Write out the header, version and authentication method.
            // The output stream is buffered but we must not flush it yet.  The
            // connection setup protocol requires the client to send multiple
            // messages before reading a response.
            //
            //   insecure: send header+context+call, read
            //   secure  : send header+negotiate, read, (sasl), context+call, read
            //
            // The client must flush only when it's prepared to read.  Otherwise
            // "broken pipe" exceptions occur if the server closes the connection
            // before all messages are sent.
            //输出流对象
            final DataOutputStream out = streams.out;
            synchronized (out) {
                //首先写入头部
                out.write(RpcConstants.HEADER.array());
                //写入当前的版本号
                out.write(RpcConstants.CURRENT_VERSION);
                //写入服务类对象
                out.write(serviceClass);
                //写入callid
                out.write(authProtocol.callId);
            }
        }

        /* Write the connection context header for each connection
         * Out is not synchronized because only the first thread does this.
         */
        // TODO: 17/3/14 by zmyer
        private void writeConnectionContext(ConnectionId remoteId,
            AuthMethod authMethod)
            throws IOException {
            // Write out the ConnectionHeader
            //创建连接上下文对象
            IpcConnectionContextProto message = ProtoUtil.makeIpcConnectionContext(
                RPC.getProtocolName(remoteId.getProtocol()),
                remoteId.getTicket(),
                authMethod);
            //创建rpc请求头部对象
            RpcRequestHeaderProto connectionContextHeader = ProtoUtil
                .makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
                    OperationProto.RPC_FINAL_PACKET, CONNECTION_CONTEXT_CALL_ID,
                    RpcConstants.INVALID_RETRY_COUNT, clientId);
            // do not flush.  the context and first ipc call request must be sent
            // together to avoid possibility of broken pipes upon authz failure.
            // see writeConnectionHeader
            //创建应答缓冲区
            final ResponseBuffer buf = new ResponseBuffer();
            //设置连接上下文对象中的接受缓冲区
            connectionContextHeader.writeDelimitedTo(buf);
            //开始将消息写入到缓冲区中
            message.writeDelimitedTo(buf);
            synchronized (ipcStreams.out) {
                //开始发送应答消息
                ipcStreams.sendRequest(buf.toByteArray());
            }
        }

        /* wait till someone signals us to start reading RPC response or
         * it is idle too long, it is marked as to be closed,
         * or the client is marked as not running.
         *
         * Return true if it is time to read a response; false otherwise.
         */
        // TODO: 17/3/14 by zmyer
        private synchronized boolean waitForWork() {
            if (calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
                //如果call列表为空,并且没连接没有关闭
                //计算超时时间
                long timeout = maxIdleTime - (Time.now() - lastActivity.get());
                if (timeout > 0) {
                    try {
                        //等待超时时间
                        wait(timeout);
                    } catch (InterruptedException e) {
                    }
                }
            }

            //如果call列表不为空并且俩链接没有关闭,则返回
            if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
                return true;
            } else if (shouldCloseConnection.get()) {
                return false;
            } else if (calls.isEmpty()) { // idle connection closed or stopped
                //关闭连接
                markClosed(null);
                return false;
            } else { // get stopped but there are still pending requests
                markClosed((IOException) new IOException().initCause(
                    new InterruptedException()));
                return false;
            }
        }

        // TODO: 17/3/14 by zmyer
        public InetSocketAddress getRemoteAddress() {
            return server;
        }

        /* Send a ping to the server if the time elapsed
         * since last I/O activity is equal to or greater than the ping interval
         */
        // TODO: 17/3/14 by zmyer
        private synchronized void sendPing() throws IOException {
            long curTime = Time.now();
            //如果超过了ping时间间隔,则直接发送ping消息
            if (curTime - lastActivity.get() >= pingInterval) {
                lastActivity.set(curTime);
                synchronized (ipcStreams.out) {
                    //发送ping请求对象
                    ipcStreams.sendRequest(pingRequest);
                    //刷新输入流对象
                    ipcStreams.flush();
                }
            }
        }

        // TODO: 17/3/14 by zmyer
        @Override
        public void run() {
            if (LOG.isDebugEnabled())
                LOG.debug(getName() + ": starting, having connections " + connections.size());

            try {
                while (waitForWork()) {//wait here for work - read or close connection
                    //等待work到来,接受应答到来
                    receiveRpcResponse();
                }
            } catch (Throwable t) {
                // This truly is unexpected, since we catch IOException in receiveResponse
                // -- this is only to be really sure that we don't leave a client hanging
                // forever.
                LOG.warn("Unexpected error reading responses on connection " + this, t);
                markClosed(new IOException("Error reading responses", t));
            }

            //关闭连接
            close();

            if (LOG.isDebugEnabled())
                LOG.debug(getName() + ": stopped, remaining connections " + connections.size());
        }

        /**
         * Initiates a rpc call by sending the rpc request to the remote server.
         * Note: this is not called from the Connection thread, but by other
         * threads.
         *
         * @param call - the rpc request
         */
        // TODO: 17/3/14 by zmyer
        public void sendRpcRequest(final Call call)
            throws InterruptedException, IOException {
            if (shouldCloseConnection.get()) {
                //连接关闭了,则直接退出
                return;
            }

            // Serialize the call to be sent. This is done from the actual
            // caller thread, rather than the sendParamsExecutor thread,

            // so that if the serialization throws an error, it is reported
            // properly. This also parallelizes the serialization.
            //
            // Format of a call on the wire:
            // 0) Length of rest below (1 + 2)
            // 1) RpcRequestHeader  - is serialized Delimited hence contains length
            // 2) RpcRequest
            //
            // Items '1' and '2' are prepared here.
            //构造rpc请求头部对象
            RpcRequestHeaderProto header = ProtoUtil.makeRpcRequestHeader(
                call.rpcKind, OperationProto.RPC_FINAL_PACKET, call.id, call.retry,
                clientId);

            //应答缓冲区
            final ResponseBuffer buf = new ResponseBuffer();
            //将头部内容写入到缓冲区中
            header.writeDelimitedTo(buf);
            //将rpc请求写入到缓冲区中
            RpcWritable.wrap(call.rpcRequest).writeTo(buf);

            synchronized (sendRpcRequestLock) {
                //开始向send线程投递发送任务
                Future<?> senderFuture = sendParamsExecutor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            synchronized (ipcStreams.out) {
                                if (shouldCloseConnection.get()) {
                                    return;
                                }
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug(getName() + " sending #" + call.id);
                                }
                                // RpcRequestHeader + RpcRequest
                                //开始发送rpc请求对象
                                ipcStreams.sendRequest(buf.toByteArray());
                                //刷新输出流对象
                                ipcStreams.flush();
                            }
                        } catch (IOException e) {
                            // exception at this point would leave the connection in an
                            // unrecoverable state (eg half a call left on the wire).
                            // So, close the connection, killing any outstanding calls
                            markClosed(e);
                        } finally {
                            //the buffer is just an in-memory buffer, but it is still polite to
                            // close early
                            IOUtils.closeStream(buf);
                        }
                    }
                });

                try {
                    //等待应答
                    senderFuture.get();
                } catch (ExecutionException e) {
                    //获取异常对象
                    Throwable cause = e.getCause();

                    // cause should only be a RuntimeException as the Runnable above
                    // catches IOException
                    //直接抛出异常
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    } else {
                        throw new RuntimeException("unexpected checked exception", cause);
                    }
                }
            }
        }

        /* Receive a response.
         * Because only one receiver, so no synchronization on in.
         */
        // TODO: 17/3/14 by zmyer
        private void receiveRpcResponse() {
            if (shouldCloseConnection.get()) {
                return;
            }
            touch();

            try {
                //从流对象中读取应答消息
                ByteBuffer bb = ipcStreams.readResponse();
                //构造rpc写入缓冲区对象
                RpcWritable.Buffer packet = RpcWritable.Buffer.wrap(bb);
                //读取rpc应答头部对象
                RpcResponseHeaderProto header =
                    packet.getValue(RpcResponseHeaderProto.getDefaultInstance());
                //检查应答头部信息
                checkResponse(header);

                //从头部中读取call id
                int callId = header.getCallId();
                if (LOG.isDebugEnabled())
                    LOG.debug(getName() + " got value #" + callId);

                //读取rpc状态信息
                RpcStatusProto status = header.getStatus();
                if (status == RpcStatusProto.SUCCESS) {
                    //如果是成功的应答,则根据提供的类对象实例化对象
                    Writable value = packet.newInstance(valueClass, conf);
                    //从call列表中读取对应的call,并删除
                    final Call call = calls.remove(callId);
                    //为call对象设置应答消息
                    call.setRpcResponse(value);
                }
                // verify that packet length was correct
                if (packet.remaining() > 0) {
                    throw new RpcClientException("RPC response length mismatch");
                }
                if (status != RpcStatusProto.SUCCESS) { // Rpc Request failed
                    //异常类名称
                    final String exceptionClassName = header.hasExceptionClassName() ?
                        header.getExceptionClassName() :
                        "ServerDidNotSetExceptionClassName";
                    //读取错误消息
                    final String errorMsg = header.hasErrorMsg() ?
                        header.getErrorMsg() : "ServerDidNotSetErrorMsg";
                    //读取rpc错误码对象
                    final RpcErrorCodeProto erCode =
                        (header.hasErrorDetail() ? header.getErrorDetail() : null);
                    if (erCode == null) {
                        LOG.warn("Detailed error code not set by server on rpc error");
                    }
                    //构造远程rpc调用异常对象
                    RemoteException re = new RemoteException(exceptionClassName, errorMsg, erCode);
                    if (status == RpcStatusProto.ERROR) {
                        //从call列表中读取call对象
                        final Call call = calls.remove(callId);
                        //为call对象设置异常对象
                        call.setException(re);
                    } else if (status == RpcStatusProto.FATAL) {
                        // Close the connection
                        //关闭连接
                        markClosed(re);
                    }
                }
            } catch (IOException e) {
                //关闭连接
                markClosed(e);
            }
        }

        // TODO: 17/3/14 by zmyer
        private synchronized void markClosed(IOException e) {
            if (shouldCloseConnection.compareAndSet(false, true)) {
                //设置关闭连接标记,并设置关闭异常
                closeException = e;
                //发送公告
                notifyAll();
            }
        }

        /** Close the connection. */
        // TODO: 17/3/14 by zmyer
        private synchronized void close() {
            if (!shouldCloseConnection.get()) {
                LOG.error("The connection is not in the closed state");
                return;
            }

            // We have marked this connection as closed. Other thread could have
            // already known it and replace this closedConnection with a new one.
            // We should only remove this closedConnection.
            //从连接列表中删除指定的连接对象
            connections.remove(remoteId, this);

            // close the streams and therefore the socket
            //关闭连接对象的流对象
            IOUtils.closeStream(ipcStreams);
            disposeSasl();

            // clean up all calls
            if (closeException == null) {
                if (!calls.isEmpty()) {
                    LOG.warn(
                        "A connection is closed for no cause and calls are not empty");

                    // clean up calls anyway
                    closeException = new IOException("Unexpected closed connection");
                    //清理call列表
                    cleanupCalls();
                }
            } else {
                // log the info
                if (LOG.isDebugEnabled()) {
                    LOG.debug("closing ipc connection to " + server + ": " +
                        closeException.getMessage(), closeException);
                }

                // cleanup calls
                //清空call列表
                cleanupCalls();
            }
            //关闭连接
            closeConnection();
            if (LOG.isDebugEnabled())
                LOG.debug(getName() + ": closed");
        }

        /* Cleanup all calls and mark them as done */
        // TODO: 17/3/14 by zmyer
        private void cleanupCalls() {
            Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator();
            while (itor.hasNext()) {
                //读取每个call对象
                Call c = itor.next().getValue();
                //从call列表中删除该call对象
                itor.remove();
                //设置关闭异常对象
                c.setException(closeException); // local exception
            }
        }
    }

    /**
     * Construct an IPC client whose values are of the given {@link Writable}
     * class.
     */
    // TODO: 17/3/14 by zmyer
    public Client(Class<? extends Writable> valueClass, Configuration conf,
        SocketFactory factory) {
        //值类对象
        this.valueClass = valueClass;
        //配置对象
        this.conf = conf;
        //socket工厂对象
        this.socketFactory = factory;
        //连接超时时间
        this.connectionTimeout = conf.getInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
            CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT);
        //是否开启回馈机制
        this.fallbackAllowed = conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
            CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
        //读取客户端id
        this.clientId = ClientId.getClientId();
        //创建发送线程
        this.sendParamsExecutor = clientExcecutorFactory.refAndGetInstance();
        //读取最大的异步化调用次数
        this.maxAsyncCalls = conf.getInt(
            CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_KEY,
            CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_DEFAULT);
    }

    /**
     * Construct an IPC client with the default SocketFactory
     *
     * @param valueClass
     * @param conf
     */
    // TODO: 17/3/14 by zmyer
    public Client(Class<? extends Writable> valueClass, Configuration conf) {
        this(valueClass, conf, NetUtils.getDefaultSocketFactory(conf));
    }

    /**
     * Return the socket factory of this client
     *
     * @return this client's socket factory
     */
    // TODO: 17/3/14 by zmyer
    SocketFactory getSocketFactory() {
        return socketFactory;
    }

    /**
     * Stop all threads related to this client.  No further calls may be made
     * using this client.
     */
    // TODO: 17/3/14 by zmyer
    public void stop() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Stopping client");
        }
        //如果运行标记为false,这说明之前就已经关闭,直接退出
        if (!running.compareAndSet(true, false)) {
            return;
        }

        // wake up all connections
        for (Connection conn : connections.values()) {
            //依次关闭所有的连接对象
            conn.interrupt();
        }

        // wait until all connections are closed
        while (!connections.isEmpty()) {
            try {
                //如果链接对象不为空,则等待片刻
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        //清理客户端发送线程
        clientExcecutorFactory.unrefAndCleanup();
    }

    /**
     * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
     * <code>remoteId</code>, returning the rpc respond.
     *
     * @param rpcKind
     * @param rpcRequest -  contains serialized method and method parameters
     * @param remoteId - the target rpc server
     * @param fallbackToSimpleAuth - set to true or false during this method to indicate if a secure client falls back
     * to simple auth
     */
    // TODO: 17/3/14 by zmyer
    public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
        ConnectionId remoteId, AtomicBoolean fallbackToSimpleAuth)
        throws IOException {
        return call(rpcKind, rpcRequest, remoteId, RPC.RPC_SERVICE_CLASS_DEFAULT,
            fallbackToSimpleAuth);
    }

    // TODO: 17/3/14 by zmyer
    private void checkAsyncCall() throws IOException {
        //在异步模式下
        if (isAsynchronousMode()) {
            if (asyncCallCounter.incrementAndGet() > maxAsyncCalls) {
                //如果异步调用的次数超过了最大的限制,则直接抛出异常
                asyncCallCounter.decrementAndGet();
                String errMsg = String.format(
                    "Exceeded limit of max asynchronous calls: %d, " +
                        "please configure %s to adjust it.",
                    maxAsyncCalls,
                    CommonConfigurationKeys.IPC_CLIENT_ASYNC_CALLS_MAX_KEY);
                throw new AsyncCallLimitExceededException(errMsg);
            }
        }
    }

    /**
     * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
     * <code>remoteId</code>, returning the rpc response.
     *
     * @param rpcKind
     * @param rpcRequest -  contains serialized method and method parameters
     * @param remoteId - the target rpc server
     * @param serviceClass - service class for RPC
     * @param fallbackToSimpleAuth - set to true or false during this method to indicate if a secure client falls back
     * to simple auth
     */
    // TODO: 17/3/14 by zmyer
    Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
        ConnectionId remoteId, int serviceClass,
        AtomicBoolean fallbackToSimpleAuth) throws IOException {
        //创建call对象
        final Call call = createCall(rpcKind, rpcRequest);
        //根据提供的call对象,创建连接对象
        final Connection connection = getConnection(remoteId, call, serviceClass,
            fallbackToSimpleAuth);

        try {
            //检查call对象合理性
            checkAsyncCall();
            try {
                //开始通过连接对象发送call请求
                connection.sendRpcRequest(call);                 // send the rpc request
            } catch (RejectedExecutionException e) {
                throw new IOException("connection has been closed", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("interrupted waiting to send rpc request to server", e);
                throw new IOException(e);
            }
        } catch (Exception e) {
            if (isAsynchronousMode()) {
                releaseAsyncCall();
            }
            throw e;
        }

        if (isAsynchronousMode()) {
            //异步获取应答请求
            final AsyncGet<Writable, IOException> asyncGet
                = new AsyncGet<Writable, IOException>() {
                @Override
                public Writable get(long timeout, TimeUnit unit)
                    throws IOException, TimeoutException {
                    boolean done = true;
                    try {
                        //读取call请求的应答消息
                        final Writable w = getRpcResponse(call, connection, timeout, unit);
                        if (w == null) {
                            done = false;
                            throw new TimeoutException(call + " timed out "
                                + timeout + " " + unit);
                        }
                        return w;
                    } finally {
                        if (done) {
                            releaseAsyncCall();
                        }
                    }
                }

                @Override
                public boolean isDone() {
                    synchronized (call) {
                        return call.done;
                    }
                }
            };

            //设置异步请求结果
            ASYNC_RPC_RESPONSE.set(asyncGet);
            return null;
        } else {
            //如果是同步模式,直接返回应答结果
            return getRpcResponse(call, connection, -1, null);
        }
    }

    /**
     * Check if RPC is in asynchronous mode or not.
     */
    // TODO: 17/3/14 by zmyer
    @Unstable
    public static boolean isAsynchronousMode() {
        return asynchronousMode.get();
    }

    /**
     * Set RPC to asynchronous or synchronous mode.
     *
     * @param async true, RPC will be in asynchronous mode, otherwise false for synchronous mode
     */
    // TODO: 17/3/14 by zmyer
    @Unstable
    public static void setAsynchronousMode(boolean async) {
        asynchronousMode.set(async);
    }

    // TODO: 17/3/14 by zmyer
    private void releaseAsyncCall() {
        asyncCallCounter.decrementAndGet();
    }

    // TODO: 17/3/14 by zmyer
    @VisibleForTesting
    int getAsyncCallCount() {
        return asyncCallCounter.get();
    }

    // TODO: 17/3/14 by zmyer
    /** @return the rpc response or, in case of timeout, null. */
    private Writable getRpcResponse(final Call call, final Connection connection,
        final long timeout, final TimeUnit unit) throws IOException {
        synchronized (call) {
            while (!call.done) {
                try {
                    //等待call请求返回
                    AsyncGet.Util.wait(call, timeout, unit);
                    if (timeout >= 0 && !call.done) {
                        return null;
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new InterruptedIOException("Call interrupted");
                }
            }

            if (call.error != null) {
                if (call.error instanceof RemoteException) {
                    //填充call请求的异常对象
                    call.error.fillInStackTrace();
                    //抛出异常
                    throw call.error;
                } else { // local exception
                    InetSocketAddress address = connection.getRemoteAddress();
                    throw NetUtils.wrapException(address.getHostName(),
                        address.getPort(),
                        NetUtils.getHostname(),
                        0,
                        call.error);
                }
            } else {
                //返回call请求的应答消息
                return call.getRpcResponse();
            }
        }
    }

    // for unit testing only
    // TODO: 17/3/14 by zmyer
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    Set<ConnectionId> getConnectionIds() {
        return connections.keySet();
    }

    /**
     * Get a connection from the pool, or create a new one and add it to the
     * pool.  Connections to a given ConnectionId are reused.
     */
    // TODO: 17/3/14 by zmyer
    private Connection getConnection(ConnectionId remoteId,
        Call call, int serviceClass, AtomicBoolean fallbackToSimpleAuth)
        throws IOException {
        if (!running.get()) {
            // the client is stopped
            throw new IOException("The client is stopped");
        }
        Connection connection;
    /* we could avoid this allocation for each RPC by having a  
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
        while (true) {
            // These lines below can be shorten with computeIfAbsent in Java8
            //读取指定的连接对象
            connection = connections.get(remoteId);
            if (connection == null) {
                //如果不存在,则重新创建
                connection = new Connection(remoteId, serviceClass);
                //将创建成功的链接对象插入到列表中
                Connection existing = connections.putIfAbsent(remoteId, connection);
                if (existing != null) {
                    connection = existing;
                }
            }

            //将指定的call对象插入到连接对象中
            if (connection.addCall(call)) {
                break;
            } else {
                // This connection is closed, should be removed. But other thread could
                // have already known this closedConnection, and replace it with a new
                // connection. So we should call conditional remove to make sure we only
                // remove this closedConnection.
                connections.remove(remoteId, connection);
            }
        }

        // If the server happens to be slow, the method below will take longer to
        // establish a connection.
        connection.setupIOstreams(fallbackToSimpleAuth);
        return connection;
    }

    /**
     * This class holds the address and the user ticket. The client connections
     * to servers are uniquely identified by <remoteAddress, protocol, ticket>
     */
    // TODO: 17/3/14 by zmyer
    @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
    @InterfaceStability.Evolving
    public static class ConnectionId {
        InetSocketAddress address;
        UserGroupInformation ticket;
        final Class<?> protocol;
        private static final int PRIME = 16777619;
        private final int rpcTimeout;
        private final int maxIdleTime; //connections will be culled if it was idle for
        //maxIdleTime msecs
        private final RetryPolicy connectionRetryPolicy;
        private final int maxRetriesOnSasl;
        // the max. no. of retries for socket connections on time out exceptions
        private final int maxRetriesOnSocketTimeouts;
        private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
        private final boolean tcpLowLatency; // if T then use low-delay QoS
        private final boolean doPing; //do we need to send ping message
        private final int pingInterval; // how often sends ping to the server in msecs
        private String saslQop; // here for testing
        private final Configuration conf; // used to get the expected kerberos principal name

        // TODO: 17/3/14 by zmyer
        ConnectionId(InetSocketAddress address, Class<?> protocol,
            UserGroupInformation ticket, int rpcTimeout,
            RetryPolicy connectionRetryPolicy, Configuration conf) {
            this.protocol = protocol;
            this.address = address;
            this.ticket = ticket;
            this.rpcTimeout = rpcTimeout;
            this.connectionRetryPolicy = connectionRetryPolicy;

            this.maxIdleTime = conf.getInt(
                CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
                CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
            this.maxRetriesOnSasl = conf.getInt(
                CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY,
                CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_DEFAULT);
            this.maxRetriesOnSocketTimeouts = conf.getInt(
                CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
                CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
            this.tcpNoDelay = conf.getBoolean(
                CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
                CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_DEFAULT);
            this.tcpLowLatency = conf.getBoolean(
                CommonConfigurationKeysPublic.IPC_CLIENT_LOW_LATENCY,
                CommonConfigurationKeysPublic.IPC_CLIENT_LOW_LATENCY_DEFAULT
            );
            this.doPing = conf.getBoolean(
                CommonConfigurationKeys.IPC_CLIENT_PING_KEY,
                CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT);
            this.pingInterval = (doPing ? Client.getPingInterval(conf) : 0);
            this.conf = conf;
        }

        // TODO: 17/3/14 by zmyer
        InetSocketAddress getAddress() {
            return address;
        }

        // TODO: 17/3/14 by zmyer
        Class<?> getProtocol() {
            return protocol;
        }

        // TODO: 17/3/14 by zmyer
        UserGroupInformation getTicket() {
            return ticket;
        }

        // TODO: 17/3/14 by zmyer
        private int getRpcTimeout() {
            return rpcTimeout;
        }

        // TODO: 17/3/14 by zmyer
        int getMaxIdleTime() {
            return maxIdleTime;
        }

        // TODO: 17/3/14 by zmyer
        public int getMaxRetriesOnSasl() {
            return maxRetriesOnSasl;
        }

        // TODO: 17/3/14 by zmyer
        /** max connection retries on socket time outs */
        public int getMaxRetriesOnSocketTimeouts() {
            return maxRetriesOnSocketTimeouts;
        }

        // TODO: 17/3/14 by zmyer
        /** disable nagle's algorithm */
        boolean getTcpNoDelay() {
            return tcpNoDelay;
        }

        // TODO: 17/3/14 by zmyer
        /** use low-latency QoS bits over TCP */
        boolean getTcpLowLatency() {
            return tcpLowLatency;
        }

        // TODO: 17/3/14 by zmyer
        boolean getDoPing() {
            return doPing;
        }

        // TODO: 17/3/14 by zmyer
        int getPingInterval() {
            return pingInterval;
        }

        @VisibleForTesting
        String getSaslQop() {
            return saslQop;
        }

        /**
         * Returns a ConnectionId object.
         *
         * @param addr Remote address for the connection.
         * @param protocol Protocol for RPC.
         * @param ticket UGI
         * @param rpcTimeout timeout
         * @param conf Configuration object
         * @return A ConnectionId instance
         * @throws IOException
         */
        // TODO: 17/3/14 by zmyer
        static ConnectionId getConnectionId(InetSocketAddress addr,
            Class<?> protocol, UserGroupInformation ticket, int rpcTimeout,
            RetryPolicy connectionRetryPolicy, Configuration conf) throws IOException {

            if (connectionRetryPolicy == null) {
                final int max = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT);
                final int retryInterval = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY,
                    CommonConfigurationKeysPublic
                        .IPC_CLIENT_CONNECT_RETRY_INTERVAL_DEFAULT);

                //创建连接重试策略
                connectionRetryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
                    max, retryInterval, TimeUnit.MILLISECONDS);
            }

            // TODO: 17/3/14 by zmyer
            return new ConnectionId(addr, protocol, ticket, rpcTimeout,
                connectionRetryPolicy, conf);
        }

        // TODO: 17/3/14 by zmyer
        static boolean isEqual(Object a, Object b) {
            return a == null ? b == null : a.equals(b);
        }

        // TODO: 17/3/14 by zmyer
        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof ConnectionId) {
                ConnectionId that = (ConnectionId) obj;
                return isEqual(this.address, that.address)
                    && this.doPing == that.doPing
                    && this.maxIdleTime == that.maxIdleTime
                    && isEqual(this.connectionRetryPolicy, that.connectionRetryPolicy)
                    && this.pingInterval == that.pingInterval
                    && isEqual(this.protocol, that.protocol)
                    && this.rpcTimeout == that.rpcTimeout
                    && this.tcpNoDelay == that.tcpNoDelay
                    && isEqual(this.ticket, that.ticket);
            }
            return false;
        }

        // TODO: 17/3/14 by zmyer
        @Override
        public int hashCode() {
            int result = connectionRetryPolicy.hashCode();
            result = PRIME * result + ((address == null) ? 0 : address.hashCode());
            result = PRIME * result + (doPing ? 1231 : 1237);
            result = PRIME * result + maxIdleTime;
            result = PRIME * result + pingInterval;
            result = PRIME * result + ((protocol == null) ? 0 : protocol.hashCode());
            result = PRIME * result + rpcTimeout;
            result = PRIME * result + (tcpNoDelay ? 1231 : 1237);
            result = PRIME * result + ((ticket == null) ? 0 : ticket.hashCode());
            return result;
        }

        // TODO: 17/3/14 by zmyer
        @Override
        public String toString() {
            return address.toString();
        }
    }

    /**
     * Returns the next valid sequential call ID by incrementing an atomic counter
     * and masking off the sign bit.  Valid call IDs are non-negative integers in
     * the range [ 0, 2^31 - 1 ].  Negative numbers are reserved for special
     * purposes.  The values can overflow back to 0 and be reused.  Note that prior
     * versions of the client did not mask off the sign bit, so a server may still
     * see a negative call ID if it receives connections from an old client.
     *
     * @return next call ID
     */
    // TODO: 17/3/14 by zmyer
    public static int nextCallId() {
        return callIdCounter.getAndIncrement() & 0x7FFFFFFF;
    }

    // TODO: 17/3/14 by zmyer
    @Override
    @Unstable
    public void close() throws Exception {
        stop();
    }

    /**
     * Manages the input and output streams for an IPC connection.
     * Only exposed for use by SaslRpcClient.
     */
    // TODO: 17/3/14 by zmyer
    @InterfaceAudience.Private
    public static class IpcStreams implements Closeable, Flushable {
        //输入流对象
        private DataInputStream in;
        //输出流对象
        public DataOutputStream out;
        //应答最大长度
        private int maxResponseLength;
        //是否是第一次收到的应答
        private boolean firstResponse = true;

        // TODO: 17/3/14 by zmyer
        IpcStreams(Socket socket, int maxResponseLength) throws IOException {
            this.maxResponseLength = maxResponseLength;
            setInputStream(
                new BufferedInputStream(NetUtils.getInputStream(socket)));
            setOutputStream(
                new BufferedOutputStream(NetUtils.getOutputStream(socket)));
        }

        // TODO: 17/3/14 by zmyer
        void setSaslClient(SaslRpcClient client) throws IOException {
            setInputStream(client.getInputStream(in));
            setOutputStream(client.getOutputStream(out));
        }

        // TODO: 17/3/14 by zmyer
        private void setInputStream(InputStream is) {
            this.in = (is instanceof DataInputStream)
                ? (DataInputStream) is : new DataInputStream(is);
        }

        // TODO: 17/3/14 by zmyer
        private void setOutputStream(OutputStream os) {
            this.out = (os instanceof DataOutputStream)
                ? (DataOutputStream) os : new DataOutputStream(os);
        }

        // TODO: 17/3/14 by zmyer
        public ByteBuffer readResponse() throws IOException {
            int length = in.readInt();
            if (firstResponse) {
                firstResponse = false;
                // pre-rpcv9 exception, almost certainly a version mismatch.
                if (length == -1) {
                    in.readInt(); // ignore fatal/error status, it's fatal for us.
                    throw new RemoteException(WritableUtils.readString(in),
                        WritableUtils.readString(in));
                }
            }
            if (length <= 0) {
                throw new RpcException("RPC response has invalid length");
            }
            if (maxResponseLength > 0 && length > maxResponseLength) {
                throw new RpcException("RPC response exceeds maximum data length");
            }
            //分配指定长度的缓冲区
            ByteBuffer bb = ByteBuffer.allocate(length);
            //
            in.readFully(bb.array());
            return bb;
        }

        // TODO: 17/3/14 by zmyer
        public void sendRequest(byte[] buf) throws IOException {
            out.write(buf);
        }

        // TODO: 17/3/14 by zmyer
        @Override
        public void flush() throws IOException {
            out.flush();
        }

        // TODO: 17/3/14 by zmyer
        @Override
        public void close() {
            IOUtils.closeStream(out);
            IOUtils.closeStream(in);
        }
    }
}
