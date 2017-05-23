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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.ipc.RPC.RpcInvoker;
import org.apache.hadoop.ipc.RPC.VersionMismatch;
import org.apache.hadoop.ipc.metrics.RpcDetailedMetrics;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcKindProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslAuth;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcSaslProto.SaslState;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.SaslPropertiesResolver;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import static org.apache.hadoop.ipc.RpcConstants.AUTHORIZATION_FAILED_CALL_ID;
import static org.apache.hadoop.ipc.RpcConstants.CONNECTION_CONTEXT_CALL_ID;
import static org.apache.hadoop.ipc.RpcConstants.CURRENT_VERSION;
import static org.apache.hadoop.ipc.RpcConstants.HEADER_LEN_AFTER_HRPC_PART;
import static org.apache.hadoop.ipc.RpcConstants.PING_CALL_ID;

/**
 * An abstract IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 *
 * @see Client
 */
@Public
@InterfaceStability.Evolving
public abstract class Server {
    //是否认证
    private final boolean authorize;
    //认证函数列表
    private List<AuthMethod> enabledAuthMethods;
    //sasl应答消息
    private RpcSaslProto negotiateResponse;
    //异常处理对象
    private ExceptionsHandler exceptionsHandler = new ExceptionsHandler();
    //跟踪对象
    private Tracer tracer;

    /**
     * Add exception classes for which server won't log stack traces.
     *
     * @param exceptionClass exception classes
     */
    // TODO: 17/3/15 by zmyer
    public void addTerseExceptions(Class<?>... exceptionClass) {
        //设置异常类对象
        exceptionsHandler.addTerseLoggingExceptions(exceptionClass);
    }

    /**
     * Add exception classes which server won't log at all.
     *
     * @param exceptionClass exception classes
     */
    // TODO: 17/3/15 by zmyer
    public void addSuppressedLoggingExceptions(Class<?>... exceptionClass) {
        exceptionsHandler.addSuppressedLoggingExceptions(exceptionClass);
    }

    /**
     * ExceptionsHandler manages Exception groups for special handling
     * e.g., terse exception group for concise logging messages
     */
    // TODO: 17/3/15 by zmyer
    static class ExceptionsHandler {
        private volatile Set<String> terseExceptions = new HashSet<>();
        private volatile Set<String> suppressedExceptions = new HashSet<>();

        /**
         * Add exception classes for which server won't log stack traces.
         * Optimized for infrequent invocation.
         *
         * @param exceptionClass exception classes
         */
        // TODO: 17/3/15 by zmyer
        void addTerseLoggingExceptions(Class<?>... exceptionClass) {
            // Thread-safe replacement of terseExceptions.
            terseExceptions = addExceptions(terseExceptions, exceptionClass);
        }

        /**
         * Add exception classes which server won't log at all.
         * Optimized for infrequent invocation.
         *
         * @param exceptionClass exception classes
         */
        // TODO: 17/3/15 by zmyer
        void addSuppressedLoggingExceptions(Class<?>... exceptionClass) {
            // Thread-safe replacement of suppressedExceptions.
            suppressedExceptions = addExceptions(
                suppressedExceptions, exceptionClass);
        }

        // TODO: 17/3/15 by zmyer
        boolean isTerseLog(Class<?> t) {
            return terseExceptions.contains(t.toString());
        }

        // TODO: 17/3/15 by zmyer
        boolean isSuppressedLog(Class<?> t) {
            return suppressedExceptions.contains(t.toString());
        }

        /**
         * Return a new set containing all the exceptions in exceptionsSet
         * and exceptionClass.
         *
         * @return
         */
        // TODO: 17/3/15 by zmyer
        private static Set<String> addExceptions(
            final Set<String> exceptionsSet, Class<?>[] exceptionClass) {
            // Make a copy of the exceptionSet for performing modification
            final HashSet<String> newSet = new HashSet<>(exceptionsSet);

            // Add all class names into the HashSet
            for (Class<?> name : exceptionClass) {
                newSet.add(name.toString());
            }

            return Collections.unmodifiableSet(newSet);
        }
    }

    /**
     * If the user accidentally sends an HTTP GET to an IPC port, we detect this
     * and send back a nicer response.
     */
    // TODO: 17/3/15 by zmyer
    private static final ByteBuffer HTTP_GET_BYTES = ByteBuffer.wrap("GET ".getBytes(StandardCharsets.UTF_8));

    /**
     * An HTTP response to send back if we detect an HTTP request to our IPC
     * port.
     */
    // TODO: 17/3/15 by zmyer
    static final String RECEIVED_HTTP_REQ_RESPONSE =
        "HTTP/1.1 404 Not Found\r\n" +
            "Content-type: text/plain\r\n\r\n" +
            "It looks like you are making an HTTP request to a Hadoop IPC port. " +
            "This is not the correct port for the web interface on this daemon.\r\n";

    /**
     * Initial and max size of response buffer
     */
    //应答缓冲区长度
    static int INITIAL_RESP_BUF_SIZE = 10240;

    // TODO: 17/3/15 by zmyer
    static class RpcKindMapValue {
        //请求封装类对象
        final Class<? extends Writable> rpcRequestWrapperClass;
        //rpc调用对象
        final RpcInvoker rpcInvoker;

        // TODO: 17/3/15 by zmyer
        RpcKindMapValue(Class<? extends Writable> rpcRequestWrapperClass,
            RpcInvoker rpcInvoker) {
            //rpc调用对象
            this.rpcInvoker = rpcInvoker;
            //设置请求封装类对象
            this.rpcRequestWrapperClass = rpcRequestWrapperClass;
        }
    }

    //rpc请求类映射表
    static Map<RPC.RpcKind, RpcKindMapValue> rpcKindMap = new HashMap<>(4);

    /**
     * Register a RPC kind and the class to deserialize the rpc request.
     *
     * Called by static initializers of rpcKind Engines
     *
     * @param rpcKind
     * @param rpcRequestWrapperClass - this class is used to deserialze the the rpc request.
     * @param rpcInvoker - use to process the calls on SS.
     */

    // TODO: 17/3/15 by zmyer
    public static void registerProtocolEngine(RPC.RpcKind rpcKind,
        Class<? extends Writable> rpcRequestWrapperClass, RpcInvoker rpcInvoker) {
        //注册rpc类型以及请rpc调用对象
        RpcKindMapValue old =
            rpcKindMap.put(rpcKind, new RpcKindMapValue(rpcRequestWrapperClass,
                rpcInvoker));
        if (old != null) {
            //如果当前的rpc类型已经存在,则需要替换掉新注册的类
            rpcKindMap.put(rpcKind, old);
            //直接抛出异常
            throw new IllegalArgumentException("ReRegistration of rpcKind: " +
                rpcKind);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("rpcKind=" + rpcKind +
                ", rpcRequestWrapperClass=" + rpcRequestWrapperClass +
                ", rpcInvoker=" + rpcInvoker);
        }
    }

    // TODO: 17/3/15 by zmyer
    public Class<? extends Writable> getRpcRequestWrapper(RpcKindProto rpcKind) {
        if (rpcRequestClass != null)
            //返回rpc请求类对象
            return rpcRequestClass;
        //根据rpc类型,读取rpc值对象
        RpcKindMapValue val = rpcKindMap.get(ProtoUtil.convert(rpcKind));
        return (val == null) ? null : val.rpcRequestWrapperClass;
    }

    // TODO: 17/3/15 by zmyer
    public static RpcInvoker getRpcInvoker(RPC.RpcKind rpcKind) {
        //根据rpc类型读取值对象
        RpcKindMapValue val = rpcKindMap.get(rpcKind);
        //返回rpc调用对象
        return (val == null) ? null : val.rpcInvoker;
    }

    public static final Log LOG = LogFactory.getLog(Server.class);
    public static final Log AUDITLOG =
        LogFactory.getLog("SecurityLogger." + Server.class.getName());
    private static final String AUTH_FAILED_FOR = "Auth failed for ";
    private static final String AUTH_SUCCESSFUL_FOR = "Auth successful for ";

    //服务器对象
    private static final ThreadLocal<Server> SERVER = new ThreadLocal<Server>();

    //协议缓存对象
    private static final Map<String, Class<?>> PROTOCOL_CACHE = new ConcurrentHashMap<String, Class<?>>();

    // TODO: 17/3/15 by zmyer
    static Class<?> getProtocolClass(String protocolName, Configuration conf)
        throws ClassNotFoundException {
        //根据协议名称,读取协议类对象
        Class<?> protocol = PROTOCOL_CACHE.get(protocolName);
        if (protocol == null) {
            //如果协议类对象为空,则从配置里面读取指定的协议名称,读取
            protocol = conf.getClassByName(protocolName);
            //将读取到的协议类对象插入到协议缓存中
            PROTOCOL_CACHE.put(protocolName, protocol);
        }
        return protocol;
    }

    /**
     * Returns the server instance called under or null.  May be called under
     * {@link #call(Writable, long)} implementations, and under {@link Writable}
     * methods of paramters and return values.  Permits applications to access
     * the server context.
     */
    // TODO: 17/3/15 by zmyer
    public static Server get() {
        return SERVER.get();
    }

    /**
     * This is set to Call object before Handler invokes an RPC and reset
     * after the call returns.
     */
    //当前的call对象
    private static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();

    /** Get the current call */
    // TODO: 17/3/15 by zmyer
    @VisibleForTesting
    public static ThreadLocal<Call> getCurCall() {
        return CurCall;
    }

    /**
     * Returns the currently active RPC call's sequential ID number.  A negative
     * call ID indicates an invalid value, such as if there is no currently active
     * RPC call.
     *
     * @return int sequential ID number of currently active RPC call
     */
    // TODO: 17/3/15 by zmyer
    public static int getCallId() {
        //读取call对象
        Call call = CurCall.get();
        //读取call id
        return call != null ? call.callId : RpcConstants.INVALID_CALL_ID;
    }

    /**
     * @return The current active RPC call's retry count. -1 indicates the retry cache is not
     * supported in the client side.
     */
    // TODO: 17/3/15 by zmyer
    public static int getCallRetryCount() {
        //读取call对象
        Call call = CurCall.get();
        //读取call的重试次数
        return call != null ? call.retryCount : RpcConstants.INVALID_RETRY_COUNT;
    }

    /**
     * Returns the remote side ip address when invoked inside an RPC
     * Returns null incase of an error.
     */
    // TODO: 17/3/15 by zmyer
    public static InetAddress getRemoteIp() {
        //读取call对象
        Call call = CurCall.get();
        //读取call的本地地址
        return (call != null) ? call.getHostInetAddress() : null;
    }

    /**
     * Returns the clientId from the current RPC request
     */
    // TODO: 17/3/15 by zmyer
    public static byte[] getClientId() {
        //读取call对象
        Call call = CurCall.get();
        //返回call id
        return call != null ? call.clientId : RpcConstants.DUMMY_CLIENT_ID;
    }

    /**
     * Returns remote address as a string when invoked inside an RPC.
     * Returns null in case of an error.
     */
    // TODO: 17/3/15 by zmyer
    public static String getRemoteAddress() {
        //读取远程的地址对象
        InetAddress addr = getRemoteIp();
        //返回地址信息
        return (addr == null) ? null : addr.getHostAddress();
    }

    /**
     * Returns the RPC remote user when invoked inside an RPC.  Note this
     * may be different than the current user if called within another doAs
     *
     * @return connection's UGI or null if not an RPC
     */
    // TODO: 17/3/15 by zmyer
    public static UserGroupInformation getRemoteUser() {
        Call call = CurCall.get();
        return (call != null) ? call.getRemoteUser() : null;
    }

    // TODO: 17/3/15 by zmyer
    public static String getProtocol() {
        Call call = CurCall.get();
        return (call != null) ? call.getProtocol() : null;
    }

    /**
     * Return true if the invocation was through an RPC.
     */
    // TODO: 17/3/15 by zmyer
    public static boolean isRpcInvocation() {
        return CurCall.get() != null;
    }

    /**
     * Return the priority level assigned by call queue to an RPC
     * Returns 0 in case no priority is assigned.
     */
    // TODO: 17/3/15 by zmyer
    public static int getPriorityLevel() {
        Call call = CurCall.get();
        return call != null ? call.getPriorityLevel() : 0;
    }

    //绑定的地址
    private String bindAddress;
    //端口号
    private int port;                               // port we listen on
    //处理线程的数量
    private int handlerCount;                       // number of handler threads
    //读线程的数量
    private int readThreads;                        // number of read threads
    //每个读线程中挂起的链接数量
    private int readerPendingConnectionQueue;         // number of connections to queue per read thread
    //rpc请求类对象
    private Class<? extends Writable> rpcRequestClass;   // class used for deserializing the rpc request
    //rpc统计对象
    final protected RpcMetrics rpcMetrics;
    //rpc详细统计对象
    final protected RpcDetailedMetrics rpcDetailedMetrics;

    //服务器配置对象
    private Configuration conf;
    //端口范围配置对象
    private String portRangeConfig = null;
    //
    private SecretManager<TokenIdentifier> secretManager;
    private SaslPropertiesResolver saslPropsResolver;
    private ServiceAuthorizationManager serviceAuthorizationManager = new ServiceAuthorizationManager();

    //服务器队列的最大长度
    private int maxQueueSize;
    //最大的应答消息长度
    private final int maxRespSize;
    //应答缓冲区对象
    private final ThreadLocal<ResponseBuffer> responseBuffer =
        new ThreadLocal<ResponseBuffer>() {
            @Override
            protected ResponseBuffer initialValue() {
                //创建应答缓冲区
                return new ResponseBuffer(INITIAL_RESP_BUF_SIZE);
            }
        };

    //套接字发送缓冲区大小
    private int socketSendBufferSize;
    //数据体最大的长度
    private final int maxDataLength;
    //tcp非延时标记
    private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm

    //服务器运行标记
    volatile private boolean running = true;         // true while server runs
    //call队列管理器
    private CallQueueManager<Call> callQueue;

    // maintains the set of client connections and handles idle timeouts
    //连接管理器
    private ConnectionManager connectionManager;
    //服务器连接监听器
    private Listener listener = null;
    //服务器应答对象
    private Responder responder = null;
    //服务器处理器数组
    private Handler[] handlers = null;

    private boolean logSlowRPC = false;

    /**
     * Checks if LogSlowRPC is set true.
     *
     * @return true, if LogSlowRPC is set true, false, otherwise.
     */
    // TODO: 17/3/15 by zmyer
    protected boolean isLogSlowRPC() {
        return logSlowRPC;
    }

    /**
     * Sets slow RPC flag.
     *
     * @param logSlowRPCFlag
     */
    // TODO: 17/3/15 by zmyer
    @VisibleForTesting
    protected void setLogSlowRPC(boolean logSlowRPCFlag) {
        this.logSlowRPC = logSlowRPCFlag;
    }

    /**
     * Logs a Slow RPC Request.
     *
     * @param methodName - RPC Request method name
     * @param processingTime - Processing Time.
     *
     * if this request took too much time relative to other requests we consider that as a slow RPC.
     * 3 is a magic number that comes from 3 sigma deviation. A very simple explanation can be found
     * by searching for 68-95-99.7 rule. We flag an RPC as slow RPC if and only if it falls above
     * 99.7% of requests. We start this logic only once we have enough sample size.
     */
    // TODO: 17/3/15 by zmyer
    void logSlowRpcCalls(String methodName, int processingTime) {
        final int deviation = 3;

        // 1024 for minSampleSize just a guess -- not a number computed based on
        // sample size analysis. It is chosen with the hope that this
        // number is high enough to avoid spurious logging, yet useful
        // in practice.
        final int minSampleSize = 1024;
        final double threeSigma = rpcMetrics.getProcessingMean() +
            (rpcMetrics.getProcessingStdDev() * deviation);

        if ((rpcMetrics.getProcessingSampleCount() > minSampleSize) &&
            (processingTime > threeSigma)) {
            if (LOG.isWarnEnabled()) {
                String client = CurCall.get().toString();
                LOG.warn(
                    "Slow RPC : " + methodName + " took " + processingTime +
                        " milliseconds to process from client " + client);
            }
            rpcMetrics.incrSlowRpc();
        }
    }

    // TODO: 17/3/15 by zmyer
    void updateMetrics(String name, int queueTime, int processingTime,
        boolean deferredCall) {
        rpcMetrics.addRpcQueueTime(queueTime);
        if (!deferredCall) {
            rpcMetrics.addRpcProcessingTime(processingTime);
            rpcDetailedMetrics.addProcessingTime(name, processingTime);
            callQueue.addResponseTime(name, getPriorityLevel(), queueTime,
                processingTime);
            if (isLogSlowRPC()) {
                logSlowRpcCalls(name, processingTime);
            }
        }
    }

    // TODO: 17/3/15 by zmyer
    void updateDeferredMetrics(String name, long processingTime) {
        rpcMetrics.addDeferredRpcProcessingTime(processingTime);
        rpcDetailedMetrics.addDeferredProcessingTime(name, processingTime);
    }

    /**
     * A convenience method to bind to a given address and report
     * better exceptions if the address is not a valid host.
     *
     * @param socket the socket to bind
     * @param address the address to bind to
     * @param backlog the number of connections allowed in the queue
     * @throws BindException if the address can't be bound
     * @throws UnknownHostException if the address isn't a valid host name
     * @throws IOException other random errors from bind
     */
    // TODO: 17/3/15 by zmyer
    public static void bind(ServerSocket socket, InetSocketAddress address,
        int backlog) throws IOException {
        //绑定服务器地址
        bind(socket, address, backlog, null, null);
    }

    // TODO: 17/3/15 by zmyer
    public static void bind(ServerSocket socket, InetSocketAddress address,
        int backlog, Configuration conf, String rangeConf) throws IOException {
        try {
            IntegerRanges range = null;
            if (rangeConf != null) {
                //从配置中读取端口范围
                range = conf.getRange(rangeConf, "");
            }
            if (range == null || range.isEmpty() || (address.getPort() != 0)) {
                //直接绑定服务器地址
                socket.bind(address, backlog);
            } else {
                for (Integer port : range) {
                    if (socket.isBound())
                        break;
                    try {
                        //根据地址信息,创建套接字对象
                        InetSocketAddress temp = new InetSocketAddress(address.getAddress(),
                            port);
                        //开始在套接字上绑定地址
                        socket.bind(temp, backlog);
                    } catch (BindException e) {
                        //Ignored
                    }
                }
                if (!socket.isBound()) {
                    throw new BindException("Could not find a free port in " + range);
                }
            }
        } catch (SocketException e) {
            throw NetUtils.wrapException(null,
                0,
                address.getHostName(),
                address.getPort(), e);
        }
    }

    /**
     * Returns a handle to the rpcMetrics (required in tests)
     *
     * @return rpc metrics
     */
    // TODO: 17/3/15 by zmyer
    @VisibleForTesting
    public RpcMetrics getRpcMetrics() {
        return rpcMetrics;
    }

    // TODO: 17/3/15 by zmyer
    @VisibleForTesting
    public RpcDetailedMetrics getRpcDetailedMetrics() {
        return rpcDetailedMetrics;
    }

    // TODO: 17/3/15 by zmyer
    @VisibleForTesting
    Iterable<? extends Thread> getHandlers() {
        return Arrays.asList(handlers);
    }

    // TODO: 17/3/15 by zmyer
    @VisibleForTesting
    Connection[] getConnections() {
        return connectionManager.toArray();
    }

    /**
     * Refresh the service authorization ACL for the service handled by this server.
     */
    // TODO: 17/3/15 by zmyer
    public void refreshServiceAcl(Configuration conf, PolicyProvider provider) {
        serviceAuthorizationManager.refresh(conf, provider);
    }

    /**
     * Refresh the service authorization ACL for the service handled by this server
     * using the specified Configuration.
     */
    // TODO: 17/3/15 by zmyer
    @Private
    public void refreshServiceAclWithLoadedConfiguration(Configuration conf,
        PolicyProvider provider) {
        serviceAuthorizationManager.refreshWithLoadedConfiguration(conf, provider);
    }

    /**
     * Returns a handle to the serviceAuthorizationManager (required in tests)
     *
     * @return instance of ServiceAuthorizationManager for this server
     */
    // TODO: 17/3/15 by zmyer
    @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
    public ServiceAuthorizationManager getServiceAuthorizationManager() {
        return serviceAuthorizationManager;
    }

    // TODO: 17/3/15 by zmyer
    private String getQueueClassPrefix() {
        return CommonConfigurationKeys.IPC_NAMESPACE + "." + port;
    }

    // TODO: 17/3/15 by zmyer
    static Class<? extends BlockingQueue<Call>> getQueueClass(
        String prefix, Configuration conf) {
        String name = prefix + "." + CommonConfigurationKeys.IPC_CALLQUEUE_IMPL_KEY;
        //读取队列类对象
        Class<?> queueClass = conf.getClass(name, LinkedBlockingQueue.class);
        //返回call队列对昂
        return CallQueueManager.convertQueueClass(queueClass, Call.class);
    }

    // TODO: 17/3/15 by zmyer
    static Class<? extends RpcScheduler> getSchedulerClass(
        String prefix, Configuration conf) {
        //调度键值名称
        String schedulerKeyname = prefix + "." + CommonConfigurationKeys
            .IPC_SCHEDULER_IMPL_KEY;
        //从配置中读取调度类对象
        Class<?> schedulerClass = conf.getClass(schedulerKeyname, null);
        // Patch the configuration for legacy fcq configuration that does not have
        // a separate scheduler setting
        if (schedulerClass == null) {
            //如果调度类对象为空,则创建队列类名称
            String queueKeyName = prefix + "." + CommonConfigurationKeys.IPC_CALLQUEUE_IMPL_KEY;
            //从配置中读取队列类对象
            Class<?> queueClass = conf.getClass(queueKeyName, null);
            if (queueClass != null) {
                if (queueClass.getCanonicalName().equals(
                    FairCallQueue.class.getCanonicalName())) {
                    //如果队列类对象是公平调度队列
                    conf.setClass(schedulerKeyname, DecayRpcScheduler.class, RpcScheduler.class);
                }
            }
        }
        //从配置中读取调度类对象
        schedulerClass = conf.getClass(schedulerKeyname, DefaultRpcScheduler.class);

        //返回调度类对象
        return CallQueueManager.convertSchedulerClass(schedulerClass);
    }

    /*
   * Refresh the call queue
   */
    // TODO: 17/3/15 by zmyer
    public synchronized void refreshCallQueue(Configuration conf) {
        // Create the next queue
        String prefix = getQueueClassPrefix();
        //交换call队列
        callQueue.swapQueue(getSchedulerClass(prefix, conf),
            getQueueClass(prefix, conf), maxQueueSize, prefix, conf);
    }

    /**
     * Get from config if client backoff is enabled on that port.
     */
    // TODO: 17/3/17 by zmyer
    static boolean getClientBackoffEnable(String prefix, Configuration conf) {
        String name = prefix + "." + CommonConfigurationKeys.IPC_BACKOFF_ENABLE;
        return conf.getBoolean(name, CommonConfigurationKeys.IPC_BACKOFF_ENABLE_DEFAULT);
    }

    /** A generic call queued for handling. */
    // TODO: 17/3/17 by zmyer
    public static class Call implements Schedulable, PrivilegedExceptionAction<Void> {
        //callid
        final int callId;            // the client's call id
        //重试次数
        final int retryCount;        // the retry count of the call
        //时间戳
        long timestamp;              // time received when response is null
        // time served when response is not null
        //等待应答的消息数目
        private AtomicInteger responseWaitCount = new AtomicInteger(1);
        //rpc类型
        final RPC.RpcKind rpcKind;
        //client id数组
        final byte[] clientId;
        private final TraceScope traceScope; // the HTrace scope on the server side
        //call上下文对象
        private final CallerContext callerContext; // the call context
        private boolean deferredResponse = false;
        //优先级
        private int priorityLevel;
        // the priority level assigned by scheduler, 0 by default

        // TODO: 17/3/17 by zmyer
        Call() {
            this(RpcConstants.INVALID_CALL_ID, RpcConstants.INVALID_RETRY_COUNT,
                RPC.RpcKind.RPC_BUILTIN, RpcConstants.DUMMY_CLIENT_ID);
        }

        // TODO: 17/3/17 by zmyer
        Call(Call call) {
            this(call.callId, call.retryCount, call.rpcKind, call.clientId,
                call.traceScope, call.callerContext);
        }

        // TODO: 17/3/17 by zmyer
        Call(int id, int retryCount, RPC.RpcKind kind, byte[] clientId) {
            this(id, retryCount, kind, clientId, null, null);
        }

        // TODO: 17/3/17 by zmyer
        @VisibleForTesting // primarily TestNamenodeRetryCache
        public Call(int id, int retryCount, Void ignore1, Void ignore2,
            RPC.RpcKind kind, byte[] clientId) {
            this(id, retryCount, kind, clientId, null, null);
        }

        // TODO: 17/3/17 by zmyer
        Call(int id, int retryCount, RPC.RpcKind kind, byte[] clientId,
            TraceScope traceScope, CallerContext callerContext) {
            this.callId = id;
            this.retryCount = retryCount;
            this.timestamp = Time.now();
            this.rpcKind = kind;
            this.clientId = clientId;
            this.traceScope = traceScope;
            this.callerContext = callerContext;
        }

        // TODO: 17/3/17 by zmyer
        @Override
        public String toString() {
            return "Call#" + callId + " Retry#" + retryCount;
        }

        // TODO: 17/3/17 by zmyer
        @Override
        public Void run() throws Exception {
            return null;
        }

        // TODO: 17/3/17 by zmyer
        // should eventually be abstract but need to avoid breaking tests
        public UserGroupInformation getRemoteUser() {
            return null;
        }

        // TODO: 17/3/17 by zmyer
        public InetAddress getHostInetAddress() {
            return null;
        }

        // TODO: 17/3/17 by zmyer
        public String getHostAddress() {
            InetAddress addr = getHostInetAddress();
            return (addr != null) ? addr.getHostAddress() : null;
        }

        // TODO: 17/3/17 by zmyer
        public String getProtocol() {
            return null;
        }

        /**
         * Allow a IPC response to be postponed instead of sent immediately
         * after the handler returns from the proxy method.  The intended use
         * case is freeing up the handler thread when the response is known,
         * but an expensive pre-condition must be satisfied before it's sent
         * to the client.
         */
        // TODO: 17/3/17 by zmyer
        @InterfaceStability.Unstable
        @InterfaceAudience.LimitedPrivate({"HDFS"})
        public final void postponeResponse() {
            //递增等待应答的次数
            int count = responseWaitCount.incrementAndGet();
            assert count > 0 : "response has already been sent";
        }

        // TODO: 17/3/17 by zmyer
        @InterfaceStability.Unstable
        @InterfaceAudience.LimitedPrivate({"HDFS"})
        public final void sendResponse() throws IOException {
            //递减应答次数
            int count = responseWaitCount.decrementAndGet();
            assert count >= 0 : "response has already been sent";
            if (count == 0) {
                doResponse(null);
            }
        }

        // TODO: 17/3/17 by zmyer
        @InterfaceStability.Unstable
        @InterfaceAudience.LimitedPrivate({"HDFS"})
        public final void abortResponse(Throwable t) throws IOException {
            // don't send response if the call was already sent or aborted.
            if (responseWaitCount.getAndSet(-1) > 0) {
                doResponse(t);
            }
        }

        // TODO: 17/3/17 by zmyer
        void doResponse(Throwable t) throws IOException {
        }

        // For Schedulable
        // TODO: 17/3/17 by zmyer
        @Override
        public UserGroupInformation getUserGroupInformation() {
            return getRemoteUser();
        }

        // TODO: 17/3/17 by zmyer
        @Override
        public int getPriorityLevel() {
            return this.priorityLevel;
        }

        // TODO: 17/3/17 by zmyer
        public void setPriorityLevel(int priorityLevel) {
            this.priorityLevel = priorityLevel;
        }

        // TODO: 17/3/17 by zmyer
        @InterfaceStability.Unstable
        public void deferResponse() {
            this.deferredResponse = true;
        }

        // TODO: 17/3/17 by zmyer
        @InterfaceStability.Unstable
        public boolean isResponseDeferred() {
            return this.deferredResponse;
        }

        // TODO: 17/3/17 by zmyer
        public void setDeferredResponse(Writable response) {
        }

        // TODO: 17/3/17 by zmyer
        public void setDeferredError(Throwable t) {
        }
    }

    /** A RPC extended call queued for handling. */
    // TODO: 17/3/17 by zmyer
    private class RpcCall extends Call {
        //连接对象
        final Connection connection;  // connection to client
        //rpc请求对象
        final Writable rpcRequest;    // Serialized Rpc request from client
        //rpc应答请求
        ByteBuffer rpcResponse;       // the response for this call

        // TODO: 17/3/17 by zmyer
        RpcCall(RpcCall call) {
            super(call);
            this.connection = call.connection;
            this.rpcRequest = call.rpcRequest;
        }

        // TODO: 17/3/17 by zmyer
        RpcCall(Connection connection, int id) {
            this(connection, id, RpcConstants.INVALID_RETRY_COUNT);
        }

        // TODO: 17/3/17 by zmyer
        RpcCall(Connection connection, int id, int retryCount) {
            this(connection, id, retryCount, null,
                RPC.RpcKind.RPC_BUILTIN, RpcConstants.DUMMY_CLIENT_ID,
                null, null);
        }

        // TODO: 17/3/17 by zmyer
        RpcCall(Connection connection, int id, int retryCount,
            Writable param, RPC.RpcKind kind, byte[] clientId,
            TraceScope traceScope, CallerContext context) {
            super(id, retryCount, kind, clientId, traceScope, context);
            this.connection = connection;
            this.rpcRequest = param;
        }

        // TODO: 17/3/17 by zmyer
        @Override
        public String getProtocol() {
            return "rpc";
        }

        // TODO: 17/3/17 by zmyer
        @Override
        public UserGroupInformation getRemoteUser() {
            return connection.user;
        }

        // TODO: 17/3/17 by zmyer
        @Override
        public InetAddress getHostInetAddress() {
            return connection.getHostInetAddress();
        }

        // TODO: 17/3/17 by zmyer
        @Override
        public Void run() throws Exception {
            //连接对象通道未打开,直接报错
            if (!connection.channel.isOpen()) {
                Server.LOG.info(Thread.currentThread().getName() + ": skipped " + this);
                return null;
            }

            Writable value = null;
            ResponseParams responseParams = new ResponseParams();

            try {
                //调用请求
                value = call(rpcKind, connection.protocolName, rpcRequest, timestamp);
            } catch (Throwable e) {
                //处理错误请求
                populateResponseParamsOnError(e, responseParams);
            }
            if (!isResponseDeferred()) {
                //创建应答请求
                setupResponse(this, responseParams.returnStatus,
                    responseParams.detailedErr,
                    value, responseParams.errorClass, responseParams.error);
                //发送应答
                sendResponse();
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Deferring response for callId: " + this.callId);
                }
            }
            return null;
        }

        /**
         * @param t the {@link java.lang.Throwable} to use to set errorInfo
         * @param responseParams the {@link ResponseParams} instance to populate
         */
        // TODO: 17/3/17 by zmyer
        private void populateResponseParamsOnError(Throwable t, ResponseParams responseParams) {
            if (t instanceof UndeclaredThrowableException) {
                t = t.getCause();
            }
            logException(Server.LOG, t, this);
            if (t instanceof RpcServerException) {
                RpcServerException rse = ((RpcServerException) t);
                responseParams.returnStatus = rse.getRpcStatusProto();
                responseParams.detailedErr = rse.getRpcErrorCodeProto();
            } else {
                responseParams.returnStatus = RpcStatusProto.ERROR;
                responseParams.detailedErr = RpcErrorCodeProto.ERROR_APPLICATION;
            }
            responseParams.errorClass = t.getClass().getName();
            responseParams.error = StringUtils.stringifyException(t);
            // Remove redundant error class name from the beginning of the
            // stack trace
            String exceptionHdr = responseParams.errorClass + ": ";
            if (responseParams.error.startsWith(exceptionHdr)) {
                responseParams.error = responseParams.error.substring(exceptionHdr.length());
            }
        }

        // TODO: 17/3/17 by zmyer
        void setResponse(ByteBuffer response) throws IOException {
            this.rpcResponse = response;
        }

        // TODO: 17/3/17 by zmyer
        @Override
        void doResponse(Throwable t) throws IOException {
            RpcCall call = this;
            if (t != null) {
                // clone the call to prevent a race with another thread stomping
                // on the response while being sent.  the original call is
                // effectively discarded since the wait count won't hit zero
                call = new RpcCall(this);
                //创建应答消息
                setupResponse(call, RpcStatusProto.FATAL, RpcErrorCodeProto.ERROR_RPC_SERVER,
                    null, t.getClass().getName(), StringUtils.stringifyException(t));
            }
            //从连接对象处发送应答消息
            connection.sendResponse(call);
        }

        /**
         * Send a deferred response, ignoring errors.
         */
        // TODO: 17/3/17 by zmyer
        private void sendDeferedResponse() {
            try {
                //发送应答消息
                connection.sendResponse(this);
            } catch (Exception e) {
                // For synchronous calls, application code is done once it's returned
                // from a method. It does not expect to receive an error.
                // This is equivalent to what happens in synchronous calls when the
                // Responder is not able to send out the response.
                LOG.error("Failed to send deferred response. ThreadName=" + Thread
                    .currentThread().getName() + ", CallId=" + callId + ", hostname=" + getHostAddress());
            }
        }

        // TODO: 17/3/17 by zmyer
        @Override
        public void setDeferredResponse(Writable response) {
            if (this.connection.getServer().running) {
                try {
                    //创建应答消息
                    setupResponse(this, RpcStatusProto.SUCCESS, null, response, null, null);
                } catch (IOException e) {
                    // For synchronous calls, application code is done once it has
                    // returned from a method. It does not expect to receive an error.
                    // This is equivalent to what happens in synchronous calls when the
                    // response cannot be sent.
                    LOG.error("Failed to setup deferred successful response. ThreadName=" +
                        Thread.currentThread().getName() + ", Call=" + this);
                    return;
                }
                //发送应答消息
                sendDeferedResponse();
            }
        }

        // TODO: 17/3/17 by zmyer
        @Override
        public void setDeferredError(Throwable t) {
            if (this.connection.getServer().running) {
                if (t == null) {
                    t = new IOException(
                        "User code indicated an error without an exception");
                }
                try {
                    ResponseParams responseParams = new ResponseParams();
                    populateResponseParamsOnError(t, responseParams);
                    setupResponse(this, responseParams.returnStatus,
                        responseParams.detailedErr,
                        null, responseParams.errorClass, responseParams.error);
                } catch (IOException e) {
                    // For synchronous calls, application code is done once it has
                    // returned from a method. It does not expect to receive an error.
                    // This is equivalent to what happens in synchronous calls when the
                    // response cannot be sent.
                    LOG.error(
                        "Failed to setup deferred error response. ThreadName=" +
                            Thread.currentThread().getName() + ", Call=" + this);
                }
                sendDeferedResponse();
            }
        }

        /**
         * Holds response parameters. Defaults set to work for successful
         * invocations
         */
        // TODO: 17/3/17 by zmyer
        private class ResponseParams {
            String errorClass = null;
            String error = null;
            RpcErrorCodeProto detailedErr = null;
            RpcStatusProto returnStatus = RpcStatusProto.SUCCESS;
        }

        // TODO: 17/3/17 by zmyer
        @Override
        public String toString() {
            return super.toString() + " " + rpcRequest + " from " + connection;
        }
    }

    /** Listens on the socket. Creates jobs for the handler threads */
    // TODO: 17/3/17 by zmyer
    private class Listener extends Thread {
        //接收链接通道对象
        private ServerSocketChannel acceptChannel = null; //the accept channel
        //选择器对象
        private Selector selector = null; //the selector that we use for the server
        //读取对象数组
        private Reader[] readers = null;
        //当前读取对象索引
        private int currentReader = 0;
        //监听器绑定地址
        private InetSocketAddress address; //the address we bind at
        //等待队列长度
        private int backlogLength = conf.getInt(
            CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
            CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);

        // TODO: 17/3/17 by zmyer
        public Listener() throws IOException {
            //绑定地址
            address = new InetSocketAddress(bindAddress, port);
            // Create a new server socket and set to non blocking mode
            //打开接收链接通道对象
            acceptChannel = ServerSocketChannel.open();
            //设置通道对象非阻塞
            acceptChannel.configureBlocking(false);

            // Bind the server socket to the local host and port
            //套接字绑定地址
            bind(acceptChannel.socket(), address, backlogLength, conf, portRangeConfig);
            //获取接收套接字本地端口
            port = acceptChannel.socket().getLocalPort(); //Could be an ephemeral port
            // create a selector;
            //打开选择器
            selector = Selector.open();
            //创建读取对象数组
            readers = new Reader[readThreads];
            for (int i = 0; i < readThreads; i++) {
                //创建读取对象
                Reader reader = new Reader("Socket Reader #" + (i + 1) + " for port " + port);
                //注册读取对象
                readers[i] = reader;
                //启动读取对象
                reader.start();
            }

            // Register accepts on the server socket with the selector.
            //开始注册接收链接通道
            acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
            this.setName("IPC Server listener on " + port);
            this.setDaemon(true);
        }

        // TODO: 17/3/17 by zmyer
        private class Reader extends Thread {
            //挂起的链接队列
            final private BlockingQueue<Connection> pendingConnections;
            //读取选择器对象
            private final Selector readSelector;

            // TODO: 17/3/17 by zmyer
            Reader(String name) throws IOException {
                super(name);

                this.pendingConnections = new LinkedBlockingQueue<Connection>(readerPendingConnectionQueue);
                this.readSelector = Selector.open();
            }

            // TODO: 17/3/17 by zmyer
            @Override
            public void run() {
                LOG.info("Starting " + Thread.currentThread().getName());
                try {
                    //循环处理
                    doRunLoop();
                } finally {
                    try {
                        readSelector.close();
                    } catch (IOException ioe) {
                        LOG.error("Error closing read selector in " + Thread.currentThread().getName(), ioe);
                    }
                }
            }

            // TODO: 17/3/18 by zmyer
            private synchronized void doRunLoop() {
                while (running) {
                    SelectionKey key = null;
                    try {
                        // consume as many connections as currently queued to avoid
                        // unbridled acceptance of connections that starves the select
                        //读取挂起的链接数量
                        int size = pendingConnections.size();
                        for (int i = size; i > 0; i--) {
                            //读取挂起的链接对象
                            Connection conn = pendingConnections.take();
                            //将当前的链接对象通道对象注册到读取选择器中
                            conn.channel.register(readSelector, SelectionKey.OP_READ, conn);
                        }
                        //开始启动读取选择器对象
                        readSelector.select();

                        //读取已经就绪的选择键对象
                        Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
                        while (iter.hasNext()) {
                            //读取键
                            key = iter.next();
                            //删除选择器迭代器
                            iter.remove();
                            try {
                                if (key.isReadable()) {
                                    //如果当前的键准备就绪,开始处理读取操作
                                    doRead(key);
                                }
                            } catch (CancelledKeyException cke) {
                                // something else closed the connection, ex. responder or
                                // the listener doing an idle scan.  ignore it and let them
                                // clean up.
                                LOG.info(Thread.currentThread().getName() +
                                    ": connection aborted from " + key.attachment());
                            }
                            key = null;
                        }
                    } catch (InterruptedException e) {
                        if (running) {                      // unexpected -- log it
                            LOG.info(Thread.currentThread().getName() + " unexpectedly interrupted", e);
                        }
                    } catch (IOException ex) {
                        LOG.error("Error in Reader", ex);
                    } catch (Throwable re) {
                        LOG.fatal("Bug in read selector!", re);
                        ExitUtil.terminate(1, "Bug in read selector!");
                    }
                }
            }

            /**
             * Updating the readSelector while it's being used is not thread-safe,
             * so the connection must be queued.  The reader will drain the queue
             * and update its readSelector before performing the next select
             */
            // TODO: 17/3/18 by zmyer
            public void addConnection(Connection conn) throws InterruptedException {
                //向挂起的链接列表中插入链接对象
                pendingConnections.put(conn);
                //唤醒挂起的选择器对象
                readSelector.wakeup();
            }

            // TODO: 17/3/18 by zmyer
            void shutdown() {
                assert !running;
                //唤醒选择器对象
                readSelector.wakeup();
                try {
                    //线程停止
                    super.interrupt();
                    //等待线程结束
                    super.join();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        // TODO: 17/3/18 by zmyer
        @Override
        public void run() {
            LOG.info(Thread.currentThread().getName() + ": starting");
            SERVER.set(Server.this);
            connectionManager.startIdleScan();
            while (running) {
                SelectionKey key = null;
                try {
                    //开启选择器对象
                    getSelector().select();
                    Iterator<SelectionKey> iter = getSelector().selectedKeys().iterator();
                    while (iter.hasNext()) {
                        //读取选择器中的键
                        key = iter.next();
                        iter.remove();
                        try {
                            if (key.isValid()) {
                                if (key.isAcceptable())
                                    //如果当前的键是链接准备完毕键,则接入到接收链接流程
                                    doAccept(key);
                            }
                        } catch (IOException e) {
                        }
                        key = null;
                    }
                } catch (OutOfMemoryError e) {
                    // we can run out of memory if we have too many threads
                    // log the event and sleep for a minute and give
                    // some thread(s) a chance to finish
                    LOG.warn("Out of Memory in server select", e);
                    //关闭当前的链接
                    closeCurrentConnection(key, e);
                    //设置关闭空闲的链接
                    connectionManager.closeIdle(true);
                    try {
                        //等待片刻
                        Thread.sleep(60000);
                    } catch (Exception ie) {
                    }
                } catch (Exception e) {
                    //关闭连接
                    closeCurrentConnection(key, e);
                }
            }
            LOG.info("Stopping " + Thread.currentThread().getName());

            synchronized (this) {
                try {
                    //关闭连接通道
                    acceptChannel.close();
                    selector.close();
                } catch (IOException e) {
                }

                selector = null;
                acceptChannel = null;

                // close all connections
                connectionManager.stopIdleScan();
                connectionManager.closeAll();
            }
        }

        // TODO: 17/3/18 by zmyer
        private void closeCurrentConnection(SelectionKey key, Throwable e) {
            if (key != null) {
                //读取连接对象
                Connection c = (Connection) key.attachment();
                if (c != null) {
                    //关闭连接对象
                    closeConnection(c);
                    c = null;
                }
            }
        }

        // TODO: 17/3/18 by zmyer
        InetSocketAddress getAddress() {
            return (InetSocketAddress) acceptChannel.socket().getLocalSocketAddress();
        }

        // TODO: 17/3/18 by zmyer
        void doAccept(SelectionKey key) throws InterruptedException, IOException, OutOfMemoryError {
            //读取服务器连接套接字对应的通道对象
            ServerSocketChannel server = (ServerSocketChannel) key.channel();
            SocketChannel channel;
            //开始接受连接
            while ((channel = server.accept()) != null) {
                //设置通道对象非阻塞标记
                channel.configureBlocking(false);
                //设置通道对象非延时标记
                channel.socket().setTcpNoDelay(tcpNoDelay);
                //设置通道套接字保持连接
                channel.socket().setKeepAlive(true);

                //获取连接对象读取对象
                Reader reader = getReader();
                //根据通道对象查找对应的连接对象
                Connection c = connectionManager.register(channel);
                // If the connectionManager can't take it, close the connection.
                if (c == null) {
                    if (channel.isOpen()) {
                        //如果连接对象失效,则关闭对应的通道对象
                        IOUtils.cleanup(null, channel);
                    }
                    continue;
                }
                //将接收到的链接对象注册的到对应的选择键中
                key.attach(c);  // so closeCurrentConnection can get the object
                //将该连接对象插入到读取对象中
                reader.addConnection(c);
            }
        }

        // TODO: 17/3/18 by zmyer
        void doRead(SelectionKey key) throws InterruptedException {
            int count;
            //读取连接对象
            Connection c = (Connection) key.attachment();
            if (c == null) {
                return;
            }

            //设置连接时间戳
            c.setLastContact(Time.now());

            try {
                //开始读取对应的连接对象,并开始处理
                count = c.readAndProcess();
            } catch (InterruptedException ieo) {
                LOG.info(Thread.currentThread().getName() + ": readAndProcess caught InterruptedException", ieo);
                throw ieo;
            } catch (Exception e) {
                // Do not log WrappedRpcServerExceptionSuppressed.
                if (!(e instanceof WrappedRpcServerExceptionSuppressed)) {
                    // A WrappedRpcServerException is an exception that has been sent
                    // to the client, so the stacktrace is unnecessary; any other
                    // exceptions are unexpected internal server errors and thus the
                    // stacktrace should be logged.
                    LOG.info(Thread.currentThread().getName() +
                            ": readAndProcess from client " + c.getHostAddress() +
                            " threw exception [" + e + "]",
                        (e instanceof WrappedRpcServerException) ? null : e);
                }
                count = -1; //so that the (count < 0) block is executed
            }
            if (count < 0) {
                //如果读取内容长度小于0,则直接关闭连接
                closeConnection(c);
                c = null;
            } else {
                c.setLastContact(Time.now());
            }
        }

        // TODO: 17/3/18 by zmyer
        synchronized void doStop() {
            if (selector != null) {
                selector.wakeup();
                Thread.yield();
            }
            if (acceptChannel != null) {
                try {
                    //关闭选择器通道对象
                    acceptChannel.socket().close();
                } catch (IOException e) {
                    LOG.info(Thread.currentThread().getName() + ":Exception in closing listener socket. " + e);
                }
            }
            for (Reader r : readers) {
                //关闭reader
                r.shutdown();
            }
        }

        // TODO: 17/3/18 by zmyer
        synchronized Selector getSelector() {
            return selector;
        }

        // The method that will return the next reader to work with
        // Simplistic implementation of round robin for now
        // TODO: 17/3/18 by zmyer
        Reader getReader() {
            //RR方式选择reader对象
            currentReader = (currentReader + 1) % readers.length;
            return readers[currentReader];
        }
    }

    // Sends responses of RPC back to clients.
    // TODO: 17/3/18 by zmyer
    private class Responder extends Thread {
        //写入选择器队形
        private final Selector writeSelector;
        //等待怪气的链接数量
        private int pending;         // connections waiting to register

        final static int PURGE_INTERVAL = 900000; // 15mins

        // TODO: 17/3/18 by zmyer
        Responder() throws IOException {
            this.setName("IPC Server Responder");
            this.setDaemon(true);
            writeSelector = Selector.open(); // create a selector
            pending = 0;
        }

        // TODO: 17/3/18 by zmyer
        @Override
        public void run() {
            LOG.info(Thread.currentThread().getName() + ": starting");
            //设置服务器对象
            SERVER.set(Server.this);
            try {
                //进入到循环流程
                doRunLoop();
            } finally {
                LOG.info("Stopping " + Thread.currentThread().getName());
                try {
                    //关闭写入选择器对象
                    writeSelector.close();
                } catch (IOException ioe) {
                    LOG.error("Couldn't close write selector in " + Thread.currentThread().getName(), ioe);
                }
            }
        }

        // TODO: 17/3/18 by zmyer
        private void doRunLoop() {
            long lastPurgeTime = 0;   // last check for old calls.

            while (running) {
                try {
                    //挂起等待
                    waitPending();     // If a channel is being registered, wait.
                    //开始进行选择流程
                    writeSelector.select(PURGE_INTERVAL);
                    Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
                    while (iter.hasNext()) {
                        //读取准备就绪的通道键对象
                        SelectionKey key = iter.next();
                        //删除指定的迭代器
                        iter.remove();
                        try {
                            if (key.isWritable()) {
                                //如果当前的键对象变为可写入,则开始进行异步写入
                                doAsyncWrite(key);
                            }
                        } catch (CancelledKeyException cke) {
                            // something else closed the connection, ex. reader or the
                            // listener doing an idle scan.  ignore it and let them clean
                            // up
                            //读取rpc call对象
                            RpcCall call = (RpcCall) key.attachment();
                            if (call != null) {
                                LOG.info(Thread.currentThread().getName() +
                                    ": connection aborted from " + call.connection);
                            }
                        } catch (IOException e) {
                            LOG.info(Thread.currentThread().getName() + ": doAsyncWrite threw exception " + e);
                        }
                    }

                    //间隔一段时间进行select操作
                    long now = Time.now();
                    if (now < lastPurgeTime + PURGE_INTERVAL) {
                        continue;
                    }

                    //记录前一次的select操作时间戳
                    lastPurgeTime = now;
                    //
                    // If there were some calls that have not been sent out for a
                    // long time, discard them.
                    //
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Checking for old call responses.");
                    }

                    //call列表
                    ArrayList<RpcCall> calls;

                    // get the list of channels from list of keys.
                    synchronized (writeSelector.keys()) {
                        //读取call列表的数量
                        calls = new ArrayList<RpcCall>(writeSelector.keys().size());
                        iter = writeSelector.keys().iterator();
                        while (iter.hasNext()) {
                            SelectionKey key = iter.next();
                            //读取call对象
                            RpcCall call = (RpcCall) key.attachment();
                            if (call != null && key.channel() == call.connection.channel) {
                                //将符合条件的call对象插入到列表中
                                calls.add(call);
                            }
                        }
                    }

                    for (RpcCall call : calls) {
                        //开始执行每个call对象
                        doPurge(call, now);
                    }
                } catch (OutOfMemoryError e) {
                    //
                    // we can run out of memory if we have too many threads
                    // log the event and sleep for a minute and give
                    // some thread(s) a chance to finish
                    //
                    LOG.warn("Out of Memory in server select", e);
                    try {
                        //等待
                        Thread.sleep(60000);
                    } catch (Exception ignored) {
                    }
                } catch (Exception e) {
                    LOG.warn("Exception in Responder", e);
                }
            }
        }

        // TODO: 17/3/18 by zmyer
        private void doAsyncWrite(SelectionKey key) throws IOException {
            //读取call对象
            RpcCall call = (RpcCall) key.attachment();
            if (call == null) {
                return;
            }
            if (key.channel() != call.connection.channel) {
                throw new IOException("doAsyncWrite: bad channel");
            }

            synchronized (call.connection.responseQueue) {
                //开始处理应答队列
                if (processResponse(call.connection.responseQueue, false)) {
                    try {
                        //清理key
                        key.interestOps(0);
                    } catch (CancelledKeyException e) {
            /* The Listener/reader might have closed the socket.
             * We don't explicitly cancel the key, so not sure if this will
             * ever fire.
             * This warning could be removed.
             */
                        LOG.warn("Exception while changing ops : " + e);
                    }
                }
            }
        }

        //
        // Remove calls that have been pending in the responseQueue
        // for a long time.
        //
        // TODO: 17/3/18 by zmyer
        private void doPurge(RpcCall call, long now) {
            //读取当前call连接对象对应的应答队列
            LinkedList<RpcCall> responseQueue = call.connection.responseQueue;
            synchronized (responseQueue) {
                //读取队列迭代器
                Iterator<RpcCall> iter = responseQueue.listIterator(0);
                while (iter.hasNext()) {
                    //读取call对象
                    call = iter.next();
                    if (now > call.timestamp + PURGE_INTERVAL) {
                        //如果call超时,则关闭对应的连接对象
                        closeConnection(call.connection);
                        break;
                    }
                }
            }
        }

        // Processes one response. Returns true if there are no more pending
        // data for this channel.
        //
        // TODO: 17/3/18 by zmyer
        private boolean processResponse(LinkedList<RpcCall> responseQueue,
            boolean inHandler) throws IOException {
            boolean error = true;
            boolean done = false;       // there is more data for this channel.
            int numElements = 0;
            RpcCall call = null;
            try {
                synchronized (responseQueue) {
                    //
                    // If there are no items for this channel, then we are done
                    //
                    //读取应答队列大小
                    numElements = responseQueue.size();
                    if (numElements == 0) {
                        error = false;
                        return true;              // no more data for this channel.
                    }
                    //
                    // Extract the first call
                    //
                    //取出队列中第一个call对象
                    call = responseQueue.removeFirst();
                    //读取call对应的套接字通道对象
                    SocketChannel channel = call.connection.channel;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(Thread.currentThread().getName() + ": responding to " + call);
                    }
                    //
                    // Send as much data as we can in the non-blocking fashion
                    //
                    //开始向应答通道对象中写入应答消息
                    int numBytes = channelWrite(channel, call.rpcResponse);
                    if (numBytes < 0) {
                        return true;
                    }
                    if (!call.rpcResponse.hasRemaining()) {
                        //Clear out the response buffer so it can be collected
                        //如果应答消息处理完毕,则需要及时清理
                        call.rpcResponse = null;
                        //递减当前的call对象连接对象中需要应答的次数
                        call.connection.decRpcCount();
                        //是否处理完毕
                        done = numElements == 1;
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(Thread.currentThread().getName() + ": responding to " + call
                                + " Wrote " + numBytes + " bytes.");
                        }
                    } else {
                        //
                        // If we were unable to write the entire response out, then
                        // insert in Selector queue.
                        //
                        //如果当前的call对象的应答消息为处理完毕,则继续将其插入到队列首部
                        call.connection.responseQueue.addFirst(call);

                        if (inHandler) {
                            // set the serve time when the response has to be sent later
                            //更新call调用时间戳
                            call.timestamp = Time.now();
                            //递增挂起的call数量
                            incPending();
                            try {
                                // Wakeup the thread blocked on select, only then can the call
                                // to channel.register() complete.
                                //首先唤醒写入选择器对象
                                writeSelector.wakeup();
                                //将当前的call链接对象注册到写入选择器中
                                channel.register(writeSelector, SelectionKey.OP_WRITE, call);
                            } catch (ClosedChannelException e) {
                                //Its ok. channel might be closed else where.
                                done = true;
                            } finally {
                                //递减挂起的call数量
                                decPending();
                            }
                        }
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(Thread.currentThread().getName() + ": responding to " + call
                                + " Wrote partial " + numBytes + " bytes.");
                        }
                    }
                    error = false;              // everything went off well
                }
            } finally {
                if (error && call != null) {
                    LOG.warn(Thread.currentThread().getName() + ", call " + call + ": output error");
                    done = true;               // error. no more data for this channel.
                    closeConnection(call.connection);
                }
            }
            return done;
        }

        //
        // Enqueue a response from the application.
        //
        // TODO: 17/3/18 by zmyer
        void doRespond(RpcCall call) throws IOException {
            synchronized (call.connection.responseQueue) {
                // must only wrap before adding to the responseQueue to prevent
                // postponed responses from being encrypted and sent out of order.
                if (call.connection.useWrap) {
                    wrapWithSasl(call);
                }
                //将待应答的call插入到队列中
                call.connection.responseQueue.addLast(call);
                //如果应答队列中消息为1,则直接进入到处理流程
                if (call.connection.responseQueue.size() == 1) {
                    //开始处理应答消息
                    processResponse(call.connection.responseQueue, true);
                }
            }
        }

        // TODO: 17/3/18 by zmyer
        private synchronized void incPending() {   // call waiting to be enqueued.
            pending++;
        }

        // TODO: 17/3/18 by zmyer
        private synchronized void decPending() { // call done enqueueing.
            pending--;
            //直接发送公告
            notify();
        }

        // TODO: 17/3/18 by zmyer
        private synchronized void waitPending() throws InterruptedException {
            while (pending > 0) {
                //等待
                wait();
            }
        }
    }

    // TODO: 17/3/18 by zmyer
    @InterfaceAudience.Private
    public static enum AuthProtocol {
        NONE(0),
        SASL(-33);

        //callid
        public final int callId;

        AuthProtocol(int callId) {
            this.callId = callId;
        }

        // TODO: 17/3/18 by zmyer
        static AuthProtocol valueOf(int callId) {
            for (AuthProtocol authType : AuthProtocol.values()) {
                if (authType.callId == callId) {
                    return authType;
                }
            }
            return null;
        }
    }

    ;

    /**
     * Wrapper for RPC IOExceptions to be returned to the client.  Used to
     * let exceptions bubble up to top of processOneRpc where the correct
     * callId can be associated with the response.  Also used to prevent
     * unnecessary stack trace logging if it's not an internal server error.
     */
    // TODO: 17/3/18 by zmyer
    @SuppressWarnings("serial")
    private static class WrappedRpcServerException extends RpcServerException {
        private final RpcErrorCodeProto errCode;

        // TODO: 17/3/18 by zmyer
        public WrappedRpcServerException(RpcErrorCodeProto errCode, IOException ioe) {
            super(ioe.toString(), ioe);
            this.errCode = errCode;
        }

        // TODO: 17/3/18 by zmyer
        public WrappedRpcServerException(RpcErrorCodeProto errCode, String message) {
            this(errCode, new RpcServerException(message));
        }

        // TODO: 17/3/18 by zmyer
        @Override
        public RpcErrorCodeProto getRpcErrorCodeProto() {
            return errCode;
        }

        // TODO: 17/3/18 by zmyer
        @Override
        public String toString() {
            return getCause().toString();
        }
    }

    /**
     * A WrappedRpcServerException that is suppressed altogether
     * for the purposes of logging.
     */
    // TODO: 17/3/18 by zmyer
    @SuppressWarnings("serial")
    private static class WrappedRpcServerExceptionSuppressed
        extends WrappedRpcServerException {
        public WrappedRpcServerExceptionSuppressed(
            RpcErrorCodeProto errCode, IOException ioe) {
            super(errCode, ioe);
        }
    }

    /** Reads calls from a connection and queues them for handling. */
    // TODO: 17/3/18 by zmyer
    public class Connection {
        //链接头部是否可读
        private boolean connectionHeaderRead = false; // connection  header is read?
        //链接上下文是否可读
        private boolean connectionContextRead = false; //if connection context that
        //follows connection header is read

        //套接字通道对象
        private SocketChannel channel;
        //数据缓冲区
        private ByteBuffer data;
        //数据长度缓冲区
        private ByteBuffer dataLengthBuffer;
        //应答队列
        private LinkedList<RpcCall> responseQueue;
        // number of outstanding rpcs
        //rpc的统计次数
        private AtomicInteger rpcCount = new AtomicInteger();
        private long lastContact;
        //数据长度
        private int dataLength;
        //套接字对象
        private Socket socket;
        // Cache the remote host & port info so that even if the socket is
        // disconnected, we can say where it used to connect to.
        //主机地址
        private String hostAddress;
        //远程端口
        private int remotePort;
        //远程地址
        private InetAddress addr;

        IpcConnectionContextProto connectionContext;
        //协议名称
        String protocolName;
        //sasl服务器
        SaslServer saslServer;
        //认证函数
        private AuthMethod authMethod;
        //认证协议
        private AuthProtocol authProtocol;
        private boolean saslContextEstablished;
        //连接头部缓冲区
        private ByteBuffer connectionHeaderBuf = null;
        //原始数据缓冲区
        private ByteBuffer unwrappedData;
        //原始数据长度缓冲区
        private ByteBuffer unwrappedDataLengthBuffer;
        //服务类对象
        private int serviceClass;

        UserGroupInformation user = null;
        public UserGroupInformation attemptingUser = null; // user name before auth

        // Fake 'call' for failed authorization response
        //认证失败的call对象
        private final RpcCall authFailedCall = new RpcCall(this, AUTHORIZATION_FAILED_CALL_ID);

        private boolean sentNegotiate = false;
        private boolean useWrap = false;

        // TODO: 17/3/18 by zmyer
        public Connection(SocketChannel channel, long lastContact) {
            this.channel = channel;
            this.lastContact = lastContact;
            this.data = null;

            // the buffer is initialized to read the "hrpc" and after that to read
            // the length of the Rpc-packet (i.e 4 bytes)
            this.dataLengthBuffer = ByteBuffer.allocate(4);
            this.unwrappedData = null;
            this.unwrappedDataLengthBuffer = ByteBuffer.allocate(4);
            this.socket = channel.socket();
            this.addr = socket.getInetAddress();
            if (addr == null) {
                this.hostAddress = "*Unknown*";
            } else {
                this.hostAddress = addr.getHostAddress();
            }
            this.remotePort = socket.getPort();
            this.responseQueue = new LinkedList<RpcCall>();
            if (socketSendBufferSize != 0) {
                try {
                    //设置套接字发送缓冲区大小
                    socket.setSendBufferSize(socketSendBufferSize);
                } catch (IOException e) {
                    LOG.warn("Connection: unable to set socket send buffer size to " +
                        socketSendBufferSize);
                }
            }
        }

        // TODO: 17/3/18 by zmyer
        @Override
        public String toString() {
            return getHostAddress() + ":" + remotePort;
        }

        // TODO: 17/3/18 by zmyer
        public String getHostAddress() {
            return hostAddress;
        }

        // TODO: 17/3/18 by zmyer
        public InetAddress getHostInetAddress() {
            return addr;
        }

        // TODO: 17/3/18 by zmyer
        public void setLastContact(long lastContact) {
            this.lastContact = lastContact;
        }

        // TODO: 17/3/18 by zmyer
        public long getLastContact() {
            return lastContact;
        }

        // TODO: 17/3/18 by zmyer
        public Server getServer() {
            return Server.this;
        }

        // TODO: 17/3/18 by zmyer
        /* Return true if the connection has no outstanding rpc */
        private boolean isIdle() {
            return rpcCount.get() == 0;
        }

        // TODO: 17/3/18 by zmyer
        /* Decrement the outstanding RPC count */
        private void decRpcCount() {
            rpcCount.decrementAndGet();
        }

        // TODO: 17/3/18 by zmyer
        /* Increment the outstanding RPC count */
        private void incRpcCount() {
            rpcCount.incrementAndGet();
        }

        // TODO: 17/3/18 by zmyer
        private UserGroupInformation getAuthorizedUgi(String authorizedId)
            throws InvalidToken, AccessControlException {
            if (authMethod == AuthMethod.TOKEN) {
                TokenIdentifier tokenId = SaslRpcServer.getIdentifier(authorizedId,
                    secretManager);
                UserGroupInformation ugi = tokenId.getUser();
                if (ugi == null) {
                    throw new AccessControlException(
                        "Can't retrieve username from tokenIdentifier.");
                }
                ugi.addTokenIdentifier(tokenId);
                return ugi;
            } else {
                return UserGroupInformation.createRemoteUser(authorizedId, authMethod);
            }
        }

        // TODO: 17/3/18 by zmyer
        private void saslReadAndProcess(RpcWritable.Buffer buffer) throws
            WrappedRpcServerException, IOException, InterruptedException {
            final RpcSaslProto saslMessage =
                getMessage(RpcSaslProto.getDefaultInstance(), buffer);
            switch (saslMessage.getState()) {
                case WRAP: {
                    if (!saslContextEstablished || !useWrap) {
                        throw new WrappedRpcServerException(
                            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                            new SaslException("Server is not wrapping data"));
                    }
                    // loops over decoded data and calls processOneRpc
                    unwrapPacketAndProcessRpcs(saslMessage.getToken().toByteArray());
                    break;
                }
                default:
                    saslProcess(saslMessage);
            }
        }

        /**
         * Some exceptions ({@link RetriableException} and {@link StandbyException})
         * that are wrapped as a cause of parameter e are unwrapped so that they can
         * be sent as the true cause to the client side. In case of
         * {@link InvalidToken} we go one level deeper to get the true cause.
         *
         * @param e the exception that may have a cause we want to unwrap.
         * @return the true cause for some exceptions.
         */
        // TODO: 17/3/18 by zmyer
        private Throwable getTrueCause(IOException e) {
            Throwable cause = e;
            while (cause != null) {
                if (cause instanceof RetriableException) {
                    return cause;
                } else if (cause instanceof StandbyException) {
                    return cause;
                } else if (cause instanceof InvalidToken) {
                    // FIXME: hadoop method signatures are restricting the SASL
                    // callbacks to only returning InvalidToken, but some services
                    // need to throw other exceptions (ex. NN + StandyException),
                    // so for now we'll tunnel the real exceptions via an
                    // InvalidToken's cause which normally is not set
                    if (cause.getCause() != null) {
                        cause = cause.getCause();
                    }
                    return cause;
                }
                cause = cause.getCause();
            }
            return e;
        }

        /**
         * Process saslMessage and send saslResponse back
         *
         * @param saslMessage received SASL message
         * @throws WrappedRpcServerException setup failed due to SASL negotiation failure, premature
         * or invalid connection context, or other state errors. This exception needs to be sent to
         * the client. This exception will wrap {@link RetriableException}, {@link InvalidToken},
         * {@link StandbyException} or {@link SaslException}.
         * @throws IOException if sending reply fails
         * @throws InterruptedException
         */
        // TODO: 17/3/18 by zmyer
        private void saslProcess(RpcSaslProto saslMessage)
            throws WrappedRpcServerException, IOException, InterruptedException {
            if (saslContextEstablished) {
                throw new WrappedRpcServerException(
                    RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                    new SaslException("Negotiation is already complete"));
            }
            RpcSaslProto saslResponse = null;
            try {
                try {
                    //处理sasl消息
                    saslResponse = processSaslMessage(saslMessage);
                } catch (IOException e) {
                    rpcMetrics.incrAuthenticationFailures();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(StringUtils.stringifyException(e));
                    }
                    // attempting user could be null
                    IOException tce = (IOException) getTrueCause(e);
                    AUDITLOG.warn(AUTH_FAILED_FOR + this.toString() + ":"
                        + attemptingUser + " (" + e.getLocalizedMessage()
                        + ") with true cause: (" + tce.getLocalizedMessage() + ")");
                    throw tce;
                }

                if (saslServer != null && saslServer.isComplete()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("SASL server context established. Negotiated QoP is "
                            + saslServer.getNegotiatedProperty(Sasl.QOP));
                    }
                    user = getAuthorizedUgi(saslServer.getAuthorizationID());
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("SASL server successfully authenticated client: " + user);
                    }
                    rpcMetrics.incrAuthenticationSuccesses();
                    AUDITLOG.info(AUTH_SUCCESSFUL_FOR + user);
                    saslContextEstablished = true;
                }
            } catch (WrappedRpcServerException wrse) { // don't re-wrap
                throw wrse;
            } catch (IOException ioe) {
                throw new WrappedRpcServerException(
                    RpcErrorCodeProto.FATAL_UNAUTHORIZED, ioe);
            }
            // send back response if any, may throw IOException
            if (saslResponse != null) {
                //开始处理sasl应答消息
                doSaslReply(saslResponse);
            }
            // do NOT enable wrapping until the last auth response is sent
            if (saslContextEstablished) {
                String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
                // SASL wrapping is only used if the connection has a QOP, and
                // the value is not auth.  ex. auth-int & auth-priv
                useWrap = (qop != null && !"auth".equalsIgnoreCase(qop));
                if (!useWrap) {
                    disposeSasl();
                }
            }
        }

        /**
         * Process a saslMessge.
         *
         * @param saslMessage received SASL message
         * @return the sasl response to send back to client
         * @throws SaslException if authentication or generating response fails, or SASL protocol
         * mixup
         * @throws IOException if a SaslServer cannot be created
         * @throws AccessControlException if the requested authentication type is not supported or
         * trying to re-attempt negotiation.
         * @throws InterruptedException
         */
        // TODO: 17/3/18 by zmyer
        private RpcSaslProto processSaslMessage(RpcSaslProto saslMessage)
            throws SaslException, IOException, AccessControlException,
            InterruptedException {
            final RpcSaslProto saslResponse;
            final SaslState state = saslMessage.getState(); // required
            switch (state) {
                case NEGOTIATE: {
                    if (sentNegotiate) {
                        // FIXME shouldn't this be SaslException?
                        throw new AccessControlException(
                            "Client already attempted negotiation");
                    }
                    //创建sasl应答
                    saslResponse = buildSaslNegotiateResponse();
                    // simple-only server negotiate response is success which client
                    // interprets as switch to simple
                    if (saslResponse.getState() == SaslState.SUCCESS) {
                        switchToSimple();
                    }
                    break;
                }
                case INITIATE: {
                    if (saslMessage.getAuthsCount() != 1) {
                        throw new SaslException("Client mechanism is malformed");
                    }
                    // verify the client requested an advertised authType
                    SaslAuth clientSaslAuth = saslMessage.getAuths(0);
                    if (!negotiateResponse.getAuthsList().contains(clientSaslAuth)) {
                        if (sentNegotiate) {
                            throw new AccessControlException(
                                clientSaslAuth.getMethod() + " authentication is not enabled."
                                    + "  Available:" + enabledAuthMethods);
                        }
                        saslResponse = buildSaslNegotiateResponse();
                        break;
                    }
                    authMethod = AuthMethod.valueOf(clientSaslAuth.getMethod());
                    // abort SASL for SIMPLE auth, server has already ensured that
                    // SIMPLE is a legit option above.  we will send no response
                    if (authMethod == AuthMethod.SIMPLE) {
                        switchToSimple();
                        saslResponse = null;
                        break;
                    }
                    // sasl server for tokens may already be instantiated
                    if (saslServer == null || authMethod != AuthMethod.TOKEN) {
                        //创建sasl服务器
                        saslServer = createSaslServer(authMethod);
                    }
                    //处理sasl toke
                    saslResponse = processSaslToken(saslMessage);
                    break;
                }
                case RESPONSE: {
                    saslResponse = processSaslToken(saslMessage);
                    break;
                }
                default:
                    throw new SaslException("Client sent unsupported state " + state);
            }
            return saslResponse;
        }

        // TODO: 17/3/18 by zmyer
        private RpcSaslProto processSaslToken(RpcSaslProto saslMessage)
            throws SaslException {
            if (!saslMessage.hasToken()) {
                throw new SaslException("Client did not send a token");
            }
            byte[] saslToken = saslMessage.getToken().toByteArray();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Have read input token of size " + saslToken.length
                    + " for processing by saslServer.evaluateResponse()");
            }
            saslToken = saslServer.evaluateResponse(saslToken);
            return buildSaslResponse(
                saslServer.isComplete() ? SaslState.SUCCESS : SaslState.CHALLENGE,
                saslToken);
        }

        // TODO: 17/3/18 by zmyer
        private void switchToSimple() {
            // disable SASL and blank out any SASL server
            authProtocol = AuthProtocol.NONE;
            disposeSasl();
        }

        // TODO: 17/3/18 by zmyer
        private RpcSaslProto buildSaslResponse(SaslState state, byte[] replyToken) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Will send " + state + " token of size "
                    + ((replyToken != null) ? replyToken.length : null)
                    + " from saslServer.");
            }
            RpcSaslProto.Builder response = RpcSaslProto.newBuilder();
            response.setState(state);
            if (replyToken != null) {
                response.setToken(ByteString.copyFrom(replyToken));
            }
            return response.build();
        }

        // TODO: 17/3/18 by zmyer
        private void doSaslReply(Message message) throws IOException {
            final RpcCall saslCall = new RpcCall(this, AuthProtocol.SASL.callId);
            setupResponse(saslCall,
                RpcStatusProto.SUCCESS, null,
                RpcWritable.wrap(message), null, null);
            saslCall.sendResponse();
        }

        // TODO: 17/3/18 by zmyer
        private void doSaslReply(Exception ioe) throws IOException {
            setupResponse(authFailedCall,
                RpcStatusProto.FATAL, RpcErrorCodeProto.FATAL_UNAUTHORIZED,
                null, ioe.getClass().getName(), ioe.getLocalizedMessage());
            authFailedCall.sendResponse();
        }

        // TODO: 17/3/18 by zmyer
        private void disposeSasl() {
            if (saslServer != null) {
                try {
                    saslServer.dispose();
                } catch (SaslException ignored) {
                } finally {
                    saslServer = null;
                }
            }
        }

        // TODO: 17/3/18 by zmyer
        private void checkDataLength(int dataLength) throws IOException {
            if (dataLength < 0) {
                String error = "Unexpected data length " + dataLength +
                    "!! from " + getHostAddress();
                LOG.warn(error);
                throw new IOException(error);
            } else if (dataLength > maxDataLength) {
                String error = "Requested data length " + dataLength +
                    " is longer than maximum configured RPC length " +
                    maxDataLength + ".  RPC came from " + getHostAddress();
                LOG.warn(error);
                throw new IOException(error);
            }
        }

        /**
         * This method reads in a non-blocking fashion from the channel:
         * this method is called repeatedly when data is present in the channel;
         * when it has enough data to process one rpc it processes that rpc.
         *
         * On the first pass, it processes the connectionHeader,
         * connectionContext (an outOfBand RPC) and at most one RPC request that
         * follows that. On future passes it will process at most one RPC request.
         *
         * Quirky things: dataLengthBuffer (4 bytes) is used to read "hrpc" OR
         * rpc request length.
         *
         * @return -1 in case of error, else num bytes read so far
         * @throws WrappedRpcServerException - an exception that has already been sent back to the
         * client that does not require verbose logging by the Listener thread
         * @throws IOException - internal error that should not be returned to client, typically
         * failure to respond to client
         * @throws InterruptedException
         */
        // TODO: 17/3/18 by zmyer
        public int readAndProcess()
            throws WrappedRpcServerException, IOException, InterruptedException {
            while (true) {
                // dataLengthBuffer is used to read "hrpc" or the rpc-packet length
                int count = -1;
                if (dataLengthBuffer.remaining() > 0) {
                    //读取通道对象
                    count = channelRead(channel, dataLengthBuffer);
                    if (count < 0 || dataLengthBuffer.remaining() > 0)
                        return count;
                }

                if (!connectionHeaderRead) {
                    // Every connection is expected to send the header;
                    // so far we read "hrpc" of the connection header.
                    if (connectionHeaderBuf == null) {
                        // for the bytes that follow "hrpc", in the connection header
                        //分配连接头部缓冲区
                        connectionHeaderBuf = ByteBuffer.allocate(HEADER_LEN_AFTER_HRPC_PART);
                    }
                    //读取连接头部信息
                    count = channelRead(channel, connectionHeaderBuf);
                    if (count < 0 || connectionHeaderBuf.remaining() > 0) {
                        return count;
                    }
                    //读取版本号
                    int version = connectionHeaderBuf.get(0);
                    // TODO we should add handler for service class later
                    //设置服务类对象
                    this.setServiceClass(connectionHeaderBuf.get(1));
                    //重置数据长度缓冲区
                    dataLengthBuffer.flip();

                    // Check if it looks like the user is hitting an IPC port
                    // with an HTTP GET - this is a common error, so we can
                    // send back a simple string indicating as much.
                    if (HTTP_GET_BYTES.equals(dataLengthBuffer)) {
                        setupHttpRequestOnIpcPortResponse();
                        return -1;
                    }

                    if (!RpcConstants.HEADER.equals(dataLengthBuffer)
                        || version != CURRENT_VERSION) {
                        //Warning is ok since this is not supposed to happen.
                        LOG.warn("Incorrect header or version mismatch from " +
                            hostAddress + ":" + remotePort +
                            " got version " + version +
                            " expected version " + CURRENT_VERSION);
                        setupBadVersionResponse(version);
                        return -1;
                    }

                    // this may switch us into SIMPLE
                    //初始化认证上下文对象
                    authProtocol = initializeAuthContext(connectionHeaderBuf.get(2));
                    //清理数据长度缓冲区
                    dataLengthBuffer.clear(); // clear to next read rpc packet len
                    connectionHeaderBuf = null;
                    connectionHeaderRead = true;
                    continue; // connection header read, now read  4 bytes rpc packet len
                }

                if (data == null) { // just read 4 bytes -  length of RPC packet
                    dataLengthBuffer.flip();
                    dataLength = dataLengthBuffer.getInt();
                    checkDataLength(dataLength);
                    // Set buffer for reading EXACTLY the RPC-packet length and no more.
                    data = ByteBuffer.allocate(dataLength);
                }
                // Now read the RPC packet
                //开始从通道对象中读取内容
                count = channelRead(channel, data);

                if (data.remaining() == 0) {
                    //清理数据长度缓冲区
                    dataLengthBuffer.clear(); // to read length of future rpc packets
                    //重置数据缓冲区的各类初始化位置
                    data.flip();
                    boolean isHeaderRead = connectionContextRead;
                    //开始处理读取的内容
                    processOneRpc(data);
                    data = null;
                    // the last rpc-request we processed could have simply been the
                    // connectionContext; if so continue to read the first RPC.
                    if (!isHeaderRead) {
                        continue;
                    }
                }
                return count;
            }
        }

        // TODO: 17/3/18 by zmyer
        private AuthProtocol initializeAuthContext(int authType)
            throws IOException {
            //根据认证类型读取对应的认证协议
            AuthProtocol authProtocol = AuthProtocol.valueOf(authType);
            if (authProtocol == null) {
                IOException ioe = new IpcException("Unknown auth protocol:" + authType);
                //返回应答
                doSaslReply(ioe);
                throw ioe;
            }

            boolean isSimpleEnabled = enabledAuthMethods.contains(AuthMethod.SIMPLE);
            switch (authProtocol) {
                case NONE: {
                    // don't reply if client is simple and server is insecure
                    if (!isSimpleEnabled) {
                        IOException ioe = new AccessControlException(
                            "SIMPLE authentication is not enabled."
                                + "  Available:" + enabledAuthMethods);
                        doSaslReply(ioe);
                        throw ioe;
                    }
                    break;
                }
                default: {
                    break;
                }
            }
            //返回认证协议
            return authProtocol;
        }

        /**
         * Process the Sasl's Negotiate request, including the optimization of
         * accelerating token negotiation.
         *
         * @return the response to Negotiate request - the list of enabled authMethods and challenge
         * if the TOKENS are supported.
         * @throws SaslException - if attempt to generate challenge fails.
         * @throws IOException - if it fails to create the SASL server for Tokens
         */
        // TODO: 17/3/18 by zmyer
        private RpcSaslProto buildSaslNegotiateResponse()
            throws InterruptedException, SaslException, IOException {
            RpcSaslProto negotiateMessage = negotiateResponse;
            // accelerate token negotiation by sending initial challenge
            // in the negotiation response
            if (enabledAuthMethods.contains(AuthMethod.TOKEN)) {
                saslServer = createSaslServer(AuthMethod.TOKEN);
                byte[] challenge = saslServer.evaluateResponse(new byte[0]);
                RpcSaslProto.Builder negotiateBuilder =
                    RpcSaslProto.newBuilder(negotiateResponse);
                negotiateBuilder.getAuthsBuilder(0)  // TOKEN is always first
                    .setChallenge(ByteString.copyFrom(challenge));
                negotiateMessage = negotiateBuilder.build();
            }
            sentNegotiate = true;
            return negotiateMessage;
        }

        // TODO: 17/3/18 by zmyer
        private SaslServer createSaslServer(AuthMethod authMethod)
            throws IOException, InterruptedException {
            final Map<String, ?> saslProps =
                saslPropsResolver.getServerProperties(addr);
            return new SaslRpcServer(authMethod).create(this, saslProps, secretManager);
        }

        /**
         * Try to set up the response to indicate that the client version
         * is incompatible with the server. This can contain special-case
         * code to speak enough of past IPC protocols to pass back
         * an exception to the caller.
         *
         * @param clientVersion the version the caller is using
         * @throws IOException
         */
        // TODO: 17/3/18 by zmyer
        private void setupBadVersionResponse(int clientVersion) throws IOException {
            String errMsg = "Server IPC version " + CURRENT_VERSION +
                " cannot communicate with client version " + clientVersion;
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();

            if (clientVersion >= 9) {
                // Versions >>9  understand the normal response
                RpcCall fakeCall = new RpcCall(this, -1);
                setupResponse(fakeCall,
                    RpcStatusProto.FATAL, RpcErrorCodeProto.FATAL_VERSION_MISMATCH,
                    null, VersionMismatch.class.getName(), errMsg);
                fakeCall.sendResponse();
            } else if (clientVersion >= 3) {
                RpcCall fakeCall = new RpcCall(this, -1);
                // Versions 3 to 8 use older response
                setupResponseOldVersionFatal(buffer, fakeCall,
                    null, VersionMismatch.class.getName(), errMsg);

                fakeCall.sendResponse();
            } else if (clientVersion == 2) { // Hadoop 0.18.3
                RpcCall fakeCall = new RpcCall(this, 0);
                DataOutputStream out = new DataOutputStream(buffer);
                out.writeInt(0); // call ID
                out.writeBoolean(true); // error
                WritableUtils.writeString(out, VersionMismatch.class.getName());
                WritableUtils.writeString(out, errMsg);
                fakeCall.setResponse(ByteBuffer.wrap(buffer.toByteArray()));
                fakeCall.sendResponse();
            }
        }

        // TODO: 17/3/18 by zmyer
        private void setupHttpRequestOnIpcPortResponse() throws IOException {
            //创建rpc call对象
            RpcCall fakeCall = new RpcCall(this, 0);
            //创建call的应答对象
            fakeCall.setResponse(ByteBuffer.wrap(
                RECEIVED_HTTP_REQ_RESPONSE.getBytes(StandardCharsets.UTF_8)));
            //发送应答消息
            fakeCall.sendResponse();
        }

        /**
         * Reads the connection context following the connection header
         *
         * @throws WrappedRpcServerException - if the header cannot be deserialized, or the user is
         * not authorized
         */
        // TODO: 17/3/18 by zmyer
        private void processConnectionContext(RpcWritable.Buffer buffer)
            throws WrappedRpcServerException {
            // allow only one connection context during a session
            if (connectionContextRead) {
                throw new WrappedRpcServerException(
                    RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                    "Connection context already processed");
            }

            //创建连接上下文对象
            connectionContext = getMessage(IpcConnectionContextProto.getDefaultInstance(), buffer);
            //读取协议名
            protocolName = connectionContext.hasProtocol() ? connectionContext.getProtocol() : null;

            UserGroupInformation protocolUser = ProtoUtil.getUgi(connectionContext);
            if (authProtocol == AuthProtocol.NONE) {
                user = protocolUser;
            } else {
                // user is authenticated
                user.setAuthenticationMethod(authMethod);
                //Now we check if this is a proxy user case. If the protocol user is
                //different from the 'user', it is a proxy user scenario. However,
                //this is not allowed if user authenticated with DIGEST.
                if ((protocolUser != null)
                    && (!protocolUser.getUserName().equals(user.getUserName()))) {
                    if (authMethod == AuthMethod.TOKEN) {
                        // Not allowed to doAs if token authentication is used
                        throw new WrappedRpcServerException(
                            RpcErrorCodeProto.FATAL_UNAUTHORIZED,
                            new AccessControlException("Authenticated user (" + user
                                + ") doesn't match what the client claims to be ("
                                + protocolUser + ")"));
                    } else {
                        // Effective user can be different from authenticated user
                        // for simple auth or kerberos auth
                        // The user is the real user. Now we create a proxy user
                        UserGroupInformation realUser = user;
                        user = UserGroupInformation.createProxyUser(protocolUser
                            .getUserName(), realUser);
                    }
                }
            }
            //开始进入到认证流程
            authorizeConnection();
            // don't set until after authz because connection isn't established
            connectionContextRead = true;
            if (user != null) {
                connectionManager.incrUserConnections(user.getShortUserName());
            }
        }

        /**
         * Process a wrapped RPC Request - unwrap the SASL packet and process
         * each embedded RPC request
         *
         * @param inBuf - SASL wrapped request of one or more RPCs
         * @throws IOException - SASL packet cannot be unwrapped
         * @throws WrappedRpcServerException - an exception that has already been sent back to the
         * client that does not require verbose logging by the Listener thread
         * @throws InterruptedException
         */
        // TODO: 17/3/18 by zmyer
        private void unwrapPacketAndProcessRpcs(byte[] inBuf)
            throws WrappedRpcServerException, IOException, InterruptedException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Have read input token of size " + inBuf.length
                    + " for processing by saslServer.unwrap()");
            }
            //解封消息
            inBuf = saslServer.unwrap(inBuf, 0, inBuf.length);
            //根据解封的消息创建可读取的通道对象
            ReadableByteChannel ch = Channels.newChannel(new ByteArrayInputStream(inBuf));
            // Read all RPCs contained in the inBuf, even partial ones
            while (true) {
                int count = -1;
                if (unwrappedDataLengthBuffer.remaining() > 0) {
                    //从通道对象中读取数据
                    count = channelRead(ch, unwrappedDataLengthBuffer);
                    if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0)
                        return;
                }

                if (unwrappedData == null) {
                    unwrappedDataLengthBuffer.flip();
                    int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();
                    //分配指定长度的缓冲区
                    unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
                }

                //读取通道对象
                count = channelRead(ch, unwrappedData);
                if (count <= 0 || unwrappedData.remaining() > 0)
                    return;

                if (unwrappedData.remaining() == 0) {
                    unwrappedDataLengthBuffer.clear();
                    unwrappedData.flip();
                    //开始处理读取到的rpc
                    processOneRpc(unwrappedData);
                    unwrappedData = null;
                }
            }
        }

        /**
         * Process one RPC Request from buffer read from socket stream
         * - decode rpc in a rpc-Call
         * - handle out-of-band RPC requests such as the initial connectionContext
         * - A successfully decoded RpcCall will be deposited in RPC-Q and
         * its response will be sent later when the request is processed.
         *
         * Prior to this call the connectionHeader ("hrpc...") has been handled and
         * if SASL then SASL has been established and the buf we are passed
         * has been unwrapped from SASL.
         *
         * @param bb - contains the RPC request header and the rpc request
         * @throws IOException - internal error that should not be returned to client, typically
         * failure to respond to client
         * @throws WrappedRpcServerException - an exception that is sent back to the client in this
         * method and does not require verbose logging by the Listener thread
         * @throws InterruptedException
         */
        // TODO: 17/3/18 by zmyer
        private void processOneRpc(ByteBuffer bb)
            throws IOException, WrappedRpcServerException, InterruptedException {
            int callId = -1;
            //初始重试次数
            int retry = RpcConstants.INVALID_RETRY_COUNT;
            try {
                //rpc请求缓冲区构造器
                final RpcWritable.Buffer buffer = RpcWritable.Buffer.wrap(bb);
                //创建rpc请求头部对象
                final RpcRequestHeaderProto header =
                    getMessage(RpcRequestHeaderProto.getDefaultInstance(), buffer);
                //读取call id
                callId = header.getCallId();
                //读取call重试次数
                retry = header.getRetryCount();
                if (LOG.isDebugEnabled()) {
                    LOG.debug(" got #" + callId);
                }
                //检查rpc头部信息合理性
                checkRpcHeaders(header);

                if (callId < 0) { // callIds typically used during connection setup
                    //call异常处理
                    processRpcOutOfBandRequest(header, buffer);
                } else if (!connectionContextRead) {
                    throw new WrappedRpcServerException(
                        RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, "Connection context not established");
                } else {
                    //开始处理rpc请求
                    processRpcRequest(header, buffer);
                }
            } catch (WrappedRpcServerException wrse) { // inform client of error
                Throwable ioe = wrse.getCause();
                final RpcCall call = new RpcCall(this, callId, retry);
                setupResponse(call,
                    RpcStatusProto.FATAL, wrse.getRpcErrorCodeProto(), null,
                    ioe.getClass().getName(), ioe.getMessage());
                call.sendResponse();
                throw wrse;
            }
        }

        /**
         * Verify RPC header is valid
         *
         * @param header - RPC request header
         * @throws WrappedRpcServerException - header contains invalid values
         */
        // TODO: 17/3/18 by zmyer
        private void checkRpcHeaders(RpcRequestHeaderProto header)
            throws WrappedRpcServerException {
            if (!header.hasRpcOp()) {
                String err = " IPC Server: No rpc op in rpcRequestHeader";
                throw new WrappedRpcServerException(
                    RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
            }
            if (header.getRpcOp() !=
                RpcRequestHeaderProto.OperationProto.RPC_FINAL_PACKET) {
                String err = "IPC Server does not implement rpc header operation" +
                    header.getRpcOp();
                throw new WrappedRpcServerException(
                    RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
            }
            // If we know the rpc kind, get its class so that we can deserialize
            // (Note it would make more sense to have the handler deserialize but
            // we continue with this original design.
            if (!header.hasRpcKind()) {
                String err = " IPC Server: No rpc kind in rpcRequestHeader";
                throw new WrappedRpcServerException(
                    RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
            }
        }

        /**
         * Process an RPC Request
         * - the connection headers and context must have been already read.
         * - Based on the rpcKind, decode the rpcRequest.
         * - A successfully decoded RpcCall will be deposited in RPC-Q and
         * its response will be sent later when the request is processed.
         *
         * @param header - RPC request header
         * @param buffer - stream to request payload
         * @throws WrappedRpcServerException - due to fatal rpc layer issues such as invalid header
         * or deserialization error. In this case a RPC fatal status response will later be sent
         * back to client.
         * @throws InterruptedException
         */
        // TODO: 17/3/18 by zmyer
        private void processRpcRequest(RpcRequestHeaderProto header,
            RpcWritable.Buffer buffer) throws WrappedRpcServerException,
            InterruptedException {
            //rpc请求类对象
            Class<? extends Writable> rpcRequestClass = getRpcRequestWrapper(header.getRpcKind());
            if (rpcRequestClass == null) {
                LOG.warn("Unknown rpc kind " + header.getRpcKind() +
                    " from client " + getHostAddress());
                final String err = "Unknown rpc kind in rpc header" + header.getRpcKind();
                throw new WrappedRpcServerException(RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, err);
            }

            Writable rpcRequest;
            try { //Read the rpc request
                //根据rpc类对象创建类实例
                rpcRequest = buffer.newInstance(rpcRequestClass, conf);
            } catch (Throwable t) { // includes runtime exception from newInstance
                LOG.warn("Unable to read call parameters for client " +
                    getHostAddress() + "on connection protocol " +
                    this.protocolName + " for rpcKind " + header.getRpcKind(), t);
                String err = "IPC server unable to read call parameters: " + t.getMessage();
                throw new WrappedRpcServerException(RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST, err);
            }

            TraceScope traceScope = null;
            if (header.hasTraceInfo()) {
                if (tracer != null) {
                    // If the incoming RPC included tracing info, always continue the
                    // trace
                    SpanId parentSpanId = new SpanId(
                        header.getTraceInfo().getTraceId(),
                        header.getTraceInfo().getParentId());
                    traceScope = tracer.newScope(
                        RpcClientUtil.toTraceName(rpcRequest.toString()),
                        parentSpanId);
                    traceScope.detach();
                }
            }

            CallerContext callerContext = null;
            if (header.hasCallerContext()) {
                //call请求处理上下文对象
                callerContext = new CallerContext.Builder(header.getCallerContext().getContext())
                    .setSignature(header.getCallerContext().getSignature()
                        .toByteArray()).build();
            }

            //创建call对象
            RpcCall call = new RpcCall(this, header.getCallId(),
                header.getRetryCount(), rpcRequest,
                ProtoUtil.convert(header.getRpcKind()),
                header.getClientId().toByteArray(), traceScope, callerContext);

            // Save the priority level assignment by the scheduler
            //设置call优先级
            call.setPriorityLevel(callQueue.getPriorityLevel(call));

            try {
                //将call对象插入到call列表中
                queueCall(call);
            } catch (IOException ioe) {
                throw new WrappedRpcServerException(
                    RpcErrorCodeProto.ERROR_RPC_SERVER, ioe);
            }
            //递增rpc次数
            incRpcCount();  // Increment the rpc count
        }

        /**
         * Establish RPC connection setup by negotiating SASL if required, then
         * reading and authorizing the connection header
         *
         * @param header - RPC header
         * @param buffer - stream to request payload
         * @throws WrappedRpcServerException - setup failed due to SASL negotiation failure,
         * premature or invalid connection context, or other state errors. This exception needs to
         * be sent to the client.
         * @throws IOException - failed to send a response back to the client
         * @throws InterruptedException
         */
        // TODO: 17/3/18 by zmyer
        private void processRpcOutOfBandRequest(RpcRequestHeaderProto header,
            RpcWritable.Buffer buffer) throws WrappedRpcServerException,
            IOException, InterruptedException {
            final int callId = header.getCallId();
            if (callId == CONNECTION_CONTEXT_CALL_ID) {
                // SASL must be established prior to connection context
                if (authProtocol == AuthProtocol.SASL && !saslContextEstablished) {
                    throw new WrappedRpcServerException(
                        RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                        "Connection header sent during SASL negotiation");
                }
                // read and authorize the user
                //在连接上下文中处理请求
                processConnectionContext(buffer);
            } else if (callId == AuthProtocol.SASL.callId) {
                // if client was switched to simple, ignore first SASL message
                if (authProtocol != AuthProtocol.SASL) {
                    throw new WrappedRpcServerException(
                        RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                        "SASL protocol not requested by client");
                }
                //处理sasl请求
                saslReadAndProcess(buffer);
            } else if (callId == PING_CALL_ID) {
                LOG.debug("Received ping message");
            } else {
                throw new WrappedRpcServerException(
                    RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                    "Unknown out of band call #" + callId);
            }
        }

        /**
         * Authorize proxy users to access this server
         *
         * @throws WrappedRpcServerException - user is not allowed to proxy
         */
        // TODO: 17/3/18 by zmyer
        private void authorizeConnection() throws WrappedRpcServerException {
            try {
                // If auth method is TOKEN, the token was obtained by the
                // real user for the effective user, therefore not required to
                // authorize real user. doAs is allowed only for simple or kerberos
                // authentication
                if (user != null && user.getRealUser() != null
                    && (authMethod != AuthMethod.TOKEN)) {
                    ProxyUsers.authorize(user, this.getHostAddress());
                }
                //进入认证流程
                authorize(user, protocolName, getHostInetAddress());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Successfully authorized " + connectionContext);
                }
                //更新认证成功次数
                rpcMetrics.incrAuthorizationSuccesses();
            } catch (AuthorizationException ae) {
                LOG.info("Connection from " + this
                    + " for protocol " + connectionContext.getProtocol()
                    + " is unauthorized for user " + user);
                rpcMetrics.incrAuthorizationFailures();
                throw new WrappedRpcServerException(
                    RpcErrorCodeProto.FATAL_UNAUTHORIZED, ae);
            }
        }

        /**
         * Decode the a protobuf from the given input stream
         *
         * @return Message - decoded protobuf
         * @throws WrappedRpcServerException - deserialization failed
         */
        // TODO: 17/3/18 by zmyer
        @SuppressWarnings("unchecked") <T extends Message> T getMessage(Message message,
            RpcWritable.Buffer buffer) throws WrappedRpcServerException {
            try {
                //解析消息
                return (T) buffer.getValue(message);
            } catch (Exception ioe) {
                //读取协议类对象
                Class<?> protoClass = message.getClass();
                //封装异常对象
                throw new WrappedRpcServerException(
                    RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST,
                    "Error decoding " + protoClass.getSimpleName() + ": " + ioe);
            }
        }

        // TODO: 17/3/18 by zmyer
        private void sendResponse(RpcCall call) throws IOException {
            //处理应答
            responder.doRespond(call);
        }

        /**
         * Get service class for connection
         *
         * @return the serviceClass
         */
        // TODO: 17/3/18 by zmyer
        public int getServiceClass() {
            return serviceClass;
        }

        /**
         * Set service class for connection
         *
         * @param serviceClass the serviceClass to set
         */
        // TODO: 17/3/18 by zmyer
        public void setServiceClass(int serviceClass) {
            this.serviceClass = serviceClass;
        }

        // TODO: 17/3/18 by zmyer
        private synchronized void close() {
            disposeSasl();
            data = null;
            dataLengthBuffer = null;
            if (!channel.isOpen())
                return;
            try {
                //关闭套接字
                socket.shutdownOutput();
            } catch (Exception e) {
                LOG.debug("Ignoring socket shutdown exception", e);
            }
            if (channel.isOpen()) {
                IOUtils.cleanup(null, channel);
            }
            IOUtils.cleanup(null, socket);
        }
    }

    // TODO: 17/3/18 by zmyer
    public void queueCall(Call call) throws IOException, InterruptedException {
        if (!callQueue.isClientBackoffEnabled()) {
            //将call插入到队列中
            callQueue.put(call); // queue the call; maybe blocked here
        } else if (callQueue.shouldBackOff(call) || !callQueue.offer(call)) {
            // If rpc scheduler indicates back off based on performance degradation
            // such as response time or rpc queue is full, we will ask the client
            // to back off by throwing RetriableException. Whether the client will
            // honor RetriableException and retry depends the client and its policy.
            // For example, IPC clients using FailoverOnNetworkExceptionRetry handle
            // RetriableException.
            rpcMetrics.incrClientBackoff();
            throw new RetriableException("Server is too busy.");
        }
    }

    /** Handles queued calls . */
    // TODO: 17/3/18 by zmyer
    private class Handler extends Thread {
        // TODO: 17/3/18 by zmyer
        public Handler(int instanceNumber) {
            this.setDaemon(true);
            this.setName("IPC Server handler " + instanceNumber + " on " + port);
        }

        // TODO: 17/3/18 by zmyer
        @Override
        public void run() {
            LOG.debug(Thread.currentThread().getName() + ": starting");
            //设置服务器对象
            SERVER.set(Server.this);
            while (running) {
                TraceScope traceScope = null;
                try {
                    //从call队列中读取call对象
                    final Call call = callQueue.take(); // pop the queue; maybe blocked here
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(Thread.currentThread().getName() + ": " + call + " for RpcKind " + call.rpcKind);
                    }
                    //设置当前正在处理的call对象
                    CurCall.set(call);
                    if (call.traceScope != null) {
                        call.traceScope.reattach();
                        traceScope = call.traceScope;
                        traceScope.getSpan().addTimelineAnnotation("called");
                    }
                    // always update the current call context
                    //设置call执行上下文对象
                    CallerContext.setCurrent(call.callerContext);
                    UserGroupInformation remoteUser = call.getRemoteUser();
                    if (remoteUser != null) {
                        remoteUser.doAs(call);
                    } else {
                        //开始执行call
                        call.run();
                    }
                } catch (InterruptedException e) {
                    if (running) {                          // unexpected -- log it
                        LOG.info(Thread.currentThread().getName() + " unexpectedly interrupted", e);
                        if (traceScope != null) {
                            traceScope.getSpan().addTimelineAnnotation("unexpectedly interrupted: " +
                                StringUtils.stringifyException(e));
                        }
                    }
                } catch (Exception e) {
                    LOG.info(Thread.currentThread().getName() + " caught an exception", e);
                    if (traceScope != null) {
                        traceScope.getSpan().addTimelineAnnotation("Exception: " +
                            StringUtils.stringifyException(e));
                    }
                } finally {
                    CurCall.set(null);
                    IOUtils.cleanup(LOG, traceScope);
                }
            }
            LOG.debug(Thread.currentThread().getName() + ": exiting");
        }

    }

    // TODO: 17/3/18 by zmyer
    @VisibleForTesting
    void logException(Log logger, Throwable e, Call call) {
        if (exceptionsHandler.isSuppressedLog(e.getClass())) {
            return; // Log nothing.
        }

        final String logMsg = Thread.currentThread().getName() + ", call " + call;
        if (exceptionsHandler.isTerseLog(e.getClass())) {
            // Don't log the whole stack trace. Way too noisy!
            logger.info(logMsg + ": " + e);
        } else if (e instanceof RuntimeException || e instanceof Error) {
            // These exception types indicate something is probably wrong
            // on the server side, as opposed to just a normal exceptional
            // result.
            logger.warn(logMsg, e);
        } else {
            logger.info(logMsg, e);
        }
    }

    // TODO: 17/3/18 by zmyer
    protected Server(String bindAddress, int port,
        Class<? extends Writable> paramClass, int handlerCount,
        Configuration conf)
        throws IOException {
        this(bindAddress, port, paramClass, handlerCount, -1, -1, conf, Integer
            .toString(port), null, null);
    }

    // TODO: 17/3/18 by zmyer
    protected Server(String bindAddress, int port,
        Class<? extends Writable> rpcRequestClass, int handlerCount,
        int numReaders, int queueSizePerHandler, Configuration conf,
        String serverName, SecretManager<? extends TokenIdentifier> secretManager)
        throws IOException {
        this(bindAddress, port, rpcRequestClass, handlerCount, numReaders,
            queueSizePerHandler, conf, serverName, secretManager, null);
    }

    /**
     * Constructs a server listening on the named port and address.  Parameters passed must
     * be of the named class.  The <code>handlerCount</handlerCount> determines
     * the number of handler threads that will be used to process calls.
     * If queueSizePerHandler or numReaders are not -1 they will be used instead of parameters
     * from configuration. Otherwise the configuration will be picked up.
     *
     * If rpcRequestClass is null then the rpcRequestClass must have been
     * registered via {@link #registerProtocolEngine(RPC.RpcKind,
     * Class, RPC.RpcInvoker)}
     * This parameter has been retained for compatibility with existing tests
     * and usage.
     */
    // TODO: 17/3/18 by zmyer
    @SuppressWarnings("unchecked")
    protected Server(String bindAddress, int port,
        Class<? extends Writable> rpcRequestClass, int handlerCount,
        int numReaders, int queueSizePerHandler, Configuration conf,
        String serverName, SecretManager<? extends TokenIdentifier> secretManager,
        String portRangeConfig)
        throws IOException {
        this.bindAddress = bindAddress;
        this.conf = conf;
        this.portRangeConfig = portRangeConfig;
        this.port = port;
        //设置rpc请求类对象
        this.rpcRequestClass = rpcRequestClass;
        this.handlerCount = handlerCount;
        this.socketSendBufferSize = 0;
        //设置数据缓冲区的大小
        this.maxDataLength = conf.getInt(CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH,
            CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
        if (queueSizePerHandler != -1) {
            //设置队列的最大长度
            this.maxQueueSize = handlerCount * queueSizePerHandler;
        } else {
            this.maxQueueSize = handlerCount * conf.getInt(
                CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
                CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT);
        }
        //设置应答的最大长度
        this.maxRespSize = conf.getInt(
            CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY,
            CommonConfigurationKeys.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT);
        if (numReaders != -1) {
            //设置读取线程的数量
            this.readThreads = numReaders;
        } else {
            this.readThreads = conf.getInt(
                CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY,
                CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_DEFAULT);
        }

        ///设置读取对象中挂起连接的数量
        this.readerPendingConnectionQueue = conf.getInt(
            CommonConfigurationKeys.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_KEY,
            CommonConfigurationKeys.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_DEFAULT);

        // Setup appropriate callqueue
        //队列类对象前缀
        final String prefix = getQueueClassPrefix();
        //创建call队列
        this.callQueue = new CallQueueManager<Call>(getQueueClass(prefix, conf),
            getSchedulerClass(prefix, conf),
            getClientBackoffEnable(prefix, conf), maxQueueSize, prefix, conf);

        this.secretManager = (SecretManager<TokenIdentifier>) secretManager;

        //设置认证标记
        this.authorize =
            conf.getBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false);

        // configure supported authentications
        //设置认证函数
        this.enabledAuthMethods = getAuthMethods(secretManager, conf);
        this.negotiateResponse = buildNegotiateResponse(enabledAuthMethods);

        // Start the listener here and let it bind to the port
        //创建监听器
        listener = new Listener();
        //读取监听器端口
        this.port = listener.getAddress().getPort();
        //创建连接管理器
        connectionManager = new ConnectionManager();
        //创建rpc统计对象
        this.rpcMetrics = RpcMetrics.create(this, conf);
        //创建rpc细节统计对象
        this.rpcDetailedMetrics = RpcDetailedMetrics.create(this.port);
        //读取tcp非延时标记
        this.tcpNoDelay = conf.getBoolean(
            CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_KEY,
            CommonConfigurationKeysPublic.IPC_SERVER_TCPNODELAY_DEFAULT);

        this.setLogSlowRPC(conf.getBoolean(
            CommonConfigurationKeysPublic.IPC_SERVER_LOG_SLOW_RPC,
            CommonConfigurationKeysPublic.IPC_SERVER_LOG_SLOW_RPC_DEFAULT));

        // Create the responder here
        //创建应答对象
        responder = new Responder();

        if (secretManager != null || UserGroupInformation.isSecurityEnabled()) {
            SaslRpcServer.init(conf);
            saslPropsResolver = SaslPropertiesResolver.getInstance(conf);
        }

        this.exceptionsHandler.addTerseLoggingExceptions(StandbyException.class);
    }

    // TODO: 17/3/18 by zmyer
    private RpcSaslProto buildNegotiateResponse(List<AuthMethod> authMethods)
        throws IOException {
        RpcSaslProto.Builder negotiateBuilder = RpcSaslProto.newBuilder();
        if (authMethods.contains(AuthMethod.SIMPLE) && authMethods.size() == 1) {
            // SIMPLE-only servers return success in response to negotiate
            negotiateBuilder.setState(SaslState.SUCCESS);
        } else {
            negotiateBuilder.setState(SaslState.NEGOTIATE);
            for (AuthMethod authMethod : authMethods) {
                SaslRpcServer saslRpcServer = new SaslRpcServer(authMethod);
                SaslAuth.Builder builder = negotiateBuilder.addAuthsBuilder()
                    .setMethod(authMethod.toString())
                    .setMechanism(saslRpcServer.mechanism);
                if (saslRpcServer.protocol != null) {
                    builder.setProtocol(saslRpcServer.protocol);
                }
                if (saslRpcServer.serverId != null) {
                    builder.setServerId(saslRpcServer.serverId);
                }
            }
        }
        return negotiateBuilder.build();
    }

    // get the security type from the conf. implicitly include token support
    // if a secret manager is provided, or fail if token is the conf value but
    // there is no secret manager
    // TODO: 17/3/18 by zmyer
    private List<AuthMethod> getAuthMethods(SecretManager<?> secretManager,
        Configuration conf) {
        AuthenticationMethod confAuthenticationMethod =
            SecurityUtil.getAuthenticationMethod(conf);
        List<AuthMethod> authMethods = new ArrayList<AuthMethod>();
        if (confAuthenticationMethod == AuthenticationMethod.TOKEN) {
            if (secretManager == null) {
                throw new IllegalArgumentException(AuthenticationMethod.TOKEN +
                    " authentication requires a secret manager");
            }
        } else if (secretManager != null) {
            LOG.debug(AuthenticationMethod.TOKEN +
                " authentication enabled for secret manager");
            // most preferred, go to the front of the line!
            authMethods.add(AuthenticationMethod.TOKEN.getAuthMethod());
        }
        authMethods.add(confAuthenticationMethod.getAuthMethod());

        LOG.debug("Server accepts auth methods:" + authMethods);
        return authMethods;
    }

    // TODO: 17/3/18 by zmyer
    private void closeConnection(Connection connection) {
        connectionManager.close(connection);
    }

    /**
     * Setup response for the IPC Call.
     *
     * @param call {@link Call} to which we are setting up the response
     * @param status of the IPC call
     * @param rv return value for the IPC Call, if the call was successful
     * @param errorClass error class, if the the call failed
     * @param error error message, if the call failed
     * @throws IOException
     */
    // TODO: 17/3/18 by zmyer
    private void setupResponse(
        RpcCall call, RpcStatusProto status, RpcErrorCodeProto erCode,
        Writable rv, String errorClass, String error)
        throws IOException {
        //rpc应答请求头部构造器
        RpcResponseHeaderProto.Builder headerBuilder = RpcResponseHeaderProto.newBuilder();
        headerBuilder.setClientId(ByteString.copyFrom(call.clientId));
        headerBuilder.setCallId(call.callId);
        headerBuilder.setRetryCount(call.retryCount);
        headerBuilder.setStatus(status);
        headerBuilder.setServerIpcVersionNum(CURRENT_VERSION);

        if (status == RpcStatusProto.SUCCESS) {
            //rpc应答头部对象
            RpcResponseHeaderProto header = headerBuilder.build();
            try {
                //初始化rpc应答头部对象
                setupResponse(call, header, rv);
            } catch (Throwable t) {
                LOG.warn("Error serializing call response for call " + call, t);
                // Call back to same function - this is OK since the
                // buffer is reset at the top, and since status is changed
                // to ERROR it won't infinite loop.
                setupResponse(call, RpcStatusProto.ERROR,
                    RpcErrorCodeProto.ERROR_SERIALIZING_RESPONSE,
                    null, t.getClass().getName(),
                    StringUtils.stringifyException(t));
                return;
            }
        } else { // Rpc Failure
            headerBuilder.setExceptionClassName(errorClass);
            headerBuilder.setErrorMsg(error);
            headerBuilder.setErrorDetail(erCode);
            setupResponse(call, headerBuilder.build(), null);
        }
    }

    // TODO: 17/3/18 by zmyer
    private void setupResponse(RpcCall call,
        RpcResponseHeaderProto header, Writable rv) throws IOException {
        final byte[] response;
        if (rv == null || (rv instanceof RpcWritable.ProtobufWrapper)) {
            response = setupResponseForProtobuf(header, rv);
        } else {
            response = setupResponseForWritable(header, rv);
        }
        if (response.length > maxRespSize) {
            LOG.warn("Large response size " + response.length + " for call "
                + call.toString());
        }
        //设置call的应答请求
        call.setResponse(ByteBuffer.wrap(response));
    }

    // TODO: 17/3/18 by zmyer
    private byte[] setupResponseForWritable(
        RpcResponseHeaderProto header, Writable rv) throws IOException {
        ResponseBuffer buf = responseBuffer.get().reset();
        try {
            RpcWritable.wrap(header).writeTo(buf);
            if (rv != null) {
                RpcWritable.wrap(rv).writeTo(buf);
            }
            return buf.toByteArray();
        } finally {
            // Discard a large buf and reset it back to smaller size
            // to free up heap.
            if (buf.capacity() > maxRespSize) {
                buf.setCapacity(INITIAL_RESP_BUF_SIZE);
            }
        }
    }

    // writing to a pre-allocated array is the most efficient way to construct
    // a protobuf response.
    // TODO: 17/3/18 by zmyer
    private byte[] setupResponseForProtobuf(
        RpcResponseHeaderProto header, Writable rv) throws IOException {
        Message payload = (rv != null)
            ? ((RpcWritable.ProtobufWrapper) rv).getMessage() : null;
        int length = getDelimitedLength(header);
        if (payload != null) {
            length += getDelimitedLength(payload);
        }
        byte[] buf = new byte[length + 4];
        CodedOutputStream cos = CodedOutputStream.newInstance(buf);
        // the stream only supports little endian ints
        cos.writeRawByte((byte) ((length >>> 24) & 0xFF));
        cos.writeRawByte((byte) ((length >>> 16) & 0xFF));
        cos.writeRawByte((byte) ((length >>> 8) & 0xFF));
        cos.writeRawByte((byte) ((length >>> 0) & 0xFF));
        cos.writeRawVarint32(header.getSerializedSize());
        header.writeTo(cos);
        if (payload != null) {
            cos.writeRawVarint32(payload.getSerializedSize());
            payload.writeTo(cos);
        }
        return buf;
    }

    // TODO: 17/3/18 by zmyer
    private static int getDelimitedLength(Message message) {
        //计算消息序列化后的长度
        int length = message.getSerializedSize();
        return length + CodedOutputStream.computeRawVarint32Size(length);
    }

    /**
     * Setup response for the IPC Call on Fatal Error from a
     * client that is using old version of Hadoop.
     * The response is serialized using the previous protocol's response
     * layout.
     *
     * @param response buffer to serialize the response into
     * @param call {@link Call} to which we are setting up the response
     * @param rv return value for the IPC Call, if the call was successful
     * @param errorClass error class, if the the call failed
     * @param error error message, if the call failed
     * @throws IOException
     */
    // TODO: 17/3/18 by zmyer
    private void setupResponseOldVersionFatal(ByteArrayOutputStream response,
        RpcCall call,
        Writable rv, String errorClass, String error)
        throws IOException {
        final int OLD_VERSION_FATAL_STATUS = -1;
        response.reset();
        DataOutputStream out = new DataOutputStream(response);
        out.writeInt(call.callId);                // write call id
        out.writeInt(OLD_VERSION_FATAL_STATUS);   // write FATAL_STATUS
        WritableUtils.writeString(out, errorClass);
        WritableUtils.writeString(out, error);
        call.setResponse(ByteBuffer.wrap(response.toByteArray()));
    }

    // TODO: 17/3/18 by zmyer
    private void wrapWithSasl(RpcCall call) throws IOException {
        if (call.connection.saslServer != null) {
            byte[] token = call.rpcResponse.array();
            // synchronization may be needed since there can be multiple Handler
            // threads using saslServer to wrap responses.
            synchronized (call.connection.saslServer) {
                token = call.connection.saslServer.wrap(token, 0, token.length);
            }
            if (LOG.isDebugEnabled())
                LOG.debug("Adding saslServer wrapped token of size " + token.length
                    + " as call response.");
            // rebuild with sasl header and payload
            RpcResponseHeaderProto saslHeader = RpcResponseHeaderProto.newBuilder()
                .setCallId(AuthProtocol.SASL.callId)
                .setStatus(RpcStatusProto.SUCCESS)
                .build();
            RpcSaslProto saslMessage = RpcSaslProto.newBuilder()
                .setState(SaslState.WRAP)
                .setToken(ByteString.copyFrom(token))
                .build();
            setupResponse(call, saslHeader, RpcWritable.wrap(saslMessage));
        }
    }

    // TODO: 17/3/18 by zmyer
    Configuration getConf() {
        return conf;
    }

    /** Sets the socket buffer size used for responding to RPCs */
    // TODO: 17/3/18 by zmyer
    public void setSocketSendBufSize(int size) {
        this.socketSendBufferSize = size;
    }

    // TODO: 17/3/18 by zmyer
    public void setTracer(Tracer t) {
        this.tracer = t;
    }

    /** Starts the service.  Must be called before any calls will be handled. */
    // TODO: 17/3/18 by zmyer
    public synchronized void start() {
        //启动应答对象
        responder.start();
        //启动监听器
        listener.start();
        //创建handler数组
        handlers = new Handler[handlerCount];

        for (int i = 0; i < handlerCount; i++) {
            //创建handler对象
            handlers[i] = new Handler(i);
            //启动handler
            handlers[i].start();
        }
    }

    /** Stops the service.  No new calls will be handled after this is called. */
    // TODO: 17/3/18 by zmyer
    public synchronized void stop() {
        LOG.info("Stopping server on " + port);
        running = false;
        if (handlers != null) {
            for (int i = 0; i < handlerCount; i++) {
                if (handlers[i] != null) {
                    handlers[i].interrupt();
                }
            }
        }
        listener.interrupt();
        listener.doStop();
        responder.interrupt();
        notifyAll();
        this.rpcMetrics.shutdown();
        this.rpcDetailedMetrics.shutdown();
    }

    /**
     * Wait for the server to be stopped.
     * Does not wait for all subthreads to finish.
     * See {@link #stop()}.
     */
    // TODO: 17/3/18 by zmyer
    public synchronized void join() throws InterruptedException {
        while (running) {
            wait();
        }
    }

    /**
     * Return the socket (ip+port) on which the RPC server is listening to.
     *
     * @return the socket (ip+port) on which the RPC server is listening to.
     */
    // TODO: 17/3/18 by zmyer
    public synchronized InetSocketAddress getListenerAddress() {
        return listener.getAddress();
    }

    /**
     * Called for each call.
     *
     * @deprecated Use  {@link #call(RPC.RpcKind, String, Writable, long)} instead
     */
    // TODO: 17/3/18 by zmyer
    @Deprecated
    public Writable call(Writable param, long receiveTime) throws Exception {
        return call(RPC.RpcKind.RPC_BUILTIN, null, param, receiveTime);
    }

    /** Called for each call. */
    // TODO: 17/3/18 by zmyer
    public abstract Writable call(RPC.RpcKind rpcKind, String protocol,
        Writable param, long receiveTime) throws Exception;

    /**
     * Authorize the incoming client connection.
     *
     * @param user client user
     * @param protocolName - the protocol
     * @param addr InetAddress of incoming connection
     * @throws AuthorizationException when the client isn't authorized to talk the protocol
     */
    // TODO: 17/3/18 by zmyer
    private void authorize(UserGroupInformation user, String protocolName,
        InetAddress addr) throws AuthorizationException {
        if (authorize) {
            if (protocolName == null) {
                throw new AuthorizationException("Null protocol not authorized");
            }
            Class<?> protocol = null;
            try {
                //读取协议类对象
                protocol = getProtocolClass(protocolName, getConf());
            } catch (ClassNotFoundException cfne) {
                throw new AuthorizationException("Unknown protocol: " +
                    protocolName);
            }
            //开始进入到认证流程
            serviceAuthorizationManager.authorize(user, protocol, getConf(), addr);
        }
    }

    /**
     * Get the port on which the IPC Server is listening for incoming connections.
     * This could be an ephemeral port too, in which case we return the real
     * port on which the Server has bound.
     *
     * @return port on which IPC Server is listening
     */
    // TODO: 17/3/18 by zmyer
    public int getPort() {
        return port;
    }

    /**
     * The number of open RPC conections
     *
     * @return the number of open rpc connections
     */
    // TODO: 17/3/18 by zmyer
    public int getNumOpenConnections() {
        return connectionManager.size();
    }

    /**
     * Get the NumOpenConnections/User.
     */
    // TODO: 17/3/18 by zmyer
    public String getNumOpenConnectionsPerUser() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(connectionManager.getUserToConnectionsMap());
        } catch (IOException ignored) {
        }
        return null;
    }

    /**
     * The number of rpc calls in the queue.
     *
     * @return The number of rpc calls in the queue.
     */
    // TODO: 17/3/18 by zmyer
    public int getCallQueueLen() {
        return callQueue.size();
    }

    // TODO: 17/3/18 by zmyer
    public boolean isClientBackoffEnabled() {
        return callQueue.isClientBackoffEnabled();
    }

    // TODO: 17/3/18 by zmyer
    public void setClientBackoffEnabled(boolean value) {
        callQueue.setClientBackoffEnabled(value);
    }

    /**
     * The maximum size of the rpc call queue of this server.
     *
     * @return The maximum size of the rpc call queue.
     */
    // TODO: 17/3/18 by zmyer
    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    /**
     * The number of reader threads for this server.
     *
     * @return The number of reader threads.
     */
    // TODO: 17/3/18 by zmyer
    public int getNumReaders() {
        return readThreads;
    }

    /**
     * When the read or write buffer size is larger than this limit, i/o will be
     * done in chunks of this size. Most RPC requests and responses would be
     * be smaller.
     */
    //缓冲区限制
    private static int NIO_BUFFER_LIMIT = 8 * 1024; //should not be more than 64KB.

    /**
     * This is a wrapper around {@link WritableByteChannel#write(ByteBuffer)}.
     * If the amount of data is large, it writes to channel in smaller chunks.
     * This is to avoid jdk from creating many direct buffers as the size of
     * buffer increases. This also minimizes extra copies in NIO layer
     * as a result of multiple write operations required to write a large
     * buffer.
     *
     * @see WritableByteChannel#write(ByteBuffer)
     */
    // TODO: 17/3/18 by zmyer
    private int channelWrite(WritableByteChannel channel, ByteBuffer buffer) throws IOException {
        //读取目前缓冲区中空闲的大小
        int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
            channel.write(buffer) : channelIO(null, channel, buffer);
        if (count > 0) {
            //更新发送的字节数
            rpcMetrics.incrSentBytes(count);
        }
        return count;
    }

    /**
     * This is a wrapper around {@link ReadableByteChannel#read(ByteBuffer)}.
     * If the amount of data is large, it writes to channel in smaller chunks.
     * This is to avoid jdk from creating many direct buffers as the size of
     * ByteBuffer increases. There should not be any performance degredation.
     *
     * @see ReadableByteChannel#read(ByteBuffer)
     */
    // TODO: 17/3/18 by zmyer
    private int channelRead(ReadableByteChannel channel, ByteBuffer buffer) throws IOException {

        int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
            channel.read(buffer) : channelIO(channel, null, buffer);
        if (count > 0) {
            rpcMetrics.incrReceivedBytes(count);
        }
        return count;
    }

    /**
     * Helper for {@link #channelRead(ReadableByteChannel, ByteBuffer)}
     * and {@link #channelWrite(WritableByteChannel, ByteBuffer)}. Only
     * one of readCh or writeCh should be non-null.
     *
     * @see #channelRead(ReadableByteChannel, ByteBuffer)
     * @see #channelWrite(WritableByteChannel, ByteBuffer)
     */
    // TODO: 17/3/18 by zmyer
    private static int channelIO(ReadableByteChannel readCh,
        WritableByteChannel writeCh, ByteBuffer buf) throws IOException {

        int originalLimit = buf.limit();
        int initialRemaining = buf.remaining();
        int ret = 0;

        while (buf.remaining() > 0) {
            try {
                int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
                buf.limit(buf.position() + ioSize);

                ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf);

                if (ret < ioSize) {
                    break;
                }

            } finally {
                buf.limit(originalLimit);
            }
        }

        int nBytes = initialRemaining - buf.remaining();
        return (nBytes > 0) ? nBytes : ret;
    }

    // TODO: 17/3/18 by zmyer
    private class ConnectionManager {
        //连接数目
        final private AtomicInteger count = new AtomicInteger();
        //连接集合
        final private Set<Connection> connections;
        /* Map to maintain the statistics per User */
        //用户与连接对象映射表
        final private Map<String, Integer> userToConnectionsMap;
        final private Object userToConnectionsMapLock = new Object();

        final private Timer idleScanTimer;
        final private int idleScanThreshold;
        final private int idleScanInterval;
        //最大的空闲时间时间间隔
        final private int maxIdleTime;
        //最大空闲关闭时间间隔
        final private int maxIdleToClose;
        //最大的链接数目
        final private int maxConnections;

        // TODO: 17/3/18 by zmyer
        ConnectionManager() {
            this.idleScanTimer = new Timer(
                "IPC Server idle connection scanner for port " + getPort(), true);
            this.idleScanThreshold = conf.getInt(
                CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY,
                CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_DEFAULT);
            this.idleScanInterval = conf.getInt(
                CommonConfigurationKeys.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_KEY,
                CommonConfigurationKeys.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_DEFAULT);
            this.maxIdleTime = 2 * conf.getInt(
                CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
                CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
            this.maxIdleToClose = conf.getInt(
                CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_KEY,
                CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_DEFAULT);
            this.maxConnections = conf.getInt(
                CommonConfigurationKeysPublic.IPC_SERVER_MAX_CONNECTIONS_KEY,
                CommonConfigurationKeysPublic.IPC_SERVER_MAX_CONNECTIONS_DEFAULT);
            // create a set with concurrency -and- a thread-safe iterator, add 2
            // for listener and idle closer threads
            this.connections = Collections.newSetFromMap(
                new ConcurrentHashMap<Connection, Boolean>(
                    maxQueueSize, 0.75f, readThreads + 2));
            this.userToConnectionsMap = new ConcurrentHashMap<>();
        }

        // TODO: 17/3/18 by zmyer
        private boolean add(Connection connection) {
            //将链接对象插入到连接集合中
            boolean added = connections.add(connection);
            if (added) {
                //递增连接数量
                count.getAndIncrement();
            }
            return added;
        }

        // TODO: 17/3/18 by zmyer
        private boolean remove(Connection connection) {
            boolean removed = connections.remove(connection);
            if (removed) {
                count.getAndDecrement();
            }
            return removed;
        }

        // TODO: 17/3/18 by zmyer
        void incrUserConnections(String user) {
            synchronized (userToConnectionsMapLock) {
                Integer count = userToConnectionsMap.get(user);
                if (count == null) {
                    count = 1;
                } else {
                    count++;
                }
                userToConnectionsMap.put(user, count);
            }
        }

        // TODO: 17/3/18 by zmyer
        void decrUserConnections(String user) {
            synchronized (userToConnectionsMapLock) {
                Integer count = userToConnectionsMap.get(user);
                if (count == null) {
                    return;
                } else {
                    count--;
                }
                if (count == 0) {
                    userToConnectionsMap.remove(user);
                } else {
                    userToConnectionsMap.put(user, count);
                }
            }
        }

        // TODO: 17/3/18 by zmyer
        Map<String, Integer> getUserToConnectionsMap() {
            return userToConnectionsMap;
        }

        // TODO: 17/3/18 by zmyer
        int size() {
            return count.get();
        }

        // TODO: 17/3/18 by zmyer
        boolean isFull() {
            // The check is disabled when maxConnections <= 0.
            return ((maxConnections > 0) && (size() >= maxConnections));
        }

        // TODO: 17/3/18 by zmyer
        Connection[] toArray() {
            return connections.toArray(new Connection[0]);
        }

        // TODO: 17/3/18 by zmyer
        Connection register(SocketChannel channel) {
            if (isFull()) {
                return null;
            }
            //创建连接对象
            Connection connection = new Connection(channel, Time.now());
            //将链接对象插入到集合中
            add(connection);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Server connection from " + connection +
                    "; # active connections: " + size() +
                    "; # queued calls: " + callQueue.size());
            }
            return connection;
        }

        // TODO: 17/3/18 by zmyer
        boolean close(Connection connection) {
            //从连接对象中删除连接对象
            boolean exists = remove(connection);
            if (exists) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(Thread.currentThread().getName() +
                        ": disconnecting client " + connection +
                        ". Number of active connections: " + size());
                }
                // only close if actually removed to avoid double-closing due
                // to possible races
                //关闭连接对象
                connection.close();
                // Remove authorized users only
                if (connection.user != null && connection.connectionContextRead) {
                    decrUserConnections(connection.user.getShortUserName());
                }
            }
            return exists;
        }

        // synch'ed to avoid explicit invocation upon OOM from colliding with
        // timer task firing
        // TODO: 17/3/18 by zmyer
        synchronized void closeIdle(boolean scanAll) {
            long minLastContact = Time.now() - maxIdleTime;
            // concurrent iterator might miss new connections added
            // during the iteration, but that's ok because they won't
            // be idle yet anyway and will be caught on next scan
            int closed = 0;
            for (Connection connection : connections) {
                // stop if connections dropped below threshold unless scanning all
                if (!scanAll && size() < idleScanThreshold) {
                    break;
                }
                // stop if not scanning all and max connections are closed
                if (connection.isIdle() &&
                    connection.getLastContact() < minLastContact &&
                    close(connection) &&
                    !scanAll && (++closed == maxIdleToClose)) {
                    break;
                }
            }
        }

        // TODO: 17/3/18 by zmyer
        void closeAll() {
            // use a copy of the connections to be absolutely sure the concurrent
            // iterator doesn't miss a connection
            for (Connection connection : toArray()) {
                //关闭连接对象
                close(connection);
            }
        }

        // TODO: 17/3/18 by zmyer
        void startIdleScan() {
            scheduleIdleScanTask();
        }

        // TODO: 17/3/18 by zmyer
        void stopIdleScan() {
            idleScanTimer.cancel();
        }

        // TODO: 17/3/18 by zmyer
        private void scheduleIdleScanTask() {
            if (!running) {
                return;
            }
            TimerTask idleScanTask = new TimerTask() {
                @Override
                public void run() {
                    if (!running) {
                        return;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(Thread.currentThread().getName() + ": task running");
                    }
                    try {
                        //关闭空闲的链接对象
                        closeIdle(false);
                    } finally {
                        // explicitly reschedule so next execution occurs relative
                        // to the end of this scan, not the beginning
                        //重复调度
                        scheduleIdleScanTask();
                    }
                }
            };
            //定时调用扫描空闲连接任务
            idleScanTimer.schedule(idleScanTask, idleScanInterval);
        }
    }
}
