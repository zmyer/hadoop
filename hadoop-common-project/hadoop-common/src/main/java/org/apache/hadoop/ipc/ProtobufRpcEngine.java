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
import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.SocketFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.RPC.RpcInvoker;
import org.apache.hadoop.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.AsyncGet;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

/**
 * RPC Engine for for protobuf based RPCs.
 */
// TODO: 17/3/19 by zmyer
@InterfaceStability.Evolving
public class ProtobufRpcEngine implements RpcEngine {
    public static final Log LOG = LogFactory.getLog(ProtobufRpcEngine.class);
    //异步返回消息集合
    private static final ThreadLocal<AsyncGet<Message, Exception>>
        ASYNC_RETURN_MESSAGE = new ThreadLocal<>();

    // TODO: 17/3/19 by zmyer
    static { // Register the rpcRequest deserializer for ProtobufRpcEngine
        //开始注册protocolbuffer rpc引擎对象
        org.apache.hadoop.ipc.Server.registerProtocolEngine(
            RPC.RpcKind.RPC_PROTOCOL_BUFFER, RpcProtobufRequest.class,
            new Server.ProtoBufRpcInvoker());
    }

    //客户端缓存对象
    private static final ClientCache CLIENTS = new ClientCache();

    // TODO: 17/3/19 by zmyer
    @Unstable
    public static AsyncGet<Message, Exception> getAsyncReturnMessage() {
        return ASYNC_RETURN_MESSAGE.get();
    }

    // TODO: 17/3/19 by zmyer
    public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
        InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
        SocketFactory factory, int rpcTimeout) throws IOException {
        return getProxy(protocol, clientVersion, addr, ticket, conf, factory,
            rpcTimeout, null);
    }

    // TODO: 17/3/19 by zmyer
    @Override
    public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
        InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
        SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy
    ) throws IOException {
        return getProxy(protocol, clientVersion, addr, ticket, conf, factory,
            rpcTimeout, connectionRetryPolicy, null);
    }

    // TODO: 17/3/19 by zmyer
    @Override
    @SuppressWarnings("unchecked")
    public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
        InetSocketAddress addr, UserGroupInformation ticket, Configuration conf,
        SocketFactory factory, int rpcTimeout, RetryPolicy connectionRetryPolicy,
        AtomicBoolean fallbackToSimpleAuth) throws IOException {

        //首先创建调用对象
        final Invoker invoker = new Invoker(protocol, addr, ticket, conf, factory,
            rpcTimeout, connectionRetryPolicy, fallbackToSimpleAuth);
        //根据创建的调用对象创建具体的服务器代理对象
        return new ProtocolProxy<T>(protocol, (T) Proxy.newProxyInstance(
            protocol.getClassLoader(), new Class[] {protocol}, invoker), false);
    }

    // TODO: 17/3/19 by zmyer
    @Override
    public ProtocolProxy<ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
        ConnectionId connId, Configuration conf, SocketFactory factory)
        throws IOException {
        //读取协议元数据信息
        Class<ProtocolMetaInfoPB> protocol = ProtocolMetaInfoPB.class;
        //根据协议元数据信息创建协议代理对象
        return new ProtocolProxy<ProtocolMetaInfoPB>(protocol,
            (ProtocolMetaInfoPB) Proxy.newProxyInstance(protocol.getClassLoader(),
                new Class[] {protocol}, new Invoker(protocol, connId, conf,
                    factory)), false);
    }

    // TODO: 17/3/19 by zmyer
    private static class Invoker implements RpcInvocationHandler {
        private final Map<String, Message> returnTypes =
            new ConcurrentHashMap<String, Message>();
        //关闭标记
        private boolean isClosed = false;
        //远程连接id
        private final Client.ConnectionId remoteId;
        //客户端对象
        private final Client client;
        //客户端协议版本号
        private final long clientProtocolVersion;
        //协议名称
        private final String protocolName;
        private AtomicBoolean fallbackToSimpleAuth;

        // TODO: 17/3/19 by zmyer
        private Invoker(Class<?> protocol, InetSocketAddress addr,
            UserGroupInformation ticket, Configuration conf, SocketFactory factory,
            int rpcTimeout, RetryPolicy connectionRetryPolicy,
            AtomicBoolean fallbackToSimpleAuth) throws IOException {
            this(protocol, Client.ConnectionId.getConnectionId(
                addr, protocol, ticket, rpcTimeout, connectionRetryPolicy, conf),
                conf, factory);
            this.fallbackToSimpleAuth = fallbackToSimpleAuth;
        }

        /**
         * This constructor takes a connectionId, instead of creating a new one.
         */
        // TODO: 17/3/19 by zmyer
        private Invoker(Class<?> protocol, Client.ConnectionId connId,
            Configuration conf, SocketFactory factory) {
            this.remoteId = connId;
            this.client = CLIENTS.getClient(conf, factory, RpcWritable.Buffer.class);
            this.protocolName = RPC.getProtocolName(protocol);
            this.clientProtocolVersion = RPC.getProtocolVersion(protocol);
        }

        // TODO: 17/3/19 by zmyer
        private RequestHeaderProto constructRpcRequestHeader(Method method) {
            //首先创建rpc请求头部对象
            RequestHeaderProto.Builder builder = RequestHeaderProto.newBuilder();
            //设置函数名称
            builder.setMethodName(method.getName());

            // For protobuf, {@code protocol} used when creating client side proxy is
            // the interface extending BlockingInterface, which has the annotations
            // such as ProtocolName etc.
            //
            // Using Method.getDeclaringClass(), as in WritableEngine to get at
            // the protocol interface will return BlockingInterface, from where
            // the annotation ProtocolName and Version cannot be
            // obtained.
            //
            // Hence we simply use the protocol class used to create the proxy.
            // For PB this may limit the use of mixins on client side.
            //设置协议名称
            builder.setDeclaringClassProtocolName(protocolName);
            //设置协议版本号
            builder.setClientProtocolVersion(clientProtocolVersion);
            //开始创建
            return builder.build();
        }

        /**
         * This is the client side invoker of RPC method. It only throws
         * ServiceException, since the invocation proxy expects only
         * ServiceException to be thrown by the method in case protobuf service.
         *
         * ServiceException has the following causes:
         * <ol>
         * <li>Exceptions encountered on the client side in this method are
         * set as cause in ServiceException as is.</li>
         * <li>Exceptions from the server are wrapped in RemoteException and are
         * set as cause in ServiceException</li>
         * </ol>
         *
         * Note that the client calling protobuf RPC methods, must handle
         * ServiceException by getting the cause from the ServiceException. If the
         * cause is RemoteException, then unwrap it to get the exception thrown by
         * the server.
         */
        // TODO: 17/3/19 by zmyer
        @Override
        public Message invoke(Object proxy, final Method method, Object[] args)
            throws ServiceException {
            long startTime = 0;
            if (LOG.isDebugEnabled()) {
                startTime = Time.now();
            }

            if (args.length != 2) { // RpcController + Message
                throw new ServiceException(
                    "Too many or few parameters for request. Method: ["
                        + method.getName() + "]" + ", Expected: 2, Actual: "
                        + args.length);
            }
            if (args[1] == null) {
                throw new ServiceException("null param while calling Method: ["
                    + method.getName() + "]");
            }

            // if Tracing is on then start a new span for this rpc.
            // guard it in the if statement to make sure there isn't
            // any extra string manipulation.
            Tracer tracer = Tracer.curThreadTracer();
            TraceScope traceScope = null;
            if (tracer != null) {
                traceScope = tracer.newScope(RpcClientUtil.methodToTraceString(method));
            }

            //创建rpc请求头部对象
            RequestHeaderProto rpcRequestHeader = constructRpcRequestHeader(method);

            if (LOG.isTraceEnabled()) {
                LOG.trace(Thread.currentThread().getId() + ": Call -> " +
                    remoteId + ": " + method.getName() +
                    " {" + TextFormat.shortDebugString((Message) args[1]) + "}");
            }

            //读取请求消息
            final Message theRequest = (Message) args[1];
            final RpcWritable.Buffer val;
            try {
                //开始使用客户端发送rpc请求
                val = (RpcWritable.Buffer) client.call(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
                    new RpcProtobufRequest(rpcRequestHeader, theRequest), remoteId,
                    fallbackToSimpleAuth);

            } catch (Throwable e) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace(Thread.currentThread().getId() + ": Exception <- " +
                        remoteId + ": " + method.getName() +
                        " {" + e + "}");
                }
                if (traceScope != null) {
                    traceScope.addTimelineAnnotation("Call got exception: " +
                        e.toString());
                }
                throw new ServiceException(e);
            } finally {
                if (traceScope != null)
                    traceScope.close();
            }

            if (LOG.isDebugEnabled()) {
                long callTime = Time.now() - startTime;
                LOG.debug("Call: " + method.getName() + " took " + callTime + "ms");
            }

            //如果客户端是异步模式
            if (Client.isAsynchronousMode()) {
                //从客户端处读取异步应答对象
                final AsyncGet<RpcWritable.Buffer, IOException> arr
                    = Client.getAsyncRpcResponse();
                //读取异步消息与异常的关系表
                final AsyncGet<Message, Exception> asyncGet
                    = new AsyncGet<Message, Exception>() {
                    @Override
                    public Message get(long timeout, TimeUnit unit) throws Exception {
                        return getReturnMessage(method, arr.get(timeout, unit));
                    }

                    @Override
                    public boolean isDone() {
                        return arr.isDone();
                    }
                };
                //设置异步返回消息
                ASYNC_RETURN_MESSAGE.set(asyncGet);
                return null;
            } else {
                //直接同步返回
                return getReturnMessage(method, val);
            }
        }

        // TODO: 17/3/19 by zmyer
        private Message getReturnMessage(final Method method,
            final RpcWritable.Buffer buf) throws ServiceException {
            Message prototype = null;
            try {
                //首先读取协议类型
                prototype = getReturnProtoType(method);
            } catch (Exception e) {
                throw new ServiceException(e);
            }
            Message returnMessage;
            try {
                //反序列化返回的结果对象
                returnMessage = buf.getValue(prototype.getDefaultInstanceForType());

                if (LOG.isTraceEnabled()) {
                    LOG.trace(Thread.currentThread().getId() + ": Response <- " +
                        remoteId + ": " + method.getName() +
                        " {" + TextFormat.shortDebugString(returnMessage) + "}");
                }

            } catch (Throwable e) {
                throw new ServiceException(e);
            }
            //返回应答消息
            return returnMessage;
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public void close() throws IOException {
            if (!isClosed) {
                //设置关闭标记
                isClosed = true;
                //停止客户端
                CLIENTS.stopClient(client);
            }
        }

        // TODO: 17/3/19 by zmyer
        private Message getReturnProtoType(Method method) throws Exception {
            if (returnTypes.containsKey(method.getName())) {
                return returnTypes.get(method.getName());
            }

            //读取函数的返回值类型
            Class<?> returnType = method.getReturnType();
            //读取函数对象
            Method newInstMethod = returnType.getMethod("getDefaultInstance");
            //设置可访问标记
            newInstMethod.setAccessible(true);
            //调用函数
            Message prototype = (Message) newInstMethod.invoke(null, (Object[]) null);
            //注册函数的返回值类型
            returnTypes.put(method.getName(), prototype);
            return prototype;
        }

        // TODO: 17/3/19 by zmyer
        @Override //RpcInvocationHandler
        public ConnectionId getConnectionId() {
            return remoteId;
        }
    }

    // TODO: 17/3/19 by zmyer
    @VisibleForTesting
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    static Client getClient(Configuration conf) {
        //创建客户端
        return CLIENTS.getClient(conf,
            SocketFactory.getDefault(), RpcWritable.Buffer.class);
    }

    // TODO: 17/3/19 by zmyer
    @Override
    public RPC.Server getServer(Class<?> protocol, Object protocolImpl,
        String bindAddress, int port, int numHandlers, int numReaders,
        int queueSizePerHandler, boolean verbose, Configuration conf,
        SecretManager<? extends TokenIdentifier> secretManager,
        String portRangeConfig)
        throws IOException {
        //创建服务器
        return new Server(protocol, protocolImpl, conf, bindAddress, port,
            numHandlers, numReaders, queueSizePerHandler, verbose, secretManager,
            portRangeConfig);
    }

    // TODO: 17/3/19 by zmyer
    public static class Server extends RPC.Server {

        //当前回调函数
        static final ThreadLocal<ProtobufRpcEngineCallback> currentCallback =
            new ThreadLocal<>();

        //call信息
        static final ThreadLocal<CallInfo> currentCallInfo = new ThreadLocal<>();

        // TODO: 17/3/19 by zmyer
        static class CallInfo {
            //call所在的服务器
            private final RPC.Server server;
            //函数名称
            private final String methodName;

            // TODO: 17/3/19 by zmyer
            public CallInfo(RPC.Server server, String methodName) {
                this.server = server;
                this.methodName = methodName;
            }
        }

        // TODO: 17/3/19 by zmyer
        static class ProtobufRpcEngineCallbackImpl
            implements ProtobufRpcEngineCallback {

            //rpc服务器
            private final RPC.Server server;
            //call对象
            private final Call call;
            //函数名称
            private final String methodName;
            //
            private final long setupTime;

            // TODO: 17/3/19 by zmyer
            public ProtobufRpcEngineCallbackImpl() {
                //设置服务器
                this.server = currentCallInfo.get().server;
                //设置call对象
                this.call = Server.getCurCall().get();
                //读取函数名称
                this.methodName = currentCallInfo.get().methodName;
                this.setupTime = Time.now();
            }

            // TODO: 17/3/19 by zmyer
            @Override
            public void setResponse(Message message) {
                //计算call处理的时间
                long processingTime = Time.now() - setupTime;
                //设置call应答消息
                call.setDeferredResponse(RpcWritable.wrap(message));
                //更新服务器统计信息
                server.updateDeferredMetrics(methodName, processingTime);
            }

            // TODO: 17/3/19 by zmyer
            @Override
            public void error(Throwable t) {
                //计算rpc处理时间
                long processingTime = Time.now() - setupTime;
                String detailedMetricsName = t.getClass().getSimpleName();
                server.updateDeferredMetrics(detailedMetricsName, processingTime);
                call.setDeferredError(t);
            }
        }

        // TODO: 17/3/19 by zmyer
        @InterfaceStability.Unstable
        public static ProtobufRpcEngineCallback registerForDeferredResponse() {
            //创建rpc回调对象
            ProtobufRpcEngineCallback callback = new ProtobufRpcEngineCallbackImpl();
            //初始化当前回调对象
            currentCallback.set(callback);
            //返回回调对象
            return callback;
        }

        /**
         * Construct an RPC server.
         *
         * @param protocolClass the class of protocol
         * @param protocolImpl the protocolImpl whose methods will be called
         * @param conf the configuration to use
         * @param bindAddress the address to bind on to listen for connection
         * @param port the port to listen for connections on
         * @param numHandlers the number of method handler threads to run
         * @param verbose whether each call should be logged
         * @param portRangeConfig A config parameter that can be used to restrict the range of ports
         * used when port is 0 (an ephemeral port)
         */
        // TODO: 17/3/19 by zmyer
        public Server(Class<?> protocolClass, Object protocolImpl,
            Configuration conf, String bindAddress, int port, int numHandlers,
            int numReaders, int queueSizePerHandler, boolean verbose,
            SecretManager<? extends TokenIdentifier> secretManager,
            String portRangeConfig)
            throws IOException {
            super(bindAddress, port, null, numHandlers,
                numReaders, queueSizePerHandler, conf, classNameBase(protocolImpl
                    .getClass().getName()), secretManager, portRangeConfig);
            //
            this.verbose = verbose;
            //注册协议以及相关实现
            registerProtocolAndImpl(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocolClass,
                protocolImpl);
        }

        /**
         * Protobuf invoker for {@link RpcInvoker}
         */
        // TODO: 17/3/18 by zmyer
        static class ProtoBufRpcInvoker implements RpcInvoker {
            private static ProtoClassProtoImpl getProtocolImpl(RPC.Server server,
                String protoName, long clientVersion) throws RpcServerException {
                //创建协议的名称和版本
                ProtoNameVer pv = new ProtoNameVer(protoName, clientVersion);
                //读取协议实现类对象
                ProtoClassProtoImpl impl =
                    server.getProtocolImplMap(RPC.RpcKind.RPC_PROTOCOL_BUFFER).get(pv);
                if (impl == null) { // no match for Protocol AND Version
                    //读取最近协议实现版本
                    VerProtocolImpl highest =
                        server.getHighestSupportedProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
                            protoName);
                    if (highest == null) {
                        //抛出异常
                        throw new RpcNoSuchProtocolException("Unknown protocol: " + protoName);
                    }
                    // protocol supported but not the version that client wants
                    throw new RPC.VersionMismatch(protoName, clientVersion, highest.version);
                }
                //协议实现类对象
                return impl;
            }

            @Override
            /**
             * This is a server side method, which is invoked over RPC. On success
             * the return response has protobuf response payload. On failure, the
             * exception name and the stack trace are returned in the response.
             * See {@link HadoopRpcResponseProto}
             *
             * In this method there three types of exceptions possible and they are
             * returned in response as follows.
             * <ol>
             * <li> Exceptions encountered in this method that are returned
             * as {@link RpcServerException} </li>
             * <li> Exceptions thrown by the service is wrapped in ServiceException.
             * In that this method returns in response the exception thrown by the
             * service.</li>
             * <li> Other exceptions thrown by the service. They are returned as
             * it is.</li>
             * </ol>
             */
            // TODO: 17/3/19 by zmyer
            public Writable call(RPC.Server server, String connectionProtocolName,
                Writable writableRequest, long receiveTime) throws Exception {
                //创建rpc请求对象
                RpcProtobufRequest request = (RpcProtobufRequest) writableRequest;
                //创建rpc请求头部对象
                RequestHeaderProto rpcRequest = request.getRequestHeader();
                //读取rpc请求函数名称
                String methodName = rpcRequest.getMethodName();

                /**
                 * RPCs for a particular interface (ie protocol) are done using a
                 * IPC connection that is setup using rpcProxy.
                 * The rpcProxy's has a declared protocol name that is
                 * sent form client to server at connection time.
                 *
                 * Each Rpc call also sends a protocol name
                 * (called declaringClassprotocolName). This name is usually the same
                 * as the connection protocol name except in some cases.
                 * For example metaProtocols such ProtocolInfoProto which get info
                 * about the protocol reuse the connection but need to indicate that
                 * the actual protocol is different (i.e. the protocol is
                 * ProtocolInfoProto) since they reuse the connection; in this case
                 * the declaringClassProtocolName field is set to the ProtocolInfoProto.
                 */

                //读取协议类名称
                String declaringClassProtoName =
                    rpcRequest.getDeclaringClassProtocolName();
                //读取客户端版本号
                long clientVersion = rpcRequest.getClientProtocolVersion();
                if (server.verbose)
                    LOG.info("Call: connectionProtocolName=" + connectionProtocolName +
                        ", method=" + methodName);

                //创建协议实现类对象
                ProtoClassProtoImpl protocolImpl = getProtocolImpl(server,
                    declaringClassProtoName, clientVersion);
                //读取服务对象
                BlockingService service = (BlockingService) protocolImpl.protocolImpl;
                //从服务对象中读取指定函数的描述信息
                MethodDescriptor methodDescriptor = service.getDescriptorForType()
                    .findMethodByName(methodName);
                if (methodDescriptor == null) {
                    String msg = "Unknown method " + methodName + " called on "
                        + connectionProtocolName + " protocol.";
                    LOG.warn(msg);
                    throw new RpcNoSuchMethodException(msg);
                }
                //读取协议类型
                Message prototype = service.getRequestPrototype(methodDescriptor);
                //读取参数类型
                Message param = request.getValue(prototype);

                Message result;
                long startTime = Time.now();
                int qTime = (int) (startTime - receiveTime);
                Exception exception = null;
                boolean isDeferred = false;
                try {
                    server.rpcDetailedMetrics.init(protocolImpl.protocolClass);
                    //登记当前的call信息
                    currentCallInfo.set(new CallInfo(server, methodName));
                    //开始使用服务对象调用指定的函数
                    result = service.callBlockingMethod(methodDescriptor, null, param);
                    // Check if this needs to be a deferred response,
                    // by checking the ThreadLocal callback being set
                    if (currentCallback.get() != null) {
                        //设置当前的call的应答消息
                        Server.getCurCall().get().deferResponse();
                        isDeferred = true;
                        currentCallback.set(null);
                        return null;
                    }
                } catch (ServiceException e) {
                    exception = (Exception) e.getCause();
                    throw (Exception) e.getCause();
                } catch (Exception e) {
                    exception = e;
                    throw e;
                } finally {
                    currentCallInfo.set(null);
                    int processingTime = (int) (Time.now() - startTime);
                    if (LOG.isDebugEnabled()) {
                        String msg = "Served: " + methodName + (isDeferred ? ", deferred" : "") +
                            ", queueTime= " + qTime + " procesingTime= " + processingTime;
                        if (exception != null) {
                            msg += " exception= " + exception.getClass().getSimpleName();
                        }
                        LOG.debug(msg);
                    }
                    String detailedMetricsName = (exception == null) ?
                        methodName :
                        exception.getClass().getSimpleName();
                    server.updateMetrics(detailedMetricsName, qTime, processingTime,
                        isDeferred);
                }
                return RpcWritable.wrap(result);
            }
        }
    }

    // htrace in the ipc layer creates the span name based on toString()
    // which uses the rpc header.  in the normal case we want to defer decoding
    // the rpc header until needed by the rpc engine.
    // TODO: 17/3/19 by zmyer
    static class RpcProtobufRequest extends RpcWritable.Buffer {
        //rpc请求头部对象
        private volatile RequestHeaderProto requestHeader;
        //rpc请求消息
        private Message payload;

        // TODO: 17/3/19 by zmyer
        public RpcProtobufRequest() {
        }

        // TODO: 17/3/19 by zmyer
        RpcProtobufRequest(RequestHeaderProto header, Message payload) {
            this.requestHeader = header;
            this.payload = payload;
        }

        // TODO: 17/3/19 by zmyer
        RequestHeaderProto getRequestHeader() throws IOException {
            if (getByteBuffer() != null && requestHeader == null) {
                requestHeader = getValue(RequestHeaderProto.getDefaultInstance());
            }
            return requestHeader;
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public void writeTo(ResponseBuffer out) throws IOException {
            //首先写入rpc请求头部对象
            requestHeader.writeDelimitedTo(out);
            if (payload != null) {
                //写入rpc请求消息
                payload.writeDelimitedTo(out);
            }
        }

        // this is used by htrace to name the span.
        // TODO: 17/3/19 by zmyer
        @Override
        public String toString() {
            try {
                RequestHeaderProto header = getRequestHeader();
                return header.getDeclaringClassProtocolName() + "." +
                    header.getMethodName();
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }
}
