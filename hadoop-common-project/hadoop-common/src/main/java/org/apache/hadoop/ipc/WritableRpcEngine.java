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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.SocketFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.RPC.RpcInvoker;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Time;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

/** An RpcEngine implementation for Writable data. */
// TODO: 17/3/18 by zmyer
@InterfaceStability.Evolving
@Deprecated
public class WritableRpcEngine implements RpcEngine {
    private static final Log LOG = LogFactory.getLog(RPC.class);

    //writableRpcVersion should be updated if there is a change
    //in format of the rpc messages.

    // 2L - added declared class to Invocation
    public static final long writableRpcVersion = 2L;

    /**
     * Whether or not this class has been initialized.
     */
    //是否初始化
    private static boolean isInitialized = false;

    static {
        //首先需要确保初始化
        ensureInitialized();
    }

    /**
     * Initialize this class if it isn't already.
     */
    // TODO: 17/3/19 by zmyer
    public static synchronized void ensureInitialized() {
        if (!isInitialized) {
            //初始化
            initialize();
        }
    }

    /**
     * Register the rpcRequest deserializer for WritableRpcEngine
     */
    // TODO: 17/3/19 by zmyer
    private static synchronized void initialize() {
        //首先注册rpc引擎
        org.apache.hadoop.ipc.Server.registerProtocolEngine(RPC.RpcKind.RPC_WRITABLE,
            Invocation.class, new Server.WritableRpcInvoker());
        //设置初始化标记
        isInitialized = true;
    }

    /** A method invocation, including the method name and its parameters. */
    // TODO: 17/3/19 by zmyer
    private static class Invocation implements Writable, Configurable {
        //函数名
        private String methodName;
        //参数类集合
        private Class<?>[] parameterClasses;
        //参数集合
        private Object[] parameters;
        //配置文件
        private Configuration conf;
        //客户端版本
        private long clientVersion;
        //客户端函数哈希值
        private int clientMethodsHash;
        //协议名称
        private String declaringClassProtocolName;

        //This could be different from static writableRpcVersion when received
        //at server, if client is using a different version.
        private long rpcVersion;

        // TODO: 17/3/19 by zmyer
        @SuppressWarnings("unused") // called when deserializing an invocation
        public Invocation() {
        }

        // TODO: 17/3/19 by zmyer
        public Invocation(Method method, Object[] parameters) {
            this.methodName = method.getName();
            this.parameterClasses = method.getParameterTypes();
            this.parameters = parameters;
            rpcVersion = writableRpcVersion;
            if (method.getDeclaringClass().equals(VersionedProtocol.class)) {
                //VersionedProtocol is exempted from version check.
                clientVersion = 0;
                clientMethodsHash = 0;
            } else {
                this.clientVersion = RPC.getProtocolVersion(method.getDeclaringClass());
                this.clientMethodsHash = ProtocolSignature.getFingerprint(method
                    .getDeclaringClass().getMethods());
            }
            this.declaringClassProtocolName =
                RPC.getProtocolName(method.getDeclaringClass());
        }

        /** The name of the method invoked. */
        // TODO: 17/3/19 by zmyer
        public String getMethodName() {
            return methodName;
        }

        /** The parameter classes. */
        // TODO: 17/3/19 by zmyer
        public Class<?>[] getParameterClasses() {
            return parameterClasses;
        }

        /** The parameter instances. */
        // TODO: 17/3/19 by zmyer
        public Object[] getParameters() {
            return parameters;
        }

        // TODO: 17/3/19 by zmyer
        private long getProtocolVersion() {
            return clientVersion;
        }

        @SuppressWarnings("unused")
        // TODO: 17/3/19 by zmyer
        private int getClientMethodsHash() {
            return clientMethodsHash;
        }

        /**
         * Returns the rpc version used by the client.
         *
         * @return rpcVersion
         */
        // TODO: 17/3/19 by zmyer
        public long getRpcVersion() {
            return rpcVersion;
        }

        // TODO: 17/3/19 by zmyer
        @Override
        @SuppressWarnings("deprecation")
        public void readFields(DataInput in) throws IOException {
            //读取版本号
            rpcVersion = in.readLong();
            //读取协议名称
            declaringClassProtocolName = UTF8.readString(in);
            //读取函数名称
            methodName = UTF8.readString(in);
            //读取客户端版本号
            clientVersion = in.readLong();
            //读取客户端函数哈希值
            clientMethodsHash = in.readInt();
            //创建保存参数数组
            parameters = new Object[in.readInt()];
            //创建参数类数组
            parameterClasses = new Class[parameters.length];
            ObjectWritable objectWritable = new ObjectWritable();
            for (int i = 0; i < parameters.length; i++) {
                //读取参数
                parameters[i] =
                    ObjectWritable.readObject(in, objectWritable, this.conf);
                //设置参数类型
                parameterClasses[i] = objectWritable.getDeclaredClass();
            }
        }

        // TODO: 17/3/19 by zmyer
        @Override
        @SuppressWarnings("deprecation")
        public void write(DataOutput out) throws IOException {
            //写入版本号
            out.writeLong(rpcVersion);
            //写入协议名称
            UTF8.writeString(out, declaringClassProtocolName);
            //写入函数名称
            UTF8.writeString(out, methodName);
            //写入客户端版本号
            out.writeLong(clientVersion);
            //写入客户端函数哈希
            out.writeInt(clientMethodsHash);
            //写入参数类对象数目
            out.writeInt(parameterClasses.length);
            for (int i = 0; i < parameterClasses.length; i++) {
                //开始写入参数
                ObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
                    conf, true);
            }
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public String toString() {
            StringBuilder buffer = new StringBuilder();
            buffer.append(methodName);
            buffer.append("(");
            for (int i = 0; i < parameters.length; i++) {
                if (i != 0)
                    buffer.append(", ");
                buffer.append(parameters[i]);
            }
            buffer.append(")");
            buffer.append(", rpc version=" + rpcVersion);
            buffer.append(", client version=" + clientVersion);
            buffer.append(", methodsFingerPrint=" + clientMethodsHash);
            return buffer.toString();
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public Configuration getConf() {
            return this.conf;
        }

    }

    //客户端缓存
    private static ClientCache CLIENTS = new ClientCache();

    // TODO: 17/3/19 by zmyer
    private static class Invoker implements RpcInvocationHandler {
        //客户端连接对象id
        private Client.ConnectionId remoteId;
        //客户端对象
        private Client client;
        //是否关闭标记
        private boolean isClosed = false;
        private final AtomicBoolean fallbackToSimpleAuth;

        // TODO: 17/3/19 by zmyer
        public Invoker(Class<?> protocol,
            InetSocketAddress address, UserGroupInformation ticket,
            Configuration conf, SocketFactory factory,
            int rpcTimeout, AtomicBoolean fallbackToSimpleAuth)
            throws IOException {
            //读取远端连接id
            this.remoteId = Client.ConnectionId.getConnectionId(address, protocol,
                ticket, rpcTimeout, null, conf);
            //从客户端缓存中读取客户端
            this.client = CLIENTS.getClient(conf, factory);
            this.fallbackToSimpleAuth = fallbackToSimpleAuth;
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
            long startTime = 0;
            if (LOG.isDebugEnabled()) {
                startTime = Time.now();
            }

            // if Tracing is on then start a new span for this rpc.
            // guard it in the if statement to make sure there isn't
            // any extra string manipulation.
            Tracer tracer = Tracer.curThreadTracer();
            TraceScope traceScope = null;
            if (tracer != null) {
                traceScope = tracer.newScope(RpcClientUtil.methodToTraceString(method));
            }
            ObjectWritable value;
            try {
                //开始使用客户端进行rpc调用
                value = (ObjectWritable)
                    client.call(RPC.RpcKind.RPC_WRITABLE, new Invocation(method, args),
                        remoteId, fallbackToSimpleAuth);
            } finally {
                if (traceScope != null)
                    traceScope.close();
            }
            if (LOG.isDebugEnabled()) {
                long callTime = Time.now() - startTime;
                LOG.debug("Call: " + method.getName() + " " + callTime);
            }
            //返回处理结果
            return value.get();
        }

        /* close the IPC client that's responsible for this invoker's RPCs */
        // TODO: 17/3/19 by zmyer
        @Override
        synchronized public void close() {
            if (!isClosed) {
                //设置成关闭标记
                isClosed = true;
                //关闭客户端
                CLIENTS.stopClient(client);
            }
        }

        // TODO: 17/3/19 by zmyer
        @Override
        public ConnectionId getConnectionId() {
            return remoteId;
        }
    }

    // for unit testing only
    // TODO: 17/3/19 by zmyer
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    static Client getClient(Configuration conf) {
        return CLIENTS.getClient(conf);
    }

    /**
     * Construct a client-side proxy object that implements the named protocol,
     * talking to a server at the named address.
     *
     * @param <T>
     */
    // TODO: 17/3/19 by zmyer
    @Override
    public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
        InetSocketAddress addr, UserGroupInformation ticket,
        Configuration conf, SocketFactory factory,
        int rpcTimeout, RetryPolicy connectionRetryPolicy)
        throws IOException {
        return getProxy(protocol, clientVersion, addr, ticket, conf, factory,
            rpcTimeout, connectionRetryPolicy, null);
    }

    /**
     * Construct a client-side proxy object that implements the named protocol,
     * talking to a server at the named address.
     *
     * @param <T>
     */
    // TODO: 17/3/19 by zmyer
    @Override
    @SuppressWarnings("unchecked")
    public <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
        InetSocketAddress addr, UserGroupInformation ticket,
        Configuration conf, SocketFactory factory,
        int rpcTimeout, RetryPolicy connectionRetryPolicy,
        AtomicBoolean fallbackToSimpleAuth)
        throws IOException {

        if (connectionRetryPolicy != null) {
            throw new UnsupportedOperationException(
                "Not supported: connectionRetryPolicy=" + connectionRetryPolicy);
        }

        //创建代理对象
        T proxy = (T) Proxy.newProxyInstance(protocol.getClassLoader(),
            new Class[] {protocol}, new Invoker(protocol, addr, ticket, conf,
                factory, rpcTimeout, fallbackToSimpleAuth));
        //封装代理对象,返回协议代理
        return new ProtocolProxy<T>(protocol, proxy, true);
    }

    /* Construct a server for a protocol implementation instance listening on a
     * port and address. */
    // TODO: 17/3/19 by zmyer
    @Override
    public RPC.Server getServer(Class<?> protocolClass,
        Object protocolImpl, String bindAddress, int port,
        int numHandlers, int numReaders, int queueSizePerHandler,
        boolean verbose, Configuration conf,
        SecretManager<? extends TokenIdentifier> secretManager,
        String portRangeConfig)
        throws IOException {
        //创建server对象
        return new Server(protocolClass, protocolImpl, conf, bindAddress, port,
            numHandlers, numReaders, queueSizePerHandler, verbose, secretManager,
            portRangeConfig);
    }

    /** An RPC Server. */
    // TODO: 17/3/19 by zmyer
    @Deprecated
    public static class Server extends RPC.Server {
        /**
         * Construct an RPC server.
         *
         * @param instance the instance whose methods will be called
         * @param conf the configuration to use
         * @param bindAddress the address to bind on to listen for connection
         * @param port the port to listen for connections on
         * @deprecated Use #Server(Class, Object, Configuration, String, int)
         */
        // TODO: 17/3/19 by zmyer
        @Deprecated
        public Server(Object instance, Configuration conf, String bindAddress,
            int port) throws IOException {
            this(null, instance, conf, bindAddress, port);
        }

        /**
         * Construct an RPC server.
         *
         * @param protocolClass class
         * @param protocolImpl the instance whose methods will be called
         * @param conf the configuration to use
         * @param bindAddress the address to bind on to listen for connection
         * @param port the port to listen for connections on
         */
        // TODO: 17/3/19 by zmyer
        public Server(Class<?> protocolClass, Object protocolImpl,
            Configuration conf, String bindAddress, int port)
            throws IOException {
            this(protocolClass, protocolImpl, conf, bindAddress, port, 1, -1, -1,
                false, null, null);
        }

        /**
         * Construct an RPC server.
         *
         * @param protocolImpl the instance whose methods will be called
         * @param conf the configuration to use
         * @param bindAddress the address to bind on to listen for connection
         * @param port the port to listen for connections on
         * @param numHandlers the number of method handler threads to run
         * @param verbose whether each call should be logged
         * @deprecated use Server#Server(Class, Object, Configuration, String, int, int, int, int, boolean,
         * SecretManager)
         */
        // TODO: 17/3/19 by zmyer
        @Deprecated
        public Server(Object protocolImpl, Configuration conf, String bindAddress,
            int port, int numHandlers, int numReaders, int queueSizePerHandler,
            boolean verbose, SecretManager<? extends TokenIdentifier> secretManager)
            throws IOException {
            this(null, protocolImpl, conf, bindAddress, port,
                numHandlers, numReaders, queueSizePerHandler, verbose,
                secretManager, null);

        }

        /**
         * Construct an RPC server.
         *
         * @param protocolClass - the protocol being registered can be null for compatibility with old usage (see below
         * for details)
         * @param protocolImpl the protocol impl that will be called
         * @param conf the configuration to use
         * @param bindAddress the address to bind on to listen for connection
         * @param port the port to listen for connections on
         * @param numHandlers the number of method handler threads to run
         * @param verbose whether each call should be logged
         */
        // TODO: 17/3/19 by zmyer
        public Server(Class<?> protocolClass, Object protocolImpl,
            Configuration conf, String bindAddress, int port,
            int numHandlers, int numReaders, int queueSizePerHandler,
            boolean verbose, SecretManager<? extends TokenIdentifier> secretManager,
            String portRangeConfig)
            throws IOException {
            super(bindAddress, port, null, numHandlers, numReaders,
                queueSizePerHandler, conf,
                classNameBase(protocolImpl.getClass().getName()), secretManager,
                portRangeConfig);

            this.verbose = verbose;

            Class<?>[] protocols;
            if (protocolClass == null) { // derive protocol from impl
        /*
         * In order to remain compatible with the old usage where a single
         * target protocolImpl is suppled for all protocol interfaces, and
         * the protocolImpl is derived from the protocolClass(es) 
         * we register all interfaces extended by the protocolImpl
         */
                //读取协议接口集合
                protocols = RPC.getProtocolInterfaces(protocolImpl.getClass());

            } else {
                if (!protocolClass.isAssignableFrom(protocolImpl.getClass())) {
                    throw new IOException("protocolClass " + protocolClass +
                        " is not implemented by protocolImpl which is of class " +
                        protocolImpl.getClass());
                }
                // register protocol class and its super interfaces
                //注册rpc协议对象以及其接口集合
                registerProtocolAndImpl(RPC.RpcKind.RPC_WRITABLE, protocolClass, protocolImpl);
                //读取协议类中所有的接口集合
                protocols = RPC.getProtocolInterfaces(protocolClass);
            }
            for (Class<?> p : protocols) {
                if (!p.equals(VersionedProtocol.class)) {
                    //开始注册rpc协议实现
                    registerProtocolAndImpl(RPC.RpcKind.RPC_WRITABLE, p, protocolImpl);
                }
            }

        }

        // TODO: 17/3/19 by zmyer
        private static void log(String value) {
            if (value != null && value.length() > 55)
                value = value.substring(0, 55) + "...";
            LOG.info(value);
        }

        // TODO: 17/3/19 by zmyer
        @Deprecated
        static class WritableRpcInvoker implements RpcInvoker {

            // TODO: 17/3/19 by zmyer
            @Override
            public Writable call(org.apache.hadoop.ipc.RPC.Server server,
                String protocolName, Writable rpcRequest, long receivedTime)
                throws IOException, RPC.VersionMismatch {

                //转换call对象
                Invocation call = (Invocation) rpcRequest;
                if (server.verbose)
                    log("Call: " + call);

                // Verify writable rpc version
                if (call.getRpcVersion() != writableRpcVersion) {
                    // Client is using a different version of WritableRpc
                    throw new RpcServerException(
                        "WritableRpc version mismatch, client side version="
                            + call.getRpcVersion() + ", server side version="
                            + writableRpcVersion);
                }

                //读取客户端版本
                long clientVersion = call.getProtocolVersion();
                final String protoName;
                ProtoClassProtoImpl protocolImpl;
                if (call.declaringClassProtocolName.equals(VersionedProtocol.class.getName())) {
                    // VersionProtocol methods are often used by client to figure out
                    // which version of protocol to use.
                    //
                    // Versioned protocol methods should go the protocolName protocol
                    // rather than the declaring class of the method since the
                    // the declaring class is VersionedProtocol which is not
                    // registered directly.
                    // Send the call to the highest  protocol version
                    //读取服务器最新的版本实现
                    VerProtocolImpl highest = server.getHighestSupportedProtocol(
                        RPC.RpcKind.RPC_WRITABLE, protocolName);
                    if (highest == null) {
                        throw new RpcServerException("Unknown protocol: " + protocolName);
                    }
                    //设置协议实现类对象
                    protocolImpl = highest.protocolTarget;
                } else {
                    protoName = call.declaringClassProtocolName;

                    // Find the right impl for the protocol based on client version.
                    ProtoNameVer pv =
                        new ProtoNameVer(call.declaringClassProtocolName, clientVersion);
                    //读取协议实现类对象
                    protocolImpl =
                        server.getProtocolImplMap(RPC.RpcKind.RPC_WRITABLE).get(pv);
                    if (protocolImpl == null) { // no match for Protocol AND Version
                        //读取版本协议实现类对象
                        VerProtocolImpl highest =
                            server.getHighestSupportedProtocol(RPC.RpcKind.RPC_WRITABLE,
                                protoName);
                        if (highest == null) {
                            throw new RpcServerException("Unknown protocol: " + protoName);
                        } else { // protocol supported but not the version that client wants
                            throw new RPC.VersionMismatch(protoName, clientVersion,
                                highest.version);
                        }
                    }
                }

                // Invoke the protocol method
                long startTime = Time.now();
                int qTime = (int) (startTime - receivedTime);
                Exception exception = null;
                try {
                    //读取函数对象
                    Method method =
                        protocolImpl.protocolClass.getMethod(call.getMethodName(),
                            call.getParameterClasses());
                    //设置可访问标记
                    method.setAccessible(true);
                    server.rpcDetailedMetrics.init(protocolImpl.protocolClass);
                    //开始调用
                    Object value =
                        method.invoke(protocolImpl.protocolImpl, call.getParameters());
                    if (server.verbose)
                        log("Return: " + value);
                    //返回调用结果
                    return new ObjectWritable(method.getReturnType(), value);

                } catch (InvocationTargetException e) {
                    //获取异常对象
                    Throwable target = e.getTargetException();
                    if (target instanceof IOException) {
                        exception = (IOException) target;
                        throw (IOException) target;
                    } else {
                        IOException ioe = new IOException(target.toString());
                        ioe.setStackTrace(target.getStackTrace());
                        exception = ioe;
                        throw ioe;
                    }
                } catch (Throwable e) {
                    if (!(e instanceof IOException)) {
                        LOG.error("Unexpected throwable object ", e);
                    }
                    IOException ioe = new IOException(e.toString());
                    ioe.setStackTrace(e.getStackTrace());
                    exception = ioe;
                    throw ioe;
                } finally {
                    int processingTime = (int) (Time.now() - startTime);
                    if (LOG.isDebugEnabled()) {
                        String msg = "Served: " + call.getMethodName() +
                            " queueTime= " + qTime + " procesingTime= " + processingTime;
                        if (exception != null) {
                            msg += " exception= " + exception.getClass().getSimpleName();
                        }
                        LOG.debug(msg);
                    }
                    String detailedMetricsName = (exception == null) ?
                        call.getMethodName() :
                        exception.getClass().getSimpleName();
                    server.updateMetrics(detailedMetricsName, qTime, processingTime, false);
                }
            }
        }
    }

    // TODO: 17/3/19 by zmyer
    @Override
    public ProtocolProxy<ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
        ConnectionId connId, Configuration conf, SocketFactory factory)
        throws IOException {
        throw new UnsupportedOperationException("This proxy is not supported");
    }
}
