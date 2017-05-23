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

import com.google.protobuf.BlockingService;
import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.SocketFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc.Client.ConnectionId;
import org.apache.hadoop.ipc.protobuf.ProtocolInfoProtos.ProtocolInfoService;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;

/**
 * A simple RPC mechanism.
 *
 * A <i>protocol</i> is a Java interface.  All parameters and return types must
 * be one of:
 *
 * <ul> <li>a primitive type, <code>boolean</code>, <code>byte</code>,
 * <code>char</code>, <code>short</code>, <code>int</code>, <code>long</code>,
 * <code>float</code>, <code>double</code>, or <code>void</code>; or</li>
 *
 * <li>a {@link String}; or</li>
 *
 * <li>a {@link Writable}; or</li>
 *
 * <li>an array of the above types</li> </ul>
 *
 * All methods in the protocol should throw only IOException.  No field data of
 * the protocol instance is transmitted.
 */
@InterfaceAudience.LimitedPrivate(value = {"Common", "HDFS", "MapReduce", "Yarn"})
@InterfaceStability.Evolving
// TODO: 17/3/18 by zmyer
public class RPC {
    final static int RPC_SERVICE_CLASS_DEFAULT = 0;

    // TODO: 17/3/18 by zmyer
    public enum RpcKind {
        //内置
        RPC_BUILTIN((short) 1),         // Used for built in calls by tests
        //writable类型
        RPC_WRITABLE((short) 2),        // Use WritableRpcEngine
        //protobuff类型
        RPC_PROTOCOL_BUFFER((short) 3); // Use ProtobufRpcEngine
        final static short MAX_INDEX = RPC_PROTOCOL_BUFFER.value; // used for array size
        private final short value;

        // TODO: 17/3/25 by zmyer
        RpcKind(short val) {
            this.value = val;
        }
    }

    // TODO: 17/3/18 by zmyer
    interface RpcInvoker {
        /**
         * Process a client call on the server side
         *
         * @param server the server within whose context this rpc call is made
         * @param protocol - the protocol name (the class of the client proxy used to make calls to the rpc server.
         * @param rpcRequest - deserialized
         * @param receiveTime time at which the call received (for metrics)
         * @return the call's return
         * @throws IOException
         **/
        // TODO: 17/3/25 by zmyer
        Writable call(Server server, String protocol,
            Writable rpcRequest, long receiveTime) throws Exception;
    }

    static final Log LOG = LogFactory.getLog(RPC.class);

    /**
     * Get all superInterfaces that extend VersionedProtocol
     *
     * @param childInterfaces
     * @return the super interfaces that extend VersionedProtocol
     */
    // TODO: 17/3/18 by zmyer
    static Class<?>[] getSuperInterfaces(Class<?>[] childInterfaces) {
        //接口集合
        List<Class<?>> allInterfaces = new ArrayList<Class<?>>();

        for (Class<?> childInterface : childInterfaces) {
            if (VersionedProtocol.class.isAssignableFrom(childInterface)) {
                //将符合条件的接口插入到集合中
                allInterfaces.add(childInterface);
                //将接口对应的父接口中所有的接口插入到集合中
                allInterfaces.addAll(
                    Arrays.asList(getSuperInterfaces(childInterface.getInterfaces())));
            } else {
                LOG.warn("Interface " + childInterface +
                    " ignored because it does not extend VersionedProtocol");
            }
        }
        //将接口集合转为数组形式
        return allInterfaces.toArray(new Class[allInterfaces.size()]);
    }

    /**
     * Get all interfaces that the given protocol implements or extends
     * which are assignable from VersionedProtocol.
     */
    // TODO: 17/3/18 by zmyer
    static Class<?>[] getProtocolInterfaces(Class<?> protocol) {
        //读取指定类对象的接口集合
        Class<?>[] interfaces = protocol.getInterfaces();
        //读取每个接口对应的父接口
        return getSuperInterfaces(interfaces);
    }

    /**
     * Get the protocol name.
     * If the protocol class has a ProtocolAnnotation, then get the protocol
     * name from the annotation; otherwise the class name is the protocol name.
     */
    // TODO: 17/3/18 by zmyer
    static public String getProtocolName(Class<?> protocol) {
        if (protocol == null) {
            return null;
        }
        //根据annation读取指定的协议信息
        ProtocolInfo anno = protocol.getAnnotation(ProtocolInfo.class);
        return (anno == null) ? protocol.getName() : anno.protocolName();
    }

    /**
     * Get the protocol version from protocol class.
     * If the protocol class has a ProtocolAnnotation,
     * then get the protocol version from the annotation;
     * otherwise get it from the versionID field of the protocol class.
     */
    // TODO: 17/3/18 by zmyer
    static public long getProtocolVersion(Class<?> protocol) {
        if (protocol == null) {
            throw new IllegalArgumentException("Null protocol");
        }
        long version;
        //读取协议信息
        ProtocolInfo anno = protocol.getAnnotation(ProtocolInfo.class);
        if (anno != null) {
            //读取协议版本
            version = anno.protocolVersion();
            if (version != -1)
                return version;
        }
        try {
            //读取协议对象中的版本域
            Field versionField = protocol.getField("versionID");
            //设置域可读
            versionField.setAccessible(true);
            //返回域值
            return versionField.getLong(protocol);
        } catch (NoSuchFieldException | IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }
    }

    // TODO: 17/3/18 by zmyer
    private RPC() {
    }                                  // no public ctor

    // cache of RpcEngines by protocol
    //rpc引擎映射表
    private static final Map<Class<?>, RpcEngine> PROTOCOL_ENGINES = new HashMap<Class<?>, RpcEngine>();

    private static final String ENGINE_PROP = "rpc.engine";

    /**
     * Set a protocol to use a non-default RpcEngine.
     *
     * @param conf configuration to use
     * @param protocol the protocol interface
     * @param engine the RpcEngine impl
     */
    // TODO: 17/3/18 by zmyer
    public static void setProtocolEngine(Configuration conf, Class<?> protocol,
        Class<?> engine) {
        //设置协议引擎
        conf.setClass(ENGINE_PROP + "." + protocol.getName(), engine, RpcEngine.class);
    }

    // return the RpcEngine configured to handle a protocol
    // TODO: 17/3/18 by zmyer
    static synchronized RpcEngine getProtocolEngine(Class<?> protocol, Configuration conf) {
        //根据协议名称,读取rpc引擎对象
        RpcEngine engine = PROTOCOL_ENGINES.get(protocol);
        if (engine == null) {
            //如果当前的rpc引擎没有注册,则需要读取默认的rpc引擎
            Class<?> impl = conf.getClass(ENGINE_PROP + "." + protocol.getName(),
                WritableRpcEngine.class);
            //将读取到的rpc引擎类对象,创建具体的实例
            engine = (RpcEngine) ReflectionUtils.newInstance(impl, conf);
            //将当前的新创建的rpc实例,插入到rpc引擎映射表中
            PROTOCOL_ENGINES.put(protocol, engine);
        }
        //返回rpc引擎
        return engine;
    }

    /**
     * A version mismatch for the RPC protocol.
     */
    // TODO: 17/3/18 by zmyer
    public static class VersionMismatch extends RpcServerException {
        private static final long serialVersionUID = 0;

        //接口名称
        private String interfaceName;
        //客户端版本号
        private long clientVersion;
        //服务器版本号
        private long serverVersion;

        /**
         * Create a version mismatch exception
         *
         * @param interfaceName the name of the protocol mismatch
         * @param clientVersion the client's version of the protocol
         * @param serverVersion the server's version of the protocol
         */
        // TODO: 17/3/18 by zmyer
        public VersionMismatch(String interfaceName, long clientVersion,
            long serverVersion) {
            super("Protocol " + interfaceName + " version mismatch. (client = " +
                clientVersion + ", server = " + serverVersion + ")");
            this.interfaceName = interfaceName;
            this.clientVersion = clientVersion;
            this.serverVersion = serverVersion;
        }

        /**
         * Get the interface name
         *
         * @return the java class name (eg. org.apache.hadoop.mapred.InterTrackerProtocol)
         */
        // TODO: 17/3/18 by zmyer
        public String getInterfaceName() {
            return interfaceName;
        }

        /**
         * Get the client's preferred version
         */
        // TODO: 17/3/18 by zmyer
        public long getClientVersion() {
            return clientVersion;
        }

        /**
         * Get the server's agreed to version.
         */
        // TODO: 17/3/18 by zmyer
        public long getServerVersion() {
            return serverVersion;
        }

        /**
         * get the rpc status corresponding to this exception
         */
        // TODO: 17/3/18 by zmyer
        public RpcStatusProto getRpcStatusProto() {
            return RpcStatusProto.ERROR;
        }

        /**
         * get the detailed rpc status corresponding to this exception
         */
        // TODO: 17/3/18 by zmyer
        public RpcErrorCodeProto getRpcErrorCodeProto() {
            return RpcErrorCodeProto.ERROR_RPC_VERSION_MISMATCH;
        }
    }

    /**
     * Get a proxy connection to a remote server
     *
     * @param protocol protocol class
     * @param clientVersion client version
     * @param addr remote address
     * @param conf configuration to use
     * @return the proxy
     * @throws IOException if the far end through a RemoteException
     */
    // TODO: 17/3/18 by zmyer
    public static <T> T waitForProxy(
        Class<T> protocol,
        long clientVersion,
        InetSocketAddress addr,
        Configuration conf
    ) throws IOException {
        return waitForProtocolProxy(protocol, clientVersion, addr, conf).getProxy();
    }

    /**
     * Get a protocol proxy that contains a proxy connection to a remote server
     * and a set of methods that are supported by the server
     *
     * @param protocol protocol class
     * @param clientVersion client version
     * @param addr remote address
     * @param conf configuration to use
     * @return the protocol proxy
     * @throws IOException if the far end through a RemoteException
     */
    // TODO: 17/3/18 by zmyer
    public static <T> ProtocolProxy<T> waitForProtocolProxy(Class<T> protocol,
        long clientVersion,
        InetSocketAddress addr,
        Configuration conf) throws IOException {
        return waitForProtocolProxy(protocol, clientVersion, addr, conf, Long.MAX_VALUE);
    }

    /**
     * Get a proxy connection to a remote server
     *
     * @param protocol protocol class
     * @param clientVersion client version
     * @param addr remote address
     * @param conf configuration to use
     * @param connTimeout time in milliseconds before giving up
     * @return the proxy
     * @throws IOException if the far end through a RemoteException
     */
    // TODO: 17/3/18 by zmyer
    public static <T> T waitForProxy(Class<T> protocol, long clientVersion,
        InetSocketAddress addr, Configuration conf,
        long connTimeout) throws IOException {
        return waitForProtocolProxy(protocol, clientVersion, addr,
            conf, connTimeout).getProxy();
    }

    /**
     * Get a protocol proxy that contains a proxy connection to a remote server
     * and a set of methods that are supported by the server
     *
     * @param protocol protocol class
     * @param clientVersion client version
     * @param addr remote address
     * @param conf configuration to use
     * @param connTimeout time in milliseconds before giving up
     * @return the protocol proxy
     * @throws IOException if the far end through a RemoteException
     */
    // TODO: 17/3/18 by zmyer
    public static <T> ProtocolProxy<T> waitForProtocolProxy(Class<T> protocol,
        long clientVersion,
        InetSocketAddress addr, Configuration conf,
        long connTimeout) throws IOException {
        return waitForProtocolProxy(protocol, clientVersion, addr, conf,
            getRpcTimeout(conf), null, connTimeout);
    }

    /**
     * Get a proxy connection to a remote server
     *
     * @param protocol protocol class
     * @param clientVersion client version
     * @param addr remote address
     * @param conf configuration to use
     * @param rpcTimeout timeout for each RPC
     * @param timeout time in milliseconds before giving up
     * @return the proxy
     * @throws IOException if the far end through a RemoteException
     */
    // TODO: 17/3/18 by zmyer
    public static <T> T waitForProxy(Class<T> protocol,
        long clientVersion,
        InetSocketAddress addr, Configuration conf,
        int rpcTimeout,
        long timeout) throws IOException {
        return waitForProtocolProxy(protocol, clientVersion, addr,
            conf, rpcTimeout, null, timeout).getProxy();
    }

    /**
     * Get a protocol proxy that contains a proxy connection to a remote server
     * and a set of methods that are supported by the server
     *
     * @param protocol protocol class
     * @param clientVersion client version
     * @param addr remote address
     * @param conf configuration to use
     * @param rpcTimeout timeout for each RPC
     * @param timeout time in milliseconds before giving up
     * @return the proxy
     * @throws IOException if the far end through a RemoteException
     */
    // TODO: 17/3/18 by zmyer
    public static <T> ProtocolProxy<T> waitForProtocolProxy(Class<T> protocol,
        long clientVersion,
        InetSocketAddress addr, Configuration conf,
        int rpcTimeout,
        RetryPolicy connectionRetryPolicy,
        long timeout) throws IOException {
        long startTime = Time.now();
        IOException ioe;
        while (true) {
            try {
                //获取协议代理对象
                return getProtocolProxy(protocol, clientVersion, addr,
                    UserGroupInformation.getCurrentUser(), conf, NetUtils
                        .getDefaultSocketFactory(conf), rpcTimeout, connectionRetryPolicy);
            } catch (ConnectException se) {  // namenode has not been started
                LOG.info("Server at " + addr + " not available yet, Zzzzz...");
                ioe = se;
            } catch (SocketTimeoutException te) {  // namenode is busy
                LOG.info("Problem connecting to server: " + addr);
                ioe = te;
            } catch (NoRouteToHostException nrthe) { // perhaps a VIP is failing over
                LOG.info("No route to host for server: " + addr);
                ioe = nrthe;
            }
            // check if timed out
            if (Time.now() - timeout >= startTime) {
                throw ioe;
            }

            if (Thread.currentThread().isInterrupted()) {
                // interrupted during some IO; this may not have been caught
                throw new InterruptedIOException("Interrupted waiting for the proxy");
            }

            // wait for retry
            try {
                //等待重试
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw (IOException) new InterruptedIOException(
                    "Interrupted waiting for the proxy").initCause(ioe);
            }
        }
    }

    /**
     * Construct a client-side proxy object that implements the named protocol,
     * talking to a server at the named address.
     *
     * @param <T>
     */
    // TODO: 17/3/18 by zmyer
    public static <T> T getProxy(Class<T> protocol,
        long clientVersion,
        InetSocketAddress addr, Configuration conf,
        SocketFactory factory) throws IOException {
        return getProtocolProxy(protocol, clientVersion, addr, conf, factory).getProxy();
    }

    /**
     * Get a protocol proxy that contains a proxy connection to a remote server
     * and a set of methods that are supported by the server
     *
     * @param protocol protocol class
     * @param clientVersion client version
     * @param addr remote address
     * @param conf configuration to use
     * @param factory socket factory
     * @return the protocol proxy
     * @throws IOException if the far end through a RemoteException
     */
    // TODO: 17/3/18 by zmyer
    public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
        long clientVersion,
        InetSocketAddress addr, Configuration conf,
        SocketFactory factory) throws IOException {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        return getProtocolProxy(protocol, clientVersion, addr, ugi, conf, factory);
    }

    /**
     * Construct a client-side proxy object that implements the named protocol,
     * talking to a server at the named address.
     *
     * @param <T>
     */
    // TODO: 17/3/18 by zmyer
    public static <T> T getProxy(Class<T> protocol,
        long clientVersion,
        InetSocketAddress addr,
        UserGroupInformation ticket,
        Configuration conf,
        SocketFactory factory) throws IOException {
        return getProtocolProxy(
            protocol, clientVersion, addr, ticket, conf, factory).getProxy();
    }

    /**
     * Get a protocol proxy that contains a proxy connection to a remote server
     * and a set of methods that are supported by the server
     *
     * @param protocol protocol class
     * @param clientVersion client version
     * @param addr remote address
     * @param ticket user group information
     * @param conf configuration to use
     * @param factory socket factory
     * @return the protocol proxy
     * @throws IOException if the far end through a RemoteException
     */
    // TODO: 17/3/18 by zmyer
    public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
        long clientVersion,
        InetSocketAddress addr,
        UserGroupInformation ticket,
        Configuration conf,
        SocketFactory factory) throws IOException {
        return getProtocolProxy(protocol, clientVersion, addr, ticket, conf,
            factory, getRpcTimeout(conf), null);
    }

    /**
     * Construct a client-side proxy that implements the named protocol,
     * talking to a server at the named address.
     *
     * @param <T>
     * @param protocol protocol
     * @param clientVersion client's version
     * @param addr server address
     * @param ticket security ticket
     * @param conf configuration
     * @param factory socket factory
     * @param rpcTimeout max time for each rpc; 0 means no timeout
     * @return the proxy
     * @throws IOException if any error occurs
     */
    // TODO: 17/3/18 by zmyer
    public static <T> T getProxy(Class<T> protocol,
        long clientVersion,
        InetSocketAddress addr,
        UserGroupInformation ticket,
        Configuration conf,
        SocketFactory factory,
        int rpcTimeout) throws IOException {
        return getProtocolProxy(protocol, clientVersion, addr, ticket,
            conf, factory, rpcTimeout, null).getProxy();
    }

    /**
     * Get a protocol proxy that contains a proxy connection to a remote server
     * and a set of methods that are supported by the server
     *
     * @param protocol protocol
     * @param clientVersion client's version
     * @param addr server address
     * @param ticket security ticket
     * @param conf configuration
     * @param factory socket factory
     * @param rpcTimeout max time for each rpc; 0 means no timeout
     * @param connectionRetryPolicy retry policy
     * @return the proxy
     * @throws IOException if any error occurs
     */
    // TODO: 17/3/18 by zmyer
    public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
        long clientVersion,
        InetSocketAddress addr,
        UserGroupInformation ticket,
        Configuration conf,
        SocketFactory factory,
        int rpcTimeout,
        RetryPolicy connectionRetryPolicy) throws IOException {
        return getProtocolProxy(protocol, clientVersion, addr, ticket,
            conf, factory, rpcTimeout, connectionRetryPolicy, null);
    }

    /**
     * Get a protocol proxy that contains a proxy connection to a remote server
     * and a set of methods that are supported by the server
     *
     * @param protocol protocol
     * @param clientVersion client's version
     * @param addr server address
     * @param ticket security ticket
     * @param conf configuration
     * @param factory socket factory
     * @param rpcTimeout max time for each rpc; 0 means no timeout
     * @param connectionRetryPolicy retry policy
     * @param fallbackToSimpleAuth set to true or false during calls to indicate if a secure client falls back to simple
     * auth
     * @return the proxy
     * @throws IOException if any error occurs
     */
    // TODO: 17/3/18 by zmyer
    public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
        long clientVersion,
        InetSocketAddress addr,
        UserGroupInformation ticket,
        Configuration conf,
        SocketFactory factory,
        int rpcTimeout,
        RetryPolicy connectionRetryPolicy,
        AtomicBoolean fallbackToSimpleAuth)
        throws IOException {
        if (UserGroupInformation.isSecurityEnabled()) {
            //初始化sasl服务器
            SaslRpcServer.init(conf);
        }
        //创建rpc引擎对象
        return getProtocolEngine(protocol, conf).getProxy(protocol, clientVersion,
            addr, ticket, conf, factory, rpcTimeout, connectionRetryPolicy,
            fallbackToSimpleAuth);
    }

    /**
     * Construct a client-side proxy object with the default SocketFactory
     *
     * @param <T>
     * @param protocol
     * @param clientVersion
     * @param addr
     * @param conf
     * @return a proxy instance
     * @throws IOException
     */
    // TODO: 17/3/18 by zmyer
    public static <T> T getProxy(Class<T> protocol,
        long clientVersion,
        InetSocketAddress addr, Configuration conf)
        throws IOException {

        return getProtocolProxy(protocol, clientVersion, addr, conf).getProxy();
    }

    /**
     * Returns the server address for a given proxy.
     */
    // TODO: 17/3/18 by zmyer
    public static InetSocketAddress getServerAddress(Object proxy) {
        return getConnectionIdForProxy(proxy).getAddress();
    }

    /**
     * Return the connection ID of the given object. If the provided object is in
     * fact a protocol translator, we'll get the connection ID of the underlying
     * proxy object.
     *
     * @param proxy the proxy object to get the connection ID of.
     * @return the connection ID for the provided proxy object.
     */
    // TODO: 17/3/18 by zmyer
    public static ConnectionId getConnectionIdForProxy(Object proxy) {
        if (proxy instanceof ProtocolTranslator) {
            proxy = ((ProtocolTranslator) proxy).getUnderlyingProxyObject();
        }
        RpcInvocationHandler inv = (RpcInvocationHandler) Proxy
            .getInvocationHandler(proxy);
        return inv.getConnectionId();
    }

    /**
     * Get a protocol proxy that contains a proxy connection to a remote server
     * and a set of methods that are supported by the server
     *
     * @param protocol
     * @param clientVersion
     * @param addr
     * @param conf
     * @return a protocol proxy
     * @throws IOException
     */
    // TODO: 17/3/18 by zmyer
    public static <T> ProtocolProxy<T> getProtocolProxy(Class<T> protocol,
        long clientVersion,
        InetSocketAddress addr, Configuration conf)
        throws IOException {

        return getProtocolProxy(protocol, clientVersion, addr, conf, NetUtils
            .getDefaultSocketFactory(conf));
    }

    /**
     * Stop the proxy. Proxy must either implement {@link Closeable} or must have
     * associated {@link RpcInvocationHandler}.
     *
     * @param proxy the RPC proxy object to be stopped
     * @throws HadoopIllegalArgumentException if the proxy does not implement {@link Closeable} interface or does not
     * have closeable {@link InvocationHandler}
     */
    // TODO: 17/3/18 by zmyer
    public static void stopProxy(Object proxy) {
        if (proxy == null) {
            throw new HadoopIllegalArgumentException(
                "Cannot close proxy since it is null");
        }
        try {
            if (proxy instanceof Closeable) {
                ((Closeable) proxy).close();
                return;
            } else {
                InvocationHandler handler = Proxy.getInvocationHandler(proxy);
                if (handler instanceof Closeable) {
                    ((Closeable) handler).close();
                    return;
                }
            }
        } catch (IOException e) {
            LOG.error("Closing proxy or invocation handler caused exception", e);
        } catch (IllegalArgumentException e) {
            LOG.error("RPC.stopProxy called on non proxy: class=" + proxy.getClass().getName(), e);
        }

        // If you see this error on a mock object in a unit test you're
        // developing, make sure to use MockitoUtil.mockProtocol() to
        // create your mock.
        throw new HadoopIllegalArgumentException(
            "Cannot close proxy - is not Closeable or "
                + "does not provide closeable invocation handler "
                + proxy.getClass());
    }

    /**
     * Get the RPC time from configuration;
     * If not set in the configuration, return the default value.
     *
     * @param conf Configuration
     * @return the RPC timeout (ms)
     */
    // TODO: 17/3/18 by zmyer
    public static int getRpcTimeout(Configuration conf) {
        return conf.getInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY,
            CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_DEFAULT);
    }

    /**
     * Class to construct instances of RPC server with specific options.
     */
    // TODO: 17/3/18 by zmyer
    public static class Builder {
        //协议类型
        private Class<?> protocol = null;
        //实例对象
        private Object instance = null;
        //绑定地址
        private String bindAddress = "0.0.0.0";
        //端口号
        private int port = 0;
        //handler的数量
        private int numHandlers = 1;
        //reader的数量
        private int numReaders = -1;
        //
        private int queueSizePerHandler = -1;
        private boolean verbose = false;
        //配置对象
        private final Configuration conf;
        private SecretManager<? extends TokenIdentifier> secretManager = null;
        private String portRangeConfig = null;

        // TODO: 17/3/18 by zmyer
        public Builder(Configuration conf) {
            this.conf = conf;
        }

        /** Mandatory field */
        // TODO: 17/3/18 by zmyer
        public Builder setProtocol(Class<?> protocol) {
            this.protocol = protocol;
            return this;
        }

        /** Mandatory field */
        // TODO: 17/3/18 by zmyer
        public Builder setInstance(Object instance) {
            this.instance = instance;
            return this;
        }

        /** Default: 0.0.0.0 */
        // TODO: 17/3/18 by zmyer
        public Builder setBindAddress(String bindAddress) {
            this.bindAddress = bindAddress;
            return this;
        }

        /** Default: 0 */
        // TODO: 17/3/18 by zmyer
        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        /** Default: 1 */
        // TODO: 17/3/18 by zmyer
        public Builder setNumHandlers(int numHandlers) {
            this.numHandlers = numHandlers;
            return this;
        }

        /** Default: -1 */
        // TODO: 17/3/18 by zmyer
        public Builder setnumReaders(int numReaders) {
            this.numReaders = numReaders;
            return this;
        }

        /** Default: -1 */
        // TODO: 17/3/18 by zmyer
        public Builder setQueueSizePerHandler(int queueSizePerHandler) {
            this.queueSizePerHandler = queueSizePerHandler;
            return this;
        }

        /** Default: false */
        // TODO: 17/3/18 by zmyer
        public Builder setVerbose(boolean verbose) {
            this.verbose = verbose;
            return this;
        }

        /** Default: null */
        // TODO: 17/3/18 by zmyer
        public Builder setSecretManager(
            SecretManager<? extends TokenIdentifier> secretManager) {
            this.secretManager = secretManager;
            return this;
        }

        /** Default: null */
        // TODO: 17/3/18 by zmyer
        public Builder setPortRangeConfig(String portRangeConfig) {
            this.portRangeConfig = portRangeConfig;
            return this;
        }

        /**
         * Build the RPC Server.
         *
         * @throws IOException on error
         * @throws HadoopIllegalArgumentException when mandatory fields are not set
         */
        // TODO: 17/3/18 by zmyer
        public Server build() throws IOException, HadoopIllegalArgumentException {
            if (this.conf == null) {
                throw new HadoopIllegalArgumentException("conf is not set");
            }
            if (this.protocol == null) {
                throw new HadoopIllegalArgumentException("protocol is not set");
            }
            if (this.instance == null) {
                throw new HadoopIllegalArgumentException("instance is not set");
            }

            //创建rpc引擎
            return getProtocolEngine(this.protocol, this.conf).getServer(
                this.protocol, this.instance, this.bindAddress, this.port,
                this.numHandlers, this.numReaders, this.queueSizePerHandler,
                this.verbose, this.conf, this.secretManager, this.portRangeConfig);
        }
    }

    /** An RPC Server. */
    // TODO: 17/3/18 by zmyer
    public abstract static class Server extends org.apache.hadoop.ipc.Server {
        boolean verbose;

        // TODO: 17/3/18 by zmyer
        static String classNameBase(String className) {
            String[] names = className.split("\\.", -1);
            if (names == null || names.length == 0) {
                return className;
            }
            return names[names.length - 1];
        }

        /**
         * Store a map of protocol and version to its implementation
         */
        /**
         * The key in Map
         */
        // TODO: 17/3/18 by zmyer
        static class ProtoNameVer {
            //协议名称
            final String protocol;
            //版本号
            final long version;

            // TODO: 17/3/18 by zmyer
            ProtoNameVer(String protocol, long ver) {
                this.protocol = protocol;
                this.version = ver;
            }

            // TODO: 17/3/18 by zmyer
            @Override
            public boolean equals(Object o) {
                if (o == null)
                    return false;
                if (this == o)
                    return true;
                if (!(o instanceof ProtoNameVer))
                    return false;
                ProtoNameVer pv = (ProtoNameVer) o;
                return ((pv.protocol.equals(this.protocol)) &&
                    (pv.version == this.version));
            }

            // TODO: 17/3/18 by zmyer
            @Override
            public int hashCode() {
                return protocol.hashCode() * 37 + (int) version;
            }
        }

        /**
         * The value in map
         */
        // TODO: 17/3/18 by zmyer
        static class ProtoClassProtoImpl {
            //协议类对象
            final Class<?> protocolClass;
            //协议实现类对象
            final Object protocolImpl;

            // TODO: 17/3/18 by zmyer
            ProtoClassProtoImpl(Class<?> protocolClass, Object protocolImpl) {
                this.protocolClass = protocolClass;
                this.protocolImpl = protocolImpl;
            }
        }

        //协议实现类映射表
        ArrayList<Map<ProtoNameVer, ProtoClassProtoImpl>> protocolImplMapArray =
            new ArrayList<Map<ProtoNameVer, ProtoClassProtoImpl>>(RpcKind.MAX_INDEX);

        // TODO: 17/3/18 by zmyer
        Map<ProtoNameVer, ProtoClassProtoImpl> getProtocolImplMap(RPC.RpcKind rpcKind) {
            if (protocolImplMapArray.size() == 0) {// initialize for all rpc kinds
                for (int i = 0; i <= RpcKind.MAX_INDEX; ++i) {
                    protocolImplMapArray.add(new HashMap<>(10));
                }
            }
            return protocolImplMapArray.get(rpcKind.ordinal());
        }

        // Register  protocol and its impl for rpc calls
        // TODO: 17/3/18 by zmyer
        void registerProtocolAndImpl(RpcKind rpcKind, Class<?> protocolClass,
            Object protocolImpl) {
            //读取协议名称
            String protocolName = RPC.getProtocolName(protocolClass);
            long version;

            try {
                //读取rpc的版本号
                version = RPC.getProtocolVersion(protocolClass);
            } catch (Exception ex) {
                LOG.warn("Protocol " + protocolClass +
                    " NOT registered as cannot get protocol version ");
                return;
            }

            //注册指定的rpc类型的协议信息
            getProtocolImplMap(rpcKind).put(new ProtoNameVer(protocolName, version),
                new ProtoClassProtoImpl(protocolClass, protocolImpl));
            if (LOG.isDebugEnabled()) {
                LOG.debug("RpcKind = " + rpcKind + " Protocol Name = " + protocolName +
                    " version=" + version +
                    " ProtocolImpl=" + protocolImpl.getClass().getName() +
                    " protocolClass=" + protocolClass.getName());
            }
        }

        // TODO: 17/3/18 by zmyer
        static class VerProtocolImpl {
            final long version;
            final ProtoClassProtoImpl protocolTarget;

            VerProtocolImpl(long ver, ProtoClassProtoImpl protocolTarget) {
                this.version = ver;
                this.protocolTarget = protocolTarget;
            }
        }

        // TODO: 17/3/18 by zmyer
        VerProtocolImpl[] getSupportedProtocolVersions(RPC.RpcKind rpcKind,
            String protocolName) {
            VerProtocolImpl[] resultk =
                new VerProtocolImpl[getProtocolImplMap(rpcKind).size()];
            int i = 0;
            for (Map.Entry<ProtoNameVer, ProtoClassProtoImpl> pv :
                getProtocolImplMap(rpcKind).entrySet()) {
                if (pv.getKey().protocol.equals(protocolName)) {
                    resultk[i++] =
                        new VerProtocolImpl(pv.getKey().version, pv.getValue());
                }
            }
            if (i == 0) {
                return null;
            }
            VerProtocolImpl[] result = new VerProtocolImpl[i];
            System.arraycopy(resultk, 0, result, 0, i);
            return result;
        }

        // TODO: 17/3/18 by zmyer
        VerProtocolImpl getHighestSupportedProtocol(RpcKind rpcKind,
            String protocolName) {
            Long highestVersion = 0L;
            ProtoClassProtoImpl highest = null;
            if (LOG.isDebugEnabled()) {
                LOG.debug("Size of protoMap for " + rpcKind + " ="
                    + getProtocolImplMap(rpcKind).size());
            }
            for (Map.Entry<ProtoNameVer, ProtoClassProtoImpl> pv :
                getProtocolImplMap(rpcKind).entrySet()) {
                if (pv.getKey().protocol.equals(protocolName)) {
                    if ((highest == null) || (pv.getKey().version > highestVersion)) {
                        highest = pv.getValue();
                        highestVersion = pv.getKey().version;
                    }
                }
            }
            if (highest == null) {
                return null;
            }
            return new VerProtocolImpl(highestVersion, highest);
        }

        // TODO: 17/3/18 by zmyer
        protected Server(String bindAddress, int port,
            Class<? extends Writable> paramClass, int handlerCount,
            int numReaders, int queueSizePerHandler,
            Configuration conf, String serverName,
            SecretManager<? extends TokenIdentifier> secretManager,
            String portRangeConfig) throws IOException {
            super(bindAddress, port, paramClass, handlerCount, numReaders, queueSizePerHandler,
                conf, serverName, secretManager, portRangeConfig);
            initProtocolMetaInfo(conf);
        }

        // TODO: 17/3/18 by zmyer
        private void initProtocolMetaInfo(Configuration conf) {
            RPC.setProtocolEngine(conf, ProtocolMetaInfoPB.class,
                ProtobufRpcEngine.class);
            ProtocolMetaInfoServerSideTranslatorPB xlator =
                new ProtocolMetaInfoServerSideTranslatorPB(this);
            BlockingService protocolInfoBlockingService = ProtocolInfoService
                .newReflectiveBlockingService(xlator);
            addProtocol(RpcKind.RPC_PROTOCOL_BUFFER, ProtocolMetaInfoPB.class,
                protocolInfoBlockingService);
        }

        /**
         * Add a protocol to the existing server.
         *
         * @param protocolClass - the protocol class
         * @param protocolImpl - the impl of the protocol that will be called
         * @return the server (for convenience)
         */
        // TODO: 17/3/18 by zmyer
        public Server addProtocol(RpcKind rpcKind, Class<?> protocolClass,
            Object protocolImpl) {
            registerProtocolAndImpl(rpcKind, protocolClass, protocolImpl);
            return this;
        }

        // TODO: 17/3/18 by zmyer
        @Override
        public Writable call(RPC.RpcKind rpcKind, String protocol,
            Writable rpcRequest, long receiveTime) throws Exception {
            return getRpcInvoker(rpcKind).call(this, protocol, rpcRequest,
                receiveTime);
        }
    }
}
