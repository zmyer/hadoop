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

package org.apache.hadoop.yarn.factories.impl.pb;

import com.google.protobuf.BlockingService;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RpcServerFactory;

// TODO: 17/3/25 by zmyer
@Private
public class RpcServerFactoryPBImpl implements RpcServerFactory {
    private static final Log LOG = LogFactory.getLog(RpcServerFactoryPBImpl.class);
    private static final String PROTO_GEN_PACKAGE_NAME = "org.apache.hadoop.yarn.proto";
    private static final String PROTO_GEN_CLASS_SUFFIX = "Service";
    //pb实现包名前缀
    private static final String PB_IMPL_PACKAGE_SUFFIX = "impl.pb.service";
    //pb服务实现前缀
    private static final String PB_IMPL_CLASS_SUFFIX = "PBServiceImpl";
    //rpc服务器工厂实现对象
    private static final RpcServerFactoryPBImpl self = new RpcServerFactoryPBImpl();

    //配置对象
    private Configuration localConf = new Configuration();
    //服务构造器缓存映射表
    private ConcurrentMap<Class<?>, Constructor<?>> serviceCache = new ConcurrentHashMap<Class<?>, Constructor<?>>();
    //协议缓存
    private ConcurrentMap<Class<?>, Method> protoCache = new ConcurrentHashMap<Class<?>, Method>();

    // TODO: 17/3/25 by zmyer
    public static RpcServerFactoryPBImpl get() {
        return RpcServerFactoryPBImpl.self;
    }

    // TODO: 17/3/25 by zmyer
    private RpcServerFactoryPBImpl() {
    }

    // TODO: 17/3/25 by zmyer
    public Server getServer(Class<?> protocol, Object instance,
        InetSocketAddress addr, Configuration conf,
        SecretManager<? extends TokenIdentifier> secretManager, int numHandlers) {
        return getServer(protocol, instance, addr, conf, secretManager, numHandlers,
            null);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public Server getServer(Class<?> protocol, Object instance,
        InetSocketAddress addr, Configuration conf,
        SecretManager<? extends TokenIdentifier> secretManager, int numHandlers,
        String portRangeConfig) {

        //根据协议,读取服务构造函数
        Constructor<?> constructor = serviceCache.get(protocol);
        if (constructor == null) {
            Class<?> pbServiceImplClazz = null;
            try {
                //如果服务构造函数为空,则从配置中读取
                pbServiceImplClazz = localConf.getClassByName(getPbServiceImplClassName(protocol));
            } catch (ClassNotFoundException e) {
                throw new YarnRuntimeException("Failed to load class: ["
                    + getPbServiceImplClassName(protocol) + "]", e);
            }
            try {
                //从类对象中读取相关的构造函数
                constructor = pbServiceImplClazz.getConstructor(protocol);
                constructor.setAccessible(true);
                //将构造函数插入到服务缓存映射表中
                serviceCache.putIfAbsent(protocol, constructor);
            } catch (NoSuchMethodException e) {
                throw new YarnRuntimeException("Could not find constructor with params: "
                    + Long.TYPE + ", " + InetSocketAddress.class + ", "
                    + Configuration.class, e);
            }
        }

        Object service;
        try {
            //构造具体的服务实例对象
            service = constructor.newInstance(instance);
        } catch (InvocationTargetException | IllegalAccessException | InstantiationException e) {
            throw new YarnRuntimeException(e);
        }

        //从服务类对象中读取协议接口对象
        Class<?> pbProtocol = service.getClass().getInterfaces()[0];
        //根据协议类对象,从协议缓存中读取对应的函数对象
        Method method = protoCache.get(protocol);
        if (method == null) {
            Class<?> protoClazz;
            try {
                //如果协议函数不存在,则直接从配置中读取
                protoClazz = localConf.getClassByName(getProtoClassName(protocol));
            } catch (ClassNotFoundException e) {
                throw new YarnRuntimeException("Failed to load class: ["
                    + getProtoClassName(protocol) + "]", e);
            }
            try {
                //从协议类中读取函数对象
                method = protoClazz.getMethod("newReflectiveBlockingService",
                    pbProtocol.getInterfaces()[0]);
                method.setAccessible(true);
                //将该函数注册到协议缓存中
                protoCache.putIfAbsent(protocol, method);
            } catch (NoSuchMethodException e) {
                throw new YarnRuntimeException(e);
            }
        }

        try {
            //创建服务器对象
            return createServer(pbProtocol, addr, conf, secretManager, numHandlers,
                (BlockingService) method.invoke(null, service), portRangeConfig);
        } catch (InvocationTargetException | IllegalAccessException | IOException e) {
            throw new YarnRuntimeException(e);
        }
    }

    // TODO: 17/3/25 by zmyer
    private String getProtoClassName(Class<?> clazz) {
        //读取类名
        String srcClassName = getClassName(clazz);
        return PROTO_GEN_PACKAGE_NAME + "." + srcClassName + "$" + srcClassName + PROTO_GEN_CLASS_SUFFIX;
    }

    // TODO: 17/3/25 by zmyer
    private String getPbServiceImplClassName(Class<?> clazz) {
        String srcPackagePart = getPackageName(clazz);
        String srcClassName = getClassName(clazz);
        String destPackagePart = srcPackagePart + "." + PB_IMPL_PACKAGE_SUFFIX;
        String destClassPart = srcClassName + PB_IMPL_CLASS_SUFFIX;
        return destPackagePart + "." + destClassPart;
    }

    // TODO: 17/3/25 by zmyer
    private String getClassName(Class<?> clazz) {
        String fqName = clazz.getName();
        return (fqName.substring(fqName.lastIndexOf(".") + 1, fqName.length()));
    }

    // TODO: 17/3/25 by zmyer
    private String getPackageName(Class<?> clazz) {
        return clazz.getPackage().getName();
    }

    // TODO: 17/3/25 by zmyer
    private Server createServer(Class<?> pbProtocol, InetSocketAddress addr, Configuration conf,
        SecretManager<? extends TokenIdentifier> secretManager, int numHandlers,
        BlockingService blockingService, String portRangeConfig) throws IOException {
        //注册pb协议引擎对象
        RPC.setProtocolEngine(conf, pbProtocol, ProtobufRpcEngine.class);
        //创建rpc服务器对象
        RPC.Server server = new RPC.Builder(conf).setProtocol(pbProtocol)
            .setInstance(blockingService).setBindAddress(addr.getHostName())
            .setPort(addr.getPort()).setNumHandlers(numHandlers).setVerbose(false)
            .setSecretManager(secretManager).setPortRangeConfig(portRangeConfig)
            .build();
        LOG.info("Adding protocol " + pbProtocol.getCanonicalName() + " to the server");
        //为rpc服务器添加pb协议支持
        server.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, pbProtocol, blockingService);
        return server;
    }
}
