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

import java.io.Closeable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RpcClientFactory;

// TODO: 17/3/25 by zmyer
@Private
public class RpcClientFactoryPBImpl implements RpcClientFactory {

    private static final Log LOG = LogFactory.getLog(RpcClientFactoryPBImpl.class);

    //pb实现前缀
    private static final String PB_IMPL_PACKAGE_SUFFIX = "impl.pb.client";
    //pb实现类前缀
    private static final String PB_IMPL_CLASS_SUFFIX = "PBClientImpl";
    //rpc客户端工厂实现对象
    private static final RpcClientFactoryPBImpl self = new RpcClientFactoryPBImpl();
    //配置对象
    private Configuration localConf = new Configuration();
    //构造函数缓存对象
    private ConcurrentMap<Class<?>, Constructor<?>> cache = new ConcurrentHashMap<Class<?>, Constructor<?>>();

    // TODO: 17/3/25 by zmyer
    public static RpcClientFactoryPBImpl get() {
        return RpcClientFactoryPBImpl.self;
    }

    // TODO: 17/3/25 by zmyer
    private RpcClientFactoryPBImpl() {
    }

    // TODO: 17/3/25 by zmyer
    public Object getClient(Class<?> protocol, long clientVersion,
        InetSocketAddress addr, Configuration conf) {

        //根据协议名,查找构造函数
        Constructor<?> constructor = cache.get(protocol);
        if (constructor == null) {
            Class<?> pbClazz = null;
            try {
                //如果缓存中不存在,则直接从配置中读取
                pbClazz = localConf.getClassByName(getPBImplClassName(protocol));
            } catch (ClassNotFoundException e) {
                throw new YarnRuntimeException("Failed to load class: ["
                    + getPBImplClassName(protocol) + "]", e);
            }
            try {
                //根据提供的类对象,读取类的构造函数
                constructor = pbClazz.getConstructor(Long.TYPE, InetSocketAddress.class, Configuration.class);
                constructor.setAccessible(true);
                //将该构造函数注册到构造函数映射表
                cache.putIfAbsent(protocol, constructor);
            } catch (NoSuchMethodException e) {
                throw new YarnRuntimeException("Could not find constructor with params: " + Long.TYPE + ", " + InetSocketAddress.class + ", " + Configuration.class, e);
            }
        }
        try {
            //根据提供的构造函数创建实例对象
            Object retObject = constructor.newInstance(clientVersion, addr, conf);
            //返回实例对象
            return retObject;
        } catch (InvocationTargetException | IllegalAccessException | InstantiationException e) {
            throw new YarnRuntimeException(e);
        }
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void stopClient(Object proxy) {
        try {
            if (proxy instanceof Closeable) {
                //关闭对象
                ((Closeable) proxy).close();
                return;
            } else {
                InvocationHandler handler = Proxy.getInvocationHandler(proxy);
                if (handler instanceof Closeable) {
                    ((Closeable) handler).close();
                    return;
                }
            }
        } catch (Exception e) {
            LOG.error("Cannot call close method due to Exception. " + "Ignoring.", e);
            throw new YarnRuntimeException(e);
        }
        throw new HadoopIllegalArgumentException(
            "Cannot close proxy - is not Closeable or "
                + "does not provide closeable invocation handler " + proxy.getClass());
    }

    // TODO: 17/3/25 by zmyer
    private String getPBImplClassName(Class<?> clazz) {
        //读取包名
        String srcPackagePart = getPackageName(clazz);
        //读取类名
        String srcClassName = getClassName(clazz);
        //创建目标包路径
        String destPackagePart = srcPackagePart + "." + PB_IMPL_PACKAGE_SUFFIX;
        //创建类路径
        String destClassPart = srcClassName + PB_IMPL_CLASS_SUFFIX;
        //返回类的绝对路径
        return destPackagePart + "." + destClassPart;
    }

    // TODO: 17/3/25 by zmyer
    private String getClassName(Class<?> clazz) {
        //读取类路径
        String fqName = clazz.getName();
        //读取类名
        return (fqName.substring(fqName.lastIndexOf(".") + 1, fqName.length()));
    }

    // TODO: 17/3/25 by zmyer
    private String getPackageName(Class<?> clazz) {
        return clazz.getPackage().getName();
    }
}