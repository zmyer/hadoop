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

package org.apache.hadoop.yarn.factory.providers;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RpcClientFactory;
import org.apache.hadoop.yarn.factories.RpcServerFactory;

/**
 * A public static get() method must be present in the Client/Server Factory implementation.
 */
// TODO: 17/3/25 by zmyer
@InterfaceAudience.LimitedPrivate({"MapReduce", "YARN"})
public class RpcFactoryProvider {
    private RpcFactoryProvider() {

    }
    // TODO: 17/3/25 by zmyer
    public static RpcServerFactory getServerFactory(Configuration conf) {
        if (conf == null) {
            //读取配置
            conf = new Configuration();
        }
        //从配置中读取rpc服务器工厂类名
        String serverFactoryClassName = conf.get(YarnConfiguration.IPC_SERVER_FACTORY_CLASS,
            YarnConfiguration.DEFAULT_IPC_SERVER_FACTORY_CLASS);
        //根据提供的类名,实例化相关对象
        return (RpcServerFactory) getFactoryClassInstance(serverFactoryClassName);
    }

    // TODO: 17/3/25 by zmyer
    public static RpcClientFactory getClientFactory(Configuration conf) {
        String clientFactoryClassName = conf.get(YarnConfiguration.IPC_CLIENT_FACTORY_CLASS,
            YarnConfiguration.DEFAULT_IPC_CLIENT_FACTORY_CLASS);
        return (RpcClientFactory) getFactoryClassInstance(clientFactoryClassName);
    }

    // TODO: 17/3/25 by zmyer
    private static Object getFactoryClassInstance(String factoryClassName) {
        try {
            //首先根据类名,创建具体的类对象
            Class<?> clazz = Class.forName(factoryClassName);
            //从类对象中读取get函数
            Method method = clazz.getMethod("get");
            method.setAccessible(true);
            //开始调用get函数
            return method.invoke(null);
        } catch (ClassNotFoundException | NoSuchMethodException |
            InvocationTargetException | IllegalAccessException e) {
            throw new YarnRuntimeException(e);
        }
    }

}
