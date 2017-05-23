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
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;

// TODO: 17/3/25 by zmyer
@LimitedPrivate({"MapReduce", "YARN"})
@Unstable
public class RecordFactoryProvider {
    //默认的配置对象
    private static Configuration defaultConf;

    static {
        //初始化默认的配置
        defaultConf = new Configuration();
    }

    // TODO: 17/3/25 by zmyer
    private RecordFactoryProvider() {
    }

    // TODO: 17/3/25 by zmyer
    public static RecordFactory getRecordFactory(Configuration conf) {
        if (conf == null) {
            //Assuming the default configuration has the correct factories set.
            //Users can specify a particular factory by providing a configuration.
            //设置默认配置
            conf = defaultConf;
        }
        //读取记录工厂类名
        String recordFactoryClassName = conf.get(YarnConfiguration.IPC_RECORD_FACTORY_CLASS,
            YarnConfiguration.DEFAULT_IPC_RECORD_FACTORY_CLASS);
        //创建记录工厂对象
        return (RecordFactory) getFactoryClassInstance(recordFactoryClassName);
    }

    // TODO: 17/3/25 by zmyer
    private static Object getFactoryClassInstance(String factoryClassName) {
        try {
            //加载工厂类对象
            Class<?> clazz = Class.forName(factoryClassName);
            //从类对象中读取指定的get函数
            Method method = clazz.getMethod("get", null);
            method.setAccessible(true);
            //返回工厂对象
            return method.invoke(null, null);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new YarnRuntimeException(e);
        }
    }
}
