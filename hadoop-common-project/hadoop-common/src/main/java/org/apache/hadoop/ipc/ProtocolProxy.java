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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashSet;

/**
 * a class wraps around a server's proxy,
 * containing a list of its supported methods.
 *
 * A list of methods with a value of null indicates that the client and server
 * have the same protocol.
 */
// TODO: 17/3/19 by zmyer
public class ProtocolProxy<T> {
    //协议类对象
    private Class<T> protocol;
    //代理对象
    private T proxy;
    //服务器函数集合
    private HashSet<Integer> serverMethods = null;
    //是否支持服务器函数检查机制
    final private boolean supportServerMethodCheck;
    private boolean serverMethodsFetched = false;

    /**
     * Constructor
     *
     * @param protocol protocol class
     * @param proxy its proxy
     * @param supportServerMethodCheck If false proxy will never fetch server methods and isMethodSupported will always
     * return true. If true, server methods will be fetched for the first call to isMethodSupported.
     */
    // TODO: 17/3/19 by zmyer
    public ProtocolProxy(Class<T> protocol, T proxy, boolean supportServerMethodCheck) {
        this.protocol = protocol;
        this.proxy = proxy;
        this.supportServerMethodCheck = supportServerMethodCheck;
    }

    // TODO: 17/3/19 by zmyer
    private void fetchServerMethods(Method method) throws IOException {
        long clientVersion;
        //读取客户端版本
        clientVersion = RPC.getProtocolVersion(method.getDeclaringClass());
        //开始进入签名
        int clientMethodsHash = ProtocolSignature.getFingerprint(method
            .getDeclaringClass().getMethods());
        //获取服务器签名
        ProtocolSignature serverInfo = ((VersionedProtocol) proxy)
            .getProtocolSignature(RPC.getProtocolName(protocol), clientVersion,
                clientMethodsHash);
        //读取服务器版本
        long serverVersion = serverInfo.getVersion();
        if (serverVersion != clientVersion) {
            //如果服务器与客户端版本不一致,则直接抛出异常
            throw new RPC.VersionMismatch(protocol.getName(), clientVersion, serverVersion);
        }

        //获取服务器函数编码集合
        int[] serverMethodsCodes = serverInfo.getMethods();
        if (serverMethodsCodes != null) {
            //创建服务器函数集合
            serverMethods = new HashSet<>(serverMethodsCodes.length);
            for (int m : serverMethodsCodes) {
                //将接口编码插入到服务器函数集合中
                this.serverMethods.add(m);
            }
        }
        serverMethodsFetched = true;
    }

    /*
     * Get the proxy
     */
    // TODO: 17/3/19 by zmyer
    public T getProxy() {
        return proxy;
    }

    /**
     * Check if a method is supported by the server or not
     *
     * @param methodName a method's name in String format
     * @param parameterTypes a method's parameter types
     * @return true if the method is supported by the server
     */
    // TODO: 17/3/19 by zmyer
    public synchronized boolean isMethodSupported(String methodName,
        Class<?>... parameterTypes)
        throws IOException {
        if (!supportServerMethodCheck) {
            return true;
        }
        Method method;
        try {
            //从协议类对象中读取指定函数对象
            method = protocol.getDeclaredMethod(methodName, parameterTypes);
        } catch (SecurityException | NoSuchMethodException e) {
            throw new IOException(e);
        }
        if (!serverMethodsFetched) {
            //开始检查服务器函数
            fetchServerMethods(method);
        }
        if (serverMethods == null) { // client & server have the same protocol
            return true;
        }
        return serverMethods.contains(ProtocolSignature.getFingerprint(method));
    }
}