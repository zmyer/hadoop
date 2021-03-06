/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.ipc;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.yarn.exceptions.YarnException;

// TODO: 17/3/25 by zmyer
@InterfaceAudience.LimitedPrivate({"MapReduce", "YARN"})
public class RPCUtil {

    /**
     * Returns an instance of {@link YarnException}
     */
    // TODO: 17/3/25 by zmyer
    public static YarnException getRemoteException(Throwable t) {
        return new YarnException(t);
    }

    /**
     * Returns an instance of {@link YarnException}
     */
    // TODO: 17/3/25 by zmyer
    public static YarnException getRemoteException(String message) {
        return new YarnException(message);
    }

    // TODO: 17/3/25 by zmyer
    private static <T extends Throwable> T instantiateException(
        Class<? extends T> cls, RemoteException re) throws RemoteException {
        try {
            //读取构造函数对象
            Constructor<? extends T> cn = cls.getConstructor(String.class);
            //设置可访问标记
            cn.setAccessible(true);
            //根据构造函数实例化异常对象
            T ex = cn.newInstance(re.getMessage());
            //初始化异常对象
            ex.initCause(re);
            //返回异常对象
            return ex;
            // RemoteException contains useful information as against the
            // java.lang.reflect exceptions.
        } catch (NoSuchMethodException | IllegalArgumentException | SecurityException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw re;
        }
    }

    // TODO: 17/3/25 by zmyer
    private static <T extends YarnException> T instantiateYarnException(
        Class<? extends T> cls, RemoteException re) throws RemoteException {
        //实例化异常
        return instantiateException(cls, re);
    }

    // TODO: 17/3/25 by zmyer
    private static <T extends IOException> T instantiateIOException(
        Class<? extends T> cls, RemoteException re) throws RemoteException {
        //实例化IO异常
        return instantiateException(cls, re);
    }

    // TODO: 17/3/25 by zmyer
    private static <T extends RuntimeException> T instantiateRuntimeException(
        Class<? extends T> cls, RemoteException re) throws RemoteException {
        return instantiateException(cls, re);
    }

    /**
     * Utility method that unwraps and returns appropriate exceptions.
     *
     * @param se ServiceException
     * @return An instance of the actual exception, which will be a subclass of {@link YarnException} or {@link
     * IOException}
     */
    // TODO: 17/3/25 by zmyer
    public static Void unwrapAndThrowException(ServiceException se)
        throws IOException, YarnException {
        Throwable cause = se.getCause();
        if (cause == null) {
            // SE generated by the RPC layer itself.
            throw new IOException(se);
        } else {
            if (cause instanceof RemoteException) {
                RemoteException re = (RemoteException) cause;
                Class<?> realClass = null;
                try {
                    realClass = Class.forName(re.getClassName());
                } catch (ClassNotFoundException cnf) {
                    // Assume this to be a new exception type added to YARN. This isn't
                    // absolutely correct since the RPC layer could add an exception as
                    // well.
                    throw instantiateYarnException(YarnException.class, re);
                }

                if (YarnException.class.isAssignableFrom(realClass)) {
                    throw instantiateYarnException(
                        realClass.asSubclass(YarnException.class), re);
                } else if (IOException.class.isAssignableFrom(realClass)) {
                    throw instantiateIOException(realClass.asSubclass(IOException.class),
                        re);
                } else if (RuntimeException.class.isAssignableFrom(realClass)) {
                    throw instantiateRuntimeException(
                        realClass.asSubclass(RuntimeException.class), re);
                } else {
                    throw re;
                }
                // RemoteException contains useful information as against the
                // java.lang.reflect exceptions.

            } else if (cause instanceof IOException) {
                // RPC Client exception.
                throw (IOException) cause;
            } else if (cause instanceof RuntimeException) {
                // RPC RuntimeException
                throw (RuntimeException) cause;
            } else {
                // Should not be generated.
                throw new IOException(se);
            }
        }
    }
}
