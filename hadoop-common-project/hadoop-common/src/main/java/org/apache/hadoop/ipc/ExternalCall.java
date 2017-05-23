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
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.ipc.Server.Call;
import org.apache.hadoop.security.UserGroupInformation;

// TODO: 17/3/19 by zmyer
public abstract class ExternalCall<T> extends Call {
    private final PrivilegedExceptionAction<T> action;
    //标记是否完成
    private final AtomicBoolean done = new AtomicBoolean();
    //call调用结果
    private T result;
    //call异常对象
    private Throwable error;

    // TODO: 17/3/19 by zmyer
    public ExternalCall(PrivilegedExceptionAction<T> action) {
        this.action = action;
    }

    // TODO: 17/3/19 by zmyer
    public abstract UserGroupInformation getRemoteUser();

    // TODO: 17/3/19 by zmyer
    public final T get() throws InterruptedException, ExecutionException {
        //等待call完成
        waitForCompletion();
        if (error != null) {
            throw new ExecutionException(error);
        }
        //返回调用结果
        return result;
    }

    // wait for response to be triggered to support postponed calls
    // TODO: 17/3/19 by zmyer
    private void waitForCompletion() throws InterruptedException {
        synchronized (done) {
            while (!done.get()) {
                try {
                    //等待结果
                    done.wait();
                } catch (InterruptedException ie) {
                    if (Thread.interrupted()) {
                        throw ie;
                    }
                }
            }
        }
    }

    // TODO: 17/3/19 by zmyer
    boolean isDone() {
        return done.get();
    }

    // invoked by ipc handler
    // TODO: 17/3/19 by zmyer
    @Override
    public final Void run() throws IOException {
        try {
            //开始调用call
            result = action.run();
            //返回应答消息
            sendResponse();
        } catch (Throwable t) {
            //返回调用异常信息
            abortResponse(t);
        }
        return null;
    }

    // TODO: 17/3/19 by zmyer
    @Override
    final void doResponse(Throwable t) {
        synchronized (done) {
            //设置错误信息
            error = t;
            //设置完成标记
            done.set(true);
            //发送公告
            done.notify();
        }
    }
}