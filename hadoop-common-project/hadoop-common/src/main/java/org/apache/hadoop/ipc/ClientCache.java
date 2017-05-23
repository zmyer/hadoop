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

import java.util.HashMap;
import java.util.Map;
import javax.net.SocketFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

/* Cache a client using its socket factory as the hash key */
// TODO: 17/3/19 by zmyer
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class ClientCache {
    //客户端映射表
    private Map<SocketFactory, Client> clients = new HashMap<SocketFactory, Client>();

    /**
     * Construct & cache an IPC client with the user-provided SocketFactory
     * if no cached client exists.
     *
     * @param conf Configuration
     * @param factory SocketFactory for client socket
     * @param valueClass Class of the expected response
     * @return an IPC client
     */
    // TODO: 17/3/19 by zmyer
    public synchronized Client getClient(Configuration conf,
        SocketFactory factory, Class<? extends Writable> valueClass) {
        // Construct & cache client.  The configuration is only used for timeout,
        // and Clients have connection pools.  So we can either (a) lose some
        // connection pooling and leak sockets, or (b) use the same timeout for all
        // configurations.  Since the IPC is usually intended globally, not
        // per-job, we choose (a).
        //根据提供的套接字对象,获取指定的客户端对象
        Client client = clients.get(factory);
        if (client == null) {
            //如果不存在该客户端,则重新创建
            client = new Client(valueClass, conf, factory);
            //将该客户端注册到映射表中
            clients.put(factory, client);
        } else {
            //递增该客户端的引用计数器
            client.incCount();
        }
        if (Client.LOG.isDebugEnabled()) {
            Client.LOG.debug("getting client out of cache: " + client);
        }
        //返回客户端
        return client;
    }

    /**
     * Construct & cache an IPC client with the default SocketFactory
     * and default valueClass if no cached client exists.
     *
     * @param conf Configuration
     * @return an IPC client
     */
    // TODO: 17/3/19 by zmyer
    public synchronized Client getClient(Configuration conf) {
        //读取客户端
        return getClient(conf, SocketFactory.getDefault(), ObjectWritable.class);
    }

    /**
     * Construct & cache an IPC client with the user-provided SocketFactory
     * if no cached client exists. Default response type is ObjectWritable.
     *
     * @param conf Configuration
     * @param factory SocketFactory for client socket
     * @return an IPC client
     */
    // TODO: 17/3/19 by zmyer
    public synchronized Client getClient(Configuration conf, SocketFactory factory) {
        //读取客户端
        return this.getClient(conf, factory, ObjectWritable.class);
    }

    /**
     * Stop a RPC client connection
     * A RPC client is closed only when its reference count becomes zero.
     */
    // TODO: 17/3/19 by zmyer
    public void stopClient(Client client) {
        if (Client.LOG.isDebugEnabled()) {
            Client.LOG.debug("stopping client from cache: " + client);
        }
        synchronized (this) {
            //递减客户端引用计数器
            client.decCount();
            if (client.isZeroReference()) {
                if (Client.LOG.isDebugEnabled()) {
                    Client.LOG.debug("removing client from cache: " + client);
                }
                //从客户端映射表删除该客户端
                clients.remove(client.getSocketFactory());
            }
        }

        //客户端引用计数器为0
        if (client.isZeroReference()) {
            if (Client.LOG.isDebugEnabled()) {
                Client.LOG.debug("stopping actual client because no more references remain: "
                    + client);
            }
            //停止客户端
            client.stop();
        }
    }
}
