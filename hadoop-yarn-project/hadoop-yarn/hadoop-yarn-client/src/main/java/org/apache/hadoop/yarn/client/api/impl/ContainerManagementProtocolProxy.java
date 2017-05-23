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

package org.apache.hadoop.yarn.client.api.impl;

import com.google.common.annotations.VisibleForTesting;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.NMProxy;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;

/**
 * Helper class to manage container manager proxies
 */
// TODO: 17/3/26 by zmyer
@LimitedPrivate({"MapReduce", "YARN"})
public class ContainerManagementProtocolProxy {
    static final Log LOG = LogFactory.getLog(ContainerManagementProtocolProxy.class);

    //管理器中最大的连接数
    private final int maxConnectedNMs;
    //容器代理映射表
    private final Map<String, ContainerManagementProtocolProxyData> cmProxy;
    //配置对象
    private final Configuration conf;
    //rpc对象
    private final YarnRPC rpc;
    //token缓存对象
    private NMTokenCache nmTokenCache;

    // TODO: 17/3/27 by zmyer
    public ContainerManagementProtocolProxy(Configuration conf) {
        this(conf, NMTokenCache.getSingleton());
    }

    // TODO: 17/3/27 by zmyer
    ContainerManagementProtocolProxy(Configuration conf, NMTokenCache nmTokenCache) {
        this.conf = new Configuration(conf);
        this.nmTokenCache = nmTokenCache;
        //最大的node节点管理器连接数目
        maxConnectedNMs =
            conf.getInt(YarnConfiguration.NM_CLIENT_MAX_NM_PROXIES,
                YarnConfiguration.DEFAULT_NM_CLIENT_MAX_NM_PROXIES);
        if (maxConnectedNMs < 0) {
            throw new YarnRuntimeException(YarnConfiguration.NM_CLIENT_MAX_NM_PROXIES
                + " (" + maxConnectedNMs + ") can not be less than 0.");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(YarnConfiguration.NM_CLIENT_MAX_NM_PROXIES + " : " + maxConnectedNMs);
        }

        if (maxConnectedNMs > 0) {
            cmProxy = new LinkedHashMap<>();
        } else {
            cmProxy = Collections.emptyMap();
            // Connections are not being cached so ensure connections close quickly
            // to avoid creating thousands of RPC client threads on large clusters.
            this.conf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
                0);
        }
        //创建rpc对象
        rpc = YarnRPC.create(conf);
    }

    // TODO: 17/3/27 by zmyer
    public synchronized ContainerManagementProtocolProxyData getProxy(
        String containerManagerBindAddr, ContainerId containerId) throws InvalidToken {

        // This get call will update the map which is working as LRU cache.
        //根据提供的容器管理器绑定地址,读取容器管理器协议代理数据
        ContainerManagementProtocolProxyData proxy = cmProxy.get(containerManagerBindAddr);

        while (proxy != null && !proxy.token.getIdentifier().equals(
            nmTokenCache.getToken(containerManagerBindAddr).getIdentifier())) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Refreshing proxy as NMToken got updated for node : "
                    + containerManagerBindAddr);
            }
            // Token is updated. check if anyone has already tried closing it.
            if (!proxy.scheduledForClose) {
                // try closing the proxy. Here if someone is already using it
                // then we might not close it. In which case we will wait.
                //当token不一致时,及时删除代理对象
                removeProxy(proxy);
            } else {
                try {
                    //等待片刻
                    this.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (proxy.activeCallers < 0) {
                //如果当前的代理对象中call数据小于0,则及时重新创建代理对象
                proxy = cmProxy.get(containerManagerBindAddr);
            }
        }

        if (proxy == null) {
            //如果代理对象为空,则重新创建
            proxy = new ContainerManagementProtocolProxyData(rpc, containerManagerBindAddr,
                containerId, nmTokenCache.getToken(containerManagerBindAddr));
            if (maxConnectedNMs > 0) {
                //将该代理对象插入到缓存中
                addProxyToCache(containerManagerBindAddr, proxy);
            }
        }
        // This is to track active users of this proxy.
        //递增激活的call数量
        proxy.activeCallers++;
        //更新lru缓存
        updateLRUCache(containerManagerBindAddr);

        return proxy;
    }

    // TODO: 17/3/27 by zmyer
    private void addProxyToCache(String containerManagerBindAddr,
        ContainerManagementProtocolProxyData proxy) {
        while (cmProxy.size() >= maxConnectedNMs) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cleaning up the proxy cache, size=" + cmProxy.size()
                    + " max=" + maxConnectedNMs);
            }
            boolean removedProxy = false;
            for (ContainerManagementProtocolProxyData otherProxy : cmProxy.values()) {
                removedProxy = removeProxy(otherProxy);
                if (removedProxy) {
                    break;
                }
            }
            if (!removedProxy) {
                // all of the proxies are currently in use and already scheduled
                // for removal, so we need to wait until at least one of them closes
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        if (maxConnectedNMs > 0) {
            //重新插入代理对象
            cmProxy.put(containerManagerBindAddr, proxy);
        }
    }

    // TODO: 17/3/27 by zmyer
    private void updateLRUCache(String containerManagerBindAddr) {
        if (maxConnectedNMs > 0) {
            ContainerManagementProtocolProxyData proxy = cmProxy.remove(containerManagerBindAddr);
            cmProxy.put(containerManagerBindAddr, proxy);
        }
    }

    // TODO: 17/3/27 by zmyer
    public synchronized void mayBeCloseProxy(ContainerManagementProtocolProxyData proxy) {
        tryCloseProxy(proxy);
    }

    // TODO: 17/3/27 by zmyer
    private boolean tryCloseProxy(
        ContainerManagementProtocolProxyData proxy) {
        proxy.activeCallers--;
        if (proxy.scheduledForClose && proxy.activeCallers < 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Closing proxy : " + proxy.containerManagerBindAddr);
            }
            cmProxy.remove(proxy.containerManagerBindAddr);
            try {
                rpc.stopProxy(proxy.getContainerManagementProtocol(), conf);
            } finally {
                this.notifyAll();
            }
            return true;
        }
        return false;
    }

    // TODO: 17/3/27 by zmyer
    private synchronized boolean removeProxy(ContainerManagementProtocolProxyData proxy) {
        if (!proxy.scheduledForClose) {
            proxy.scheduledForClose = true;
            return tryCloseProxy(proxy);
        }
        return false;
    }

    // TODO: 17/3/27 by zmyer
    synchronized void stopAllProxies() {
        List<String> nodeIds = new ArrayList<String>();
        nodeIds.addAll(this.cmProxy.keySet());
        for (String nodeId : nodeIds) {
            ContainerManagementProtocolProxyData proxy = cmProxy.get(nodeId);
            // Explicitly reducing the proxy count to allow stopping proxy.
            proxy.activeCallers = 0;
            try {
                removeProxy(proxy);
            } catch (Throwable t) {
                LOG.error("Error closing connection", t);
            }
        }
        cmProxy.clear();
    }

    // TODO: 17/3/27 by zmyer
    public class ContainerManagementProtocolProxyData {
        //容器管理器绑定的地址
        private final String containerManagerBindAddr;
        //容器管理器协议代理对象
        private final ContainerManagementProtocol proxy;
        //激活的call数量
        private int activeCallers;
        //是否关闭调度标记
        private boolean scheduledForClose;
        //token
        private final Token token;

        @Private
        @VisibleForTesting
        // TODO: 17/3/27 by zmyer
        public ContainerManagementProtocolProxyData(YarnRPC rpc,
            String containerManagerBindAddr,
            ContainerId containerId, Token token) throws InvalidToken {
            this.containerManagerBindAddr = containerManagerBindAddr;
            ;
            this.activeCallers = 0;
            this.scheduledForClose = false;
            this.token = token;
            this.proxy = newProxy(rpc, containerManagerBindAddr, containerId, token);
        }

        @Private
        @VisibleForTesting
            // TODO: 17/3/27 by zmyer
        ContainerManagementProtocol newProxy(final YarnRPC rpc,
            String containerManagerBindAddr, ContainerId containerId, Token token)
            throws InvalidToken {

            if (token == null) {
                throw new InvalidToken("No NMToken sent for " + containerManagerBindAddr);
            }

            //容器对象地址
            final InetSocketAddress cmAddr = NetUtils.createSocketAddr(containerManagerBindAddr);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Opening proxy : " + containerManagerBindAddr);
            }
            // the user in createRemoteUser in this context has to be ContainerID
            UserGroupInformation user =
                UserGroupInformation.createRemoteUser(containerId
                    .getApplicationAttemptId().toString());

            org.apache.hadoop.security.token.Token<NMTokenIdentifier> nmToken =
                ConverterUtils.convertFromYarn(token, cmAddr);
            user.addToken(nmToken);

            //创建容器管理协议代理对象
            return NMProxy.createNMProxy(conf, ContainerManagementProtocol.class,
                user, rpc, cmAddr);
        }

        // TODO: 17/3/27 by zmyer
        public ContainerManagementProtocol getContainerManagementProtocol() {
            return proxy;
        }
    }

}
