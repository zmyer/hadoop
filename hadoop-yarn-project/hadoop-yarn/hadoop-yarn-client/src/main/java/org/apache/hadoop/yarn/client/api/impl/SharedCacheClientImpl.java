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
import java.io.IOException;
import java.net.InetSocketAddress;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ClientSCMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ReleaseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.UseSharedCacheResourceResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.SharedCacheClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.sharedcache.SharedCacheChecksum;
import org.apache.hadoop.yarn.sharedcache.SharedCacheChecksumFactory;
import org.apache.hadoop.yarn.util.Records;

/**
 * An implementation of the SharedCacheClient API.
 */
@Private
@Unstable
// TODO: 17/3/25 by zmyer
public class SharedCacheClientImpl extends SharedCacheClient {
    private static final Log LOG = LogFactory.getLog(SharedCacheClientImpl.class);
    //客户端与共享缓存管理器协议对象
    private ClientSCMProtocol scmClient;
    //共享缓存管理器地址对象
    private InetSocketAddress scmAddress;
    //配置对象
    private Configuration conf;
    //共享缓存校验器对象
    private SharedCacheChecksum checksum;

    // TODO: 17/3/25 by zmyer
    public SharedCacheClientImpl() {
        super(SharedCacheClientImpl.class.getName());
    }

    // TODO: 17/3/25 by zmyer
    private static InetSocketAddress getScmAddress(Configuration conf) {
        //读取共享管理器地址
        return conf.getSocketAddr(YarnConfiguration.SCM_CLIENT_SERVER_ADDRESS,
            YarnConfiguration.DEFAULT_SCM_CLIENT_SERVER_ADDRESS,
            YarnConfiguration.DEFAULT_SCM_CLIENT_SERVER_PORT);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        if (this.scmAddress == null) {
            //读取共享缓存管理器地址
            this.scmAddress = getScmAddress(conf);
        }
        //设置配置对象
        this.conf = conf;
        //创建校验器
        this.checksum = SharedCacheChecksumFactory.getChecksum(conf);
        //父类服务初始化
        super.serviceInit(conf);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    protected void serviceStart() throws Exception {
        //创建共享缓存管理器代理对象
        this.scmClient = createClientProxy();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Connecting to Shared Cache Manager at " + this.scmAddress);
        }
        //启动父类服务对象
        super.serviceStart();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    protected void serviceStop() throws Exception {
        //关闭共享缓存管理器代理对象
        stopClientProxy();
        //关闭父类服务对象
        super.serviceStop();
    }

    // TODO: 17/3/25 by zmyer
    @VisibleForTesting
    protected ClientSCMProtocol createClientProxy() {
        //创建yarn rpc服务对象
        YarnRPC rpc = YarnRPC.create(getConfig());
        //创建共享缓存管理器代理对象
        return (ClientSCMProtocol) rpc.getProxy(ClientSCMProtocol.class,
            this.scmAddress, getConfig());
    }

    // TODO: 17/3/25 by zmyer
    @VisibleForTesting
    protected void stopClientProxy() {
        if (this.scmClient != null) {
            //停止代理对象
            RPC.stopProxy(this.scmClient);
            this.scmClient = null;
        }
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public Path use(ApplicationId applicationId, String resourceKey) throws YarnException {
        Path resourcePath = null;
        //使用共享缓存资源请求对象
        UseSharedCacheResourceRequest request = Records.newRecord(UseSharedCacheResourceRequest.class);
        //设置应用id
        request.setAppId(applicationId);
        //设置资源键值
        request.setResourceKey(resourceKey);
        try {
            //开始发送使用缓存资源请求对象
            UseSharedCacheResourceResponse response = this.scmClient.use(request);
            if (response != null && response.getPath() != null) {
                resourcePath = new Path(response.getPath());
            }
        } catch (Exception e) {
            // Just catching IOException isn't enough.
            // RPC call can throw ConnectionException.
            // We don't handle different exceptions separately at this point.
            throw new YarnException(e);
        }
        return resourcePath;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void release(ApplicationId applicationId, String resourceKey) throws YarnException {
        ReleaseSharedCacheResourceRequest request = Records.newRecord(
            ReleaseSharedCacheResourceRequest.class);
        request.setAppId(applicationId);
        request.setResourceKey(resourceKey);
        try {
            // We do not care about the response because it is empty.
            //请求释放共享缓存
            this.scmClient.release(request);
        } catch (Exception e) {
            // Just catching IOException isn't enough.
            // RPC call can throw ConnectionException.
            throw new YarnException(e);
        }
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public String getFileChecksum(Path sourceFile) throws IOException {
        //读取配置
        FileSystem fs = sourceFile.getFileSystem(this.conf);
        FSDataInputStream in = null;
        try {
            //打开配置文件
            in = fs.open(sourceFile);
            //检查配置文件校验码
            return this.checksum.computeChecksum(in);
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }
}
