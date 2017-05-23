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

package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * Leader election implementation that uses Curator.
 */
// TODO: 17/3/22 by zmyer
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CuratorBasedElectorService extends AbstractService
    implements EmbeddedElector, LeaderLatchListener {
    public static final Log LOG = LogFactory.getLog(CuratorBasedElectorService.class);
    //选主屏障对象
    private LeaderLatch leaderLatch;
    //zk客户端
    private CuratorFramework curator;
    //rm执行上下文对象
    private RMContext rmContext;
    //屏障路径
    private String latchPath;
    //资源管理器id
    private String rmId;
    //资源管理器
    private ResourceManager rm;

    // TODO: 17/3/24 by zmyer
    public CuratorBasedElectorService(RMContext rmContext, ResourceManager rm) {
        super(CuratorBasedElectorService.class.getName());
        this.rmContext = rmContext;
        this.rm = rm;
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        //创建资源管理器id
        rmId = HAUtil.getRMHAId(conf);
        //读取集群id
        String clusterId = YarnConfiguration.getClusterId(conf);
        //选主根路径
        String zkBasePath = conf.get(YarnConfiguration.AUTO_FAILOVER_ZK_BASE_PATH,
            YarnConfiguration.DEFAULT_AUTO_FAILOVER_ZK_BASE_PATH);
        //创建选主屏障路径
        latchPath = zkBasePath + "/" + clusterId;
        //读取zk客户端
        curator = rm.getCurator();
        //初始化并启动选主屏障
        initAndStartLeaderLatch();
        //父类服务初始化
        super.serviceInit(conf);
    }

    // TODO: 17/3/24 by zmyer
    private void initAndStartLeaderLatch() throws Exception {
        //创建选主屏障对象
        leaderLatch = new LeaderLatch(curator, latchPath, rmId);
        //在该屏障对象中注册选主服务
        leaderLatch.addListener(this);
        //启动屏障对象
        leaderLatch.start();
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected void serviceStop() throws Exception {
        //关闭选主屏障对象
        closeLeaderLatch();
        //停止父类服务对象
        super.serviceStop();
    }

    // TODO: 17/3/24 by zmyer
    @Override
    public void rejoinElection() {
        try {
            //关闭选主屏障对象
            closeLeaderLatch();
            //等待片刻
            Thread.sleep(1000);
            //重新初始化屏障对象,并启动
            initAndStartLeaderLatch();
        } catch (Exception e) {
            LOG.info("Fail to re-join election.", e);
        }
    }

    // TODO: 17/3/24 by zmyer
    @Override
    public String getZookeeperConnectionState() {
        return "Connected to zookeeper : " + curator.getZookeeperClient().isConnected();
    }

    // TODO: 17/3/24 by zmyer
    @Override
    public void isLeader() {
        LOG.info(rmId + "is elected leader, transitioning to active");
        try {
            //切换状态
            rmContext.getRMAdminService().transitionToActive(
                new HAServiceProtocol.StateChangeRequestInfo(
                    HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC));
        } catch (Exception e) {
            LOG.info(rmId + " failed to transition to active, giving up leadership",
                e);
            //非主节点
            notLeader();
            //重新加入选主
            rejoinElection();
        }
    }

    // TODO: 17/3/24 by zmyer
    private void closeLeaderLatch() throws IOException {
        if (leaderLatch != null) {
            leaderLatch.close();
        }
    }

    // TODO: 17/3/24 by zmyer
    @Override
    public void notLeader() {
        LOG.info(rmId + " relinquish leadership");
        try {
            rmContext.getRMAdminService().transitionToStandby(
                new HAServiceProtocol.StateChangeRequestInfo(
                    HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC));
        } catch (Exception e) {
            LOG.info(rmId + " did not transition to standby successfully.");
        }
    }

    // TODO: 17/3/24 by zmyer
    // only for testing
    @VisibleForTesting
    public CuratorFramework getCuratorClient() {
        return this.curator;
    }
}
