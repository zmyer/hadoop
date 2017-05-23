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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the implementation of {@link ReservationSystem} based on the
 * {@link CapacityScheduler}
 */
// TODO: 17/3/24 by zmyer
@LimitedPrivate("yarn")
@Unstable
public class CapacityReservationSystem extends AbstractReservationSystem {

    private static final Logger LOG = LoggerFactory.getLogger(CapacityReservationSystem.class);

    //容量调度对象
    private CapacityScheduler capScheduler;

    // TODO: 17/3/24 by zmyer
    public CapacityReservationSystem() {
        super(CapacityReservationSystem.class.getName());
    }

    // TODO: 17/3/24 by zmyer
    @Override
    public void reinitialize(Configuration conf, RMContext rmContext)
        throws YarnException {
        // Validate if the scheduler is capacity based
        //读取资源调度对象
        ResourceScheduler scheduler = rmContext.getScheduler();
        if (!(scheduler instanceof CapacityScheduler)) {
            throw new YarnRuntimeException("Class "
                + scheduler.getClass().getCanonicalName() + " not instance of "
                + CapacityScheduler.class.getCanonicalName());
        }
        capScheduler = (CapacityScheduler) scheduler;
        this.conf = conf;
        //父类初始化
        super.reinitialize(conf, rmContext);
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected Resource getMinAllocation() {
        return capScheduler.getMinimumResourceCapability();
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected Resource getMaxAllocation() {
        return capScheduler.getMaximumResourceCapability();
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected ResourceCalculator getResourceCalculator() {
        return capScheduler.getResourceCalculator();
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected QueueMetrics getRootQueueMetrics() {
        return capScheduler.getRootQueueMetrics();
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected String getPlanQueuePath(String planQueueName) {
        return capScheduler.getQueue(planQueueName).getQueuePath();
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected Resource getPlanQueueCapacity(String planQueueName) {
        Resource minAllocation = getMinAllocation();
        ResourceCalculator rescCalc = getResourceCalculator();
        CSQueue planQueue = capScheduler.getQueue(planQueueName);
        return rescCalc.multiplyAndNormalizeDown(capScheduler.getClusterResource(),
            planQueue.getAbsoluteCapacity(), minAllocation);
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected ReservationSchedulerConfiguration
    getReservationSchedulerConfiguration() {
        return capScheduler.getConfiguration();
    }

}
