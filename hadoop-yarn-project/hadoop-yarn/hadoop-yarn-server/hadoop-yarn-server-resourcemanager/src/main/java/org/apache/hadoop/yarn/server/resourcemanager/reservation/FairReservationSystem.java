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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

// TODO: 17/3/24 by zmyer
public class FairReservationSystem extends AbstractReservationSystem {

    private FairScheduler fairScheduler;

    public FairReservationSystem() {
        super(FairReservationSystem.class.getName());
    }

    // TODO: 17/3/24 by zmyer
    @Override
    public void reinitialize(Configuration conf, RMContext rmContext)
        throws YarnException {
        // Validate if the scheduler is fair scheduler
        ResourceScheduler scheduler = rmContext.getScheduler();
        if (!(scheduler instanceof FairScheduler)) {
            throw new YarnRuntimeException("Class "
                + scheduler.getClass().getCanonicalName() + " not instance of "
                + FairScheduler.class.getCanonicalName());
        }
        fairScheduler = (FairScheduler) scheduler;
        this.conf = conf;
        super.reinitialize(conf, rmContext);
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected ReservationSchedulerConfiguration
    getReservationSchedulerConfiguration() {
        return fairScheduler.getAllocationConfiguration();
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected ResourceCalculator getResourceCalculator() {
        return fairScheduler.getResourceCalculator();
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected QueueMetrics getRootQueueMetrics() {
        return fairScheduler.getRootQueueMetrics();
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected Resource getMinAllocation() {
        return fairScheduler.getMinimumResourceCapability();
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected Resource getMaxAllocation() {
        return fairScheduler.getMaximumResourceCapability();
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected String getPlanQueuePath(String planQueueName) {
        return planQueueName;
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected Resource getPlanQueueCapacity(String planQueueName) {
        return fairScheduler.getQueueManager().getParentQueue(planQueueName, false)
            .getSteadyFairShare();
    }

}
