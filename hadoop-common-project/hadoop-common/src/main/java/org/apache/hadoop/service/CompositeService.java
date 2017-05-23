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

package org.apache.hadoop.service;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;

/**
 * Composition of services.
 */
// TODO: 17/3/22 by zmyer
@Public
@Evolving
public class CompositeService extends AbstractService {

    private static final Log LOG = LogFactory.getLog(CompositeService.class);

    /**
     * Policy on shutdown: attempt to close everything (purest) or
     * only try to close started services (which assumes
     * that the service implementations may not handle the stop() operation
     * except when started.
     * Irrespective of this policy, if a child service fails during
     * its init() or start() operations, it will have stop() called on it.
     */
    static final boolean STOP_ONLY_STARTED_SERVICES = false;

    //服务列表
    private final List<Service> serviceList = new ArrayList<Service>();

    // TODO: 17/3/22 by zmyer
    public CompositeService(String name) {
        super(name);
    }

    /**
     * Get a cloned list of services
     *
     * @return a list of child services at the time of invocation - added services will not be picked up.
     */
    // TODO: 17/3/22 by zmyer
    public List<Service> getServices() {
        synchronized (serviceList) {
            return new ArrayList<>(serviceList);
        }
    }

    /**
     * Add the passed {@link Service} to the list of services managed by this
     * {@link CompositeService}
     *
     * @param service the {@link Service} to be added
     */
    // TODO: 17/3/22 by zmyer
    protected void addService(Service service) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding service " + service.getName());
        }
        synchronized (serviceList) {
            serviceList.add(service);
        }
    }

    /**
     * If the passed object is an instance of {@link Service},
     * add it to the list of services managed by this {@link CompositeService}
     *
     * @param object
     * @return true if a service is added, false otherwise.
     */
    // TODO: 17/3/22 by zmyer
    protected boolean addIfService(Object object) {
        if (object instanceof Service) {
            addService((Service) object);
            return true;
        } else {
            return false;
        }
    }

    // TODO: 17/3/22 by zmyer
    protected synchronized boolean removeService(Service service) {
        synchronized (serviceList) {
            return serviceList.remove(service);
        }
    }

    // TODO: 17/3/22 by zmyer
    protected void serviceInit(Configuration conf) throws Exception {
        //读取服务列表
        List<Service> services = getServices();
        if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": initing services, size=" + services.size());
        }
        for (Service service : services) {
            //每个服务初始化
            service.init(conf);
        }
        //父类初始化
        super.serviceInit(conf);
    }

    // TODO: 17/3/22 by zmyer
    protected void serviceStart() throws Exception {
        //读取服务列表
        List<Service> services = getServices();
        if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": starting services, size=" + services.size());
        }
        for (Service service : services) {
            // start the service. If this fails that service
            // will be stopped and an exception raised
            //启动每个服务
            service.start();
        }
        //启动父类服务
        super.serviceStart();
    }

    // TODO: 17/3/22 by zmyer
    protected void serviceStop() throws Exception {
        //stop all services that were started
        int numOfServicesToStop = serviceList.size();
        if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": stopping services, size=" + numOfServicesToStop);
        }
        stop(numOfServicesToStop, STOP_ONLY_STARTED_SERVICES);
        super.serviceStop();
    }

    /**
     * Stop the services in reverse order
     *
     * @param numOfServicesStarted index from where the stop should work
     * @param stopOnlyStartedServices flag to say "only start services that are started, not those that are NOTINITED or
     * INITED.
     * @throws RuntimeException the first exception raised during the stop process -<i>after all services are
     * stopped</i>
     */
    // TODO: 17/3/22 by zmyer
    private void stop(int numOfServicesStarted, boolean stopOnlyStartedServices) {
        // stop in reverse order of start
        Exception firstException = null;
        //读取服务列表
        List<Service> services = getServices();
        for (int i = numOfServicesStarted - 1; i >= 0; i--) {
            //读取每个服务对象
            Service service = services.get(i);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Stopping service #" + i + ": " + service);
            }
            //读取服务对象的状态信息
            STATE state = service.getServiceState();
            //depending on the stop police
            if (state == STATE.STARTED
                || (!stopOnlyStartedServices && state == STATE.INITED)) {
                //停止服务
                Exception ex = ServiceOperations.stopQuietly(LOG, service);
                if (ex != null && firstException == null) {
                    firstException = ex;
                }
            }
        }
        //after stopping all services, rethrow the first exception raised
        if (firstException != null) {
            throw ServiceStateException.convert(firstException);
        }
    }

    /**
     * JVM Shutdown hook for CompositeService which will stop the give
     * CompositeService gracefully in case of JVM shutdown.
     */
    // TODO: 17/3/22 by zmyer
    public static class CompositeServiceShutdownHook implements Runnable {

        //复合服务对象
        private CompositeService compositeService;

        // TODO: 17/3/22 by zmyer
        public CompositeServiceShutdownHook(CompositeService compositeService) {
            this.compositeService = compositeService;
        }

        // TODO: 17/3/22 by zmyer
        @Override
        public void run() {
            ServiceOperations.stopQuietly(compositeService);
        }
    }

}
