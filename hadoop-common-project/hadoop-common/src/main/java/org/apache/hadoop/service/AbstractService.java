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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;

/**
 * This is the base implementation class for services.
 */
// TODO: 17/3/20 by zmyer
@Public
@Evolving
public abstract class AbstractService implements Service {

    private static final Log LOG = LogFactory.getLog(AbstractService.class);

    /**
     * Service name.
     */
    //服务名称
    private final String name;

    /** service state */
    //服务状态机
    private final ServiceStateModel stateModel;

    /**
     * Service start time. Will be zero until the service is started.
     */
    //服务开启时间
    private long startTime;

    /**
     * The configuration. Will be null until the service is initialized.
     */
    //服务配置对象
    private volatile Configuration config;

    /**
     * List of state change listeners; it is final to ensure
     * that it will never be null.
     */
    //服务状态变更监听器列表
    private final ServiceOperations.ServiceListeners listeners
        = new ServiceOperations.ServiceListeners();
    /**
     * Static listeners to all events across all services
     */
    //服务状态变更全局监听器
    private static ServiceOperations.ServiceListeners globalListeners
        = new ServiceOperations.ServiceListeners();

    /**
     * The cause of any failure -will be null.
     * if a service did not stop due to a failure.
     */
    //异常对象
    private Exception failureCause;

    /**
     * the state in which the service was when it failed.
     * Only valid when the service is stopped due to a failure
     */
    //失败的状态对象
    private STATE failureState = null;

    /**
     * object used to co-ordinate {@link #waitForServiceToStop(long)}
     * across threads.
     */
    // TODO: 17/3/20 by zmyer
    private final AtomicBoolean terminationNotification = new AtomicBoolean(false);

    /**
     * History of lifecycle transitions
     */
    //状态转移历史记录集合
    private final List<LifecycleEvent> lifecycleHistory = new ArrayList<LifecycleEvent>(5);

    /**
     * Map of blocking dependencies
     */
    //内存块依赖映射表
    private final Map<String, String> blockerMap = new HashMap<String, String>();

    //
    private final Object stateChangeLock = new Object();

    /**
     * Construct the service.
     *
     * @param name service name
     */
    // TODO: 17/3/20 by zmyer
    public AbstractService(String name) {
        this.name = name;
        stateModel = new ServiceStateModel(name);
    }

    // TODO: 17/3/20 by zmyer
    @Override
    public final STATE getServiceState() {
        return stateModel.getState();
    }

    // TODO: 17/3/20 by zmyer
    @Override
    public final synchronized Throwable getFailureCause() {
        return failureCause;
    }

    // TODO: 17/3/20 by zmyer
    @Override
    public synchronized STATE getFailureState() {
        return failureState;
    }

    /**
     * Set the configuration for this service.
     * This method is called during {@link #init(Configuration)}
     * and should only be needed if for some reason a service implementation
     * needs to override that initial setting -for example replacing
     * it with a new subclass of {@link Configuration}
     *
     * @param conf new configuration.
     */
    // TODO: 17/3/20 by zmyer
    protected void setConfig(Configuration conf) {
        this.config = conf;
    }

    /**
     * {@inheritDoc}
     * This invokes {@link #serviceInit}
     *
     * @param conf the configuration of the service. This must not be null
     * @throws ServiceStateException if the configuration was null, the state change not permitted, or something else
     * went wrong
     */
    // TODO: 17/3/20 by zmyer
    @Override
    public void init(Configuration conf) {
        if (conf == null) {
            throw new ServiceStateException("Cannot initialize service "
                + getName() + ": null configuration");
        }
        //如果服务已经初始化了,则直接跳过
        if (isInState(STATE.INITED)) {
            return;
        }
        synchronized (stateChangeLock) {
            if (enterState(STATE.INITED) != STATE.INITED) {
                //设置配置
                setConfig(conf);
                try {
                    //初始化服务
                    serviceInit(config);
                    if (isInState(STATE.INITED)) {
                        //if the service ended up here during init,
                        //notify the listeners
                        //发送公告
                        notifyListeners();
                    }
                } catch (Exception e) {
                    //处理服务初始化失败
                    noteFailure(e);
                    ServiceOperations.stopQuietly(LOG, this);
                    throw ServiceStateException.convert(e);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws ServiceStateException if the current service state does not permit this action
     */
    // TODO: 17/3/22 by zmyer
    @Override
    public void start() {
        //服务已经启动过,则直接跳过
        if (isInState(STATE.STARTED)) {
            return;
        }
        //enter the started state
        synchronized (stateChangeLock) {
            if (stateModel.enterState(STATE.STARTED) != STATE.STARTED) {
                try {
                    startTime = System.currentTimeMillis();
                    //启动服务
                    serviceStart();
                    if (isInState(STATE.STARTED)) {
                        //if the service started (and isn't now in a later state), notify
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Service " + getName() + " is started");
                        }
                        //发送公告
                        notifyListeners();
                    }
                } catch (Exception e) {
                    //处理失败
                    noteFailure(e);
                    ServiceOperations.stopQuietly(LOG, this);
                    throw ServiceStateException.convert(e);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    // TODO: 17/3/22 by zmyer
    @Override
    public void stop() {
        if (isInState(STATE.STOPPED)) {
            return;
        }
        synchronized (stateChangeLock) {
            if (enterState(STATE.STOPPED) != STATE.STOPPED) {
                try {
                    //停止服务
                    serviceStop();
                } catch (Exception e) {
                    //stop-time exceptions are logged if they are the first one,
                    noteFailure(e);
                    throw ServiceStateException.convert(e);
                } finally {
                    //report that the service has terminated
                    //设置停止公告服务
                    terminationNotification.set(true);
                    synchronized (terminationNotification) {
                        //发送终止公告
                        terminationNotification.notifyAll();
                    }
                    //notify anything listening for events
                    //发送公告
                    notifyListeners();
                }
            } else {
                //already stopped: note it
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring re-entrant call to stop()");
                }
            }
        }
    }

    /**
     * Relay to {@link #stop()}
     *
     * @throws IOException
     */
    // TODO: 17/3/22 by zmyer
    @Override
    public final void close() throws IOException {
        stop();
    }

    /**
     * Failure handling: record the exception
     * that triggered it -if there was not one already.
     * Services are free to call this themselves.
     *
     * @param exception the exception
     */
    // TODO: 17/3/22 by zmyer
    protected final void noteFailure(Exception exception) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("noteFailure " + exception, null);
        }
        if (exception == null) {
            //make sure failure logic doesn't itself cause problems
            return;
        }
        //record the failure details, and log it
        synchronized (this) {
            if (failureCause == null) {
                failureCause = exception;
                failureState = getServiceState();
                LOG.info("Service " + getName()
                        + " failed in state " + failureState
                        + "; cause: " + exception,
                    exception);
            }
        }
    }

    // TODO: 17/3/22 by zmyer
    @Override
    public final boolean waitForServiceToStop(long timeout) {
        //读取服务终止公告
        boolean completed = terminationNotification.get();
        while (!completed) {
            try {
                synchronized (terminationNotification) {
                    //等待终止公告
                    terminationNotification.wait(timeout);
                }
                // here there has been a timeout, the object has terminated,
                // or there has been a spurious wakeup (which we ignore)
                //设置完成标记
                completed = true;
            } catch (InterruptedException e) {
                // interrupted; have another look at the flag
                //读取完成标记
                completed = terminationNotification.get();
            }
        }
        //返回完成标记
        return terminationNotification.get();
    }

  /* ===================================================================== */
  /* Override Points */
  /* ===================================================================== */

    /**
     * All initialization code needed by a service.
     *
     * This method will only ever be called once during the lifecycle of
     * a specific service instance.
     *
     * Implementations do not need to be synchronized as the logic
     * in {@link #init(Configuration)} prevents re-entrancy.
     *
     * The base implementation checks to see if the subclass has created
     * a new configuration instance, and if so, updates the base class value
     *
     * @param conf configuration
     * @throws Exception on a failure -these will be caught, possibly wrapped, and wil; trigger a service stop
     */
    // TODO: 17/3/22 by zmyer
    protected void serviceInit(Configuration conf) throws Exception {
        if (conf != config) {
            LOG.debug("Config has been overridden during init");
            //设置配置文件
            setConfig(conf);
        }
    }

    /**
     * Actions called during the INITED to STARTED transition.
     *
     * This method will only ever be called once during the lifecycle of
     * a specific service instance.
     *
     * Implementations do not need to be synchronized as the logic
     * in {@link #start()} prevents re-entrancy.
     *
     * @throws Exception if needed -these will be caught, wrapped, and trigger a service stop
     */
    // TODO: 17/3/22 by zmyer
    protected void serviceStart() throws Exception {

    }

    /**
     * Actions called during the transition to the STOPPED state.
     *
     * This method will only ever be called once during the lifecycle of
     * a specific service instance.
     *
     * Implementations do not need to be synchronized as the logic
     * in {@link #stop()} prevents re-entrancy.
     *
     * Implementations MUST write this to be robust against failures, including
     * checks for null references -and for the first failure to not stop other
     * attempts to shut down parts of the service.
     *
     * @throws Exception if needed -these will be caught and logged.
     */
    // TODO: 17/3/22 by zmyer
    protected void serviceStop() throws Exception {

    }

    // TODO: 17/3/22 by zmyer
    @Override
    public void registerServiceListener(ServiceStateChangeListener l) {
        listeners.add(l);
    }

    // TODO: 17/3/22 by zmyer
    @Override
    public void unregisterServiceListener(ServiceStateChangeListener l) {
        listeners.remove(l);
    }

    /**
     * Register a global listener, which receives notifications
     * from the state change events of all services in the JVM
     *
     * @param l listener
     */
    // TODO: 17/3/22 by zmyer
    public static void registerGlobalListener(ServiceStateChangeListener l) {
        globalListeners.add(l);
    }

    /**
     * unregister a global listener.
     *
     * @param l listener to unregister
     * @return true if the listener was found (and then deleted)
     */
    // TODO: 17/3/22 by zmyer
    public static boolean unregisterGlobalListener(ServiceStateChangeListener l) {
        return globalListeners.remove(l);
    }

    /**
     * Package-scoped method for testing -resets the global listener list
     */
    // TODO: 17/3/22 by zmyer
    @VisibleForTesting
    static void resetGlobalListeners() {
        globalListeners.reset();
    }

    // TODO: 17/3/22 by zmyer
    @Override
    public String getName() {
        return name;
    }

    // TODO: 17/3/22 by zmyer
    @Override
    public Configuration getConfig() {
        return config;
    }

    // TODO: 17/3/22 by zmyer
    @Override
    public long getStartTime() {
        return startTime;
    }

    /**
     * Notify local and global listeners of state changes.
     * Exceptions raised by listeners are NOT passed up.
     */
    // TODO: 17/3/22 by zmyer
    private void notifyListeners() {
        try {
            //开始执行所有注册的监听器
            listeners.notifyListeners(this);
            //执行所有注册的全局监听器
            globalListeners.notifyListeners(this);
        } catch (Throwable e) {
            LOG.warn("Exception while notifying listeners of " + this + ": " + e,
                e);
        }
    }

    /**
     * Add a state change event to the lifecycle history
     */
    // TODO: 17/3/22 by zmyer
    private void recordLifecycleEvent() {
        //生命周期事件对象
        LifecycleEvent event = new LifecycleEvent();
        //设置时间戳
        event.time = System.currentTimeMillis();
        //设置服务状态
        event.state = getServiceState();
        //将当前的事件对象注册到服务生命周期历史记录中
        lifecycleHistory.add(event);
    }

    // TODO: 17/3/22 by zmyer
    @Override
    public synchronized List<LifecycleEvent> getLifecycleHistory() {
        //读取服务生命周期历史记录
        return new ArrayList<LifecycleEvent>(lifecycleHistory);
    }

    /**
     * Enter a state; record this via {@link #recordLifecycleEvent}
     * and log at the info level.
     *
     * @param newState the proposed new state
     * @return the original state it wasn't already in that state, and the state model permits state re-entrancy.
     */
    // TODO: 17/3/22 by zmyer
    private STATE enterState(STATE newState) {
        assert stateModel != null : "null state in " + name + " " + this.getClass();
        //替换新的服务状态
        STATE oldState = stateModel.enterState(newState);
        if (oldState != newState) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Service: " + getName() + " entered state " + getServiceState());
            }
            //记录当前的服务状态
            recordLifecycleEvent();
        }
        //返回旧的状态
        return oldState;
    }

    // TODO: 17/3/22 by zmyer
    @Override
    public final boolean isInState(Service.STATE expected) {
        return stateModel.isInState(expected);
    }

    // TODO: 17/3/22 by zmyer
    @Override
    public String toString() {
        return "Service " + name + " in state " + stateModel;
    }

    /**
     * Put a blocker to the blocker map -replacing any
     * with the same name.
     *
     * @param name blocker name
     * @param details any specifics on the block. This must be non-null.
     */
    // TODO: 17/3/22 by zmyer
    protected void putBlocker(String name, String details) {
        synchronized (blockerMap) {
            blockerMap.put(name, details);
        }
    }

    /**
     * Remove a blocker from the blocker map -
     * this is a no-op if the blocker is not present
     *
     * @param name the name of the blocker
     */
    // TODO: 17/3/22 by zmyer
    public void removeBlocker(String name) {
        synchronized (blockerMap) {
            blockerMap.remove(name);
        }
    }

    // TODO: 17/3/22 by zmyer
    @Override
    public Map<String, String> getBlockers() {
        synchronized (blockerMap) {
            Map<String, String> map = new HashMap<String, String>(blockerMap);
            return map;
        }
    }
}
