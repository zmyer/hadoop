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

package org.apache.hadoop.yarn.server.resourcemanager.ahs;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.applicationhistoryservice.NullApplicationHistoryStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerStartData;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * <p>
 * {@link ResourceManager} uses this class to write the information of
 * {@link RMApp}, {@link RMAppAttempt} and {@link RMContainer}. These APIs are
 * non-blocking, and just schedule a writing history event. An self-contained
 * dispatcher vector will handle the event in separate threads, and extract the
 * required fields that are going to be persisted. Then, the extracted
 * information will be persisted via the implementation of
 * {@link ApplicationHistoryStore}.
 * </p>
 */
// TODO: 17/3/22 by zmyer
@Private
@Unstable
public class RMApplicationHistoryWriter extends CompositeService {

    public static final Log LOG = LogFactory.getLog(RMApplicationHistoryWriter.class);
    //事件分发器对象
    private Dispatcher dispatcher;
    //app历史写入对象
    @VisibleForTesting
    ApplicationHistoryWriter writer;
    //
    @VisibleForTesting
    boolean historyServiceEnabled;

    // TODO: 17/3/22 by zmyer
    public RMApplicationHistoryWriter() {
        super(RMApplicationHistoryWriter.class.getName());
    }

    // TODO: 17/3/24 by zmyer
    @Override
    protected synchronized void serviceInit(Configuration conf) throws Exception {
        historyServiceEnabled = conf.getBoolean(YarnConfiguration.APPLICATION_HISTORY_ENABLED,
                YarnConfiguration.DEFAULT_APPLICATION_HISTORY_ENABLED);
        if (conf.get(YarnConfiguration.APPLICATION_HISTORY_STORE) == null ||
            conf.get(YarnConfiguration.APPLICATION_HISTORY_STORE).length() == 0 ||
            conf.get(YarnConfiguration.APPLICATION_HISTORY_STORE).equals(
                NullApplicationHistoryStore.class.getName())) {
            historyServiceEnabled = false;
        }

        // Only create the services when the history service is enabled and not
        // using the null store, preventing wasting the system resources.
        if (historyServiceEnabled) {
            //创建app历史记录存储对象
            writer = createApplicationHistoryStore(conf);
            //将该服务对象插入到服务列表中
            addIfService(writer);
            //创建事件分发对象
            dispatcher = createDispatcher(conf);
            //在事件分发对象中注册转发事件处理器对象
            dispatcher.register(WritingHistoryEventType.class, new ForwardingEventHandler());
            //将该事件分发对象插入到服务列表中
            addIfService(dispatcher);
        }
        //初始化父类服务
        super.serviceInit(conf);
    }

    // TODO: 17/3/24 by zmyer
    protected Dispatcher createDispatcher(Configuration conf) {
        //创建多线程事件分发器对象
        MultiThreadedDispatcher dispatcher = new MultiThreadedDispatcher(
                conf.getInt(YarnConfiguration.RM_HISTORY_WRITER_MULTI_THREADED_DISPATCHER_POOL_SIZE,
                        YarnConfiguration.DEFAULT_RM_HISTORY_WRITER_MULTI_THREADED_DISPATCHER_POOL_SIZE));
        dispatcher.setDrainEventsOnStop();
        //返回事件分发器对象
        return dispatcher;
    }

    // TODO: 17/3/24 by zmyer
    protected ApplicationHistoryStore createApplicationHistoryStore(Configuration conf) {
        try {
            //读取存储类对象
            Class<? extends ApplicationHistoryStore> storeClass =
                conf.getClass(YarnConfiguration.APPLICATION_HISTORY_STORE,
                    NullApplicationHistoryStore.class, ApplicationHistoryStore.class);
            //创建存储对象
            return storeClass.newInstance();
        } catch (Exception e) {
            String msg = "Could not instantiate ApplicationHistoryWriter: "
                    + conf.get(YarnConfiguration.APPLICATION_HISTORY_STORE,
                    NullApplicationHistoryStore.class.getName());
            LOG.error(msg, e);
            throw new YarnRuntimeException(msg, e);
        }
    }

    // TODO: 17/3/24 by zmyer
    protected void handleWritingApplicationHistoryEvent(
        WritingApplicationHistoryEvent event) {
        switch (event.getType()) {
            case APP_START:
                //创建app启动事件对象
                WritingApplicationStartEvent wasEvent = (WritingApplicationStartEvent) event;
                try {
                    writer.applicationStarted(wasEvent.getApplicationStartData());
                    LOG.info("Stored the start data of application " + wasEvent.getApplicationId());
                } catch (IOException e) {
                    LOG.error("Error when storing the start data of application "
                        + wasEvent.getApplicationId());
                }
                break;
            case APP_FINISH:
                WritingApplicationFinishEvent wafEvent = (WritingApplicationFinishEvent) event;
                try {
                    writer.applicationFinished(wafEvent.getApplicationFinishData());
                    LOG.info("Stored the finish data of application " + wafEvent.getApplicationId());
                } catch (IOException e) {
                    LOG.error("Error when storing the finish data of application "
                        + wafEvent.getApplicationId());
                }
                break;
            case APP_ATTEMPT_START:
                WritingApplicationAttemptStartEvent waasEvent = (WritingApplicationAttemptStartEvent) event;
                try {
                    writer.applicationAttemptStarted(waasEvent.getApplicationAttemptStartData());
                    LOG.info("Stored the start data of application attempt "
                        + waasEvent.getApplicationAttemptId());
                } catch (IOException e) {
                    LOG.error("Error when storing the start data of application attempt "
                        + waasEvent.getApplicationAttemptId());
                }
                break;
            case APP_ATTEMPT_FINISH:
                WritingApplicationAttemptFinishEvent waafEvent = (WritingApplicationAttemptFinishEvent) event;
                try {
                    writer.applicationAttemptFinished(waafEvent.getApplicationAttemptFinishData());
                    LOG.info("Stored the finish data of application attempt "
                        + waafEvent.getApplicationAttemptId());
                } catch (IOException e) {
                    LOG
                        .error("Error when storing the finish data of application attempt "
                            + waafEvent.getApplicationAttemptId());
                }
                break;
            case CONTAINER_START:
                WritingContainerStartEvent wcsEvent = (WritingContainerStartEvent) event;
                try {
                    writer.containerStarted(wcsEvent.getContainerStartData());
                    LOG.info("Stored the start data of container "
                        + wcsEvent.getContainerId());
                } catch (IOException e) {
                    LOG.error("Error when storing the start data of container "
                        + wcsEvent.getContainerId());
                }
                break;
            case CONTAINER_FINISH:
                WritingContainerFinishEvent wcfEvent =
                    (WritingContainerFinishEvent) event;
                try {
                    writer.containerFinished(wcfEvent.getContainerFinishData());
                    LOG.info("Stored the finish data of container "
                        + wcfEvent.getContainerId());
                } catch (IOException e) {
                    LOG.error("Error when storing the finish data of container "
                        + wcfEvent.getContainerId());
                }
                break;
            default:
                LOG.error("Unknown WritingApplicationHistoryEvent type: "
                    + event.getType());
        }
    }

    // TODO: 17/4/3 by zmyer
    @SuppressWarnings("unchecked")
    public void applicationStarted(RMApp app) {
        if (historyServiceEnabled) {
            //如果开启了历史服务,则由事件分发器处理app启动事件
            dispatcher.getEventHandler().handle(new WritingApplicationStartEvent(app.getApplicationId(),
                    ApplicationStartData.newInstance(app.getApplicationId(), app.getName(),
                        app.getApplicationType(), app.getQueue(), app.getUser(),
                        app.getSubmitTime(), app.getStartTime())));
        }
    }

    // TODO: 17/4/3 by zmyer
    @SuppressWarnings("unchecked")
    public void applicationFinished(RMApp app, RMAppState finalState) {
        if (historyServiceEnabled) {
            //事件分发器处理app完成事件
            dispatcher.getEventHandler().handle(
                new WritingApplicationFinishEvent(app.getApplicationId(),
                    ApplicationFinishData.newInstance(app.getApplicationId(),
                        app.getFinishTime(), app.getDiagnostics().toString(),
                        app.getFinalApplicationStatus(),
                        RMServerUtils.createApplicationState(finalState))));
        }
    }

    // TODO: 17/4/3 by zmyer
    @SuppressWarnings("unchecked")
    public void applicationAttemptStarted(RMAppAttempt appAttempt) {
        if (historyServiceEnabled) {
            dispatcher.getEventHandler().handle(
                new WritingApplicationAttemptStartEvent(appAttempt.getAppAttemptId(),
                    ApplicationAttemptStartData.newInstance(appAttempt.getAppAttemptId(),
                        appAttempt.getHost(), appAttempt.getRpcPort(), appAttempt
                            .getMasterContainer().getId())));
        }
    }

    // TODO: 17/4/3 by zmyer
    @SuppressWarnings("unchecked")
    public void applicationAttemptFinished(RMAppAttempt appAttempt,
        RMAppAttemptState finalState) {
        if (historyServiceEnabled) {
            dispatcher.getEventHandler().handle(
                new WritingApplicationAttemptFinishEvent(appAttempt.getAppAttemptId(),
                    ApplicationAttemptFinishData.newInstance(
                        appAttempt.getAppAttemptId(), appAttempt.getDiagnostics(),
                        appAttempt.getTrackingUrl(), appAttempt.getFinalApplicationStatus(),
                        RMServerUtils.createApplicationAttemptState(finalState))));
        }
    }

    // TODO: 17/4/3 by zmyer
    @SuppressWarnings("unchecked")
    public void containerStarted(RMContainer container) {
        if (historyServiceEnabled) {
            dispatcher.getEventHandler().handle(
                new WritingContainerStartEvent(container.getContainerId(),
                    ContainerStartData.newInstance(container.getContainerId(),
                        container.getAllocatedResource(), container.getAllocatedNode(),
                        container.getAllocatedPriority(), container.getCreationTime())));
        }
    }

    // TODO: 17/4/3 by zmyer
    @SuppressWarnings("unchecked")
    public void containerFinished(RMContainer container) {
        if (historyServiceEnabled) {
            dispatcher.getEventHandler().handle(
                new WritingContainerFinishEvent(container.getContainerId(),
                    ContainerFinishData.newInstance(container.getContainerId(),
                        container.getFinishTime(), container.getDiagnosticsInfo(),
                        container.getContainerExitStatus(),
                        container.getContainerState())));
        }
    }

    /**
     * EventHandler implementation which forward events to HistoryWriter Making
     * use of it, HistoryWriter can avoid to have a public handle method
     */
    // TODO: 17/4/3 by zmyer
    private final class ForwardingEventHandler implements EventHandler<WritingApplicationHistoryEvent> {

        // TODO: 17/4/3 by zmyer
        @Override
        public void handle(WritingApplicationHistoryEvent event) {
            handleWritingApplicationHistoryEvent(event);
        }

    }

    // TODO: 17/4/3 by zmyer
    @SuppressWarnings({"rawtypes", "unchecked"})
    protected static class MultiThreadedDispatcher extends CompositeService implements Dispatcher {

        //事件分发器列表
        private List<AsyncDispatcher> dispatchers = new ArrayList<AsyncDispatcher>();

        // TODO: 17/4/3 by zmyer
        public MultiThreadedDispatcher(int num) {
            super(MultiThreadedDispatcher.class.getName());
            for (int i = 0; i < num; ++i) {
                //创建异步事件分发器对象
                AsyncDispatcher dispatcher = createDispatcher();
                //将新创建的事件分发器对象插入到列表中
                dispatchers.add(dispatcher);
                //将事件分发器插入到服务列表中
                addIfService(dispatcher);
            }
        }

        // TODO: 17/4/3 by zmyer
        @Override
        public EventHandler<Event> getEventHandler() {
            return new CompositEventHandler();
        }

        // TODO: 17/4/3 by zmyer
        @Override
        public void register(Class<? extends Enum> eventType, EventHandler handler) {
            for (AsyncDispatcher dispatcher : dispatchers) {
                dispatcher.register(eventType, handler);
            }
        }

        // TODO: 17/4/3 by zmyer
        public void setDrainEventsOnStop() {
            for (AsyncDispatcher dispatcher : dispatchers) {
                dispatcher.setDrainEventsOnStop();
            }
        }

        // TODO: 17/4/3 by zmyer
        private class CompositEventHandler implements EventHandler<Event> {

            // TODO: 17/4/3 by zmyer
            @Override
            public void handle(Event event) {
                // Use hashCode (of ApplicationId) to dispatch the event to the child
                // dispatcher, such that all the writing events of one application will
                // be handled by one thread, the scheduled order of the these events
                // will be preserved
                //计算事件分发器索引
                int index = (event.hashCode() & Integer.MAX_VALUE) % dispatchers.size();
                dispatchers.get(index).getEventHandler().handle(event);
            }

        }

        // TODO: 17/4/3 by zmyer
        protected AsyncDispatcher createDispatcher() {
            return new AsyncDispatcher();
        }

    }

}
