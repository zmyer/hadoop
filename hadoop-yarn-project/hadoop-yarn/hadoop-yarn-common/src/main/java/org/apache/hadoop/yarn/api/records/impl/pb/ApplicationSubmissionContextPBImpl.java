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

package org.apache.hadoop.yarn.api.records.impl.pb;

import com.google.common.base.CharMatcher;
import com.google.protobuf.TextFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSubmissionContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSubmissionContextProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationTimeoutMapProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerLaunchContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LogAggregationContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceRequestProto;

// TODO: 17/3/25 by zmyer
@Private
@Unstable
public class ApplicationSubmissionContextPBImpl extends ApplicationSubmissionContext {
    //应用提交上下文协议对象
    ApplicationSubmissionContextProto proto =
        ApplicationSubmissionContextProto.getDefaultInstance();
    ApplicationSubmissionContextProto.Builder builder = null;
    boolean viaProto = false;

    //应用id
    private ApplicationId applicationId = null;
    //优先级
    private Priority priority = null;
    //容器加载上下文对象
    private ContainerLaunchContext amContainer = null;
    //资源对象
    private Resource resource = null;
    //应用tag集合
    private Set<String> applicationTags = null;
    //资源请求对象
    private ResourceRequest amResourceRequest = null;
    //
    private LogAggregationContext logAggregationContext = null;
    //
    private ReservationId reservationId = null;
    //应用超时映射表
    private Map<ApplicationTimeoutType, Long> applicationTimeouts = null;

    // TODO: 17/3/25 by zmyer
    public ApplicationSubmissionContextPBImpl() {
        builder = ApplicationSubmissionContextProto.newBuilder();
    }

    // TODO: 17/3/25 by zmyer
    public ApplicationSubmissionContextPBImpl(
        ApplicationSubmissionContextProto proto) {
        this.proto = proto;
        viaProto = true;
    }

    // TODO: 17/3/25 by zmyer
    public ApplicationSubmissionContextProto getProto() {
        mergeLocalToProto();
        proto = viaProto ? proto : builder.build();
        viaProto = true;
        return proto;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public int hashCode() {
        return getProto().hashCode();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        if (other.getClass().isAssignableFrom(this.getClass())) {
            return this.getProto().equals(this.getClass().cast(other).getProto());
        }
        return false;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public String toString() {
        return TextFormat.shortDebugString(getProto());
    }

    // TODO: 17/3/25 by zmyer
    private void mergeLocalToBuilder() {
        //设置应用id
        if (this.applicationId != null) {
            builder.setApplicationId(convertToProtoFormat(this.applicationId));
        }
        //设置优先级
        if (this.priority != null) {
            builder.setPriority(convertToProtoFormat(this.priority));
        }
        //设置容器加载上下文对象
        if (this.amContainer != null) {
            builder.setAmContainerSpec(convertToProtoFormat(this.amContainer));
        }
        //设置资源对象
        if (this.resource != null &&
            !((ResourcePBImpl) this.resource).getProto().equals(
                builder.getResource())) {
            builder.setResource(convertToProtoFormat(this.resource));
        }
        //设置应用tag集合
        if (this.applicationTags != null && !this.applicationTags.isEmpty()) {
            builder.clearApplicationTags();
            builder.addAllApplicationTags(this.applicationTags);
        }
        //设置向rm请求资源对象
        if (this.amResourceRequest != null) {
            builder.setAmContainerResourceRequest(
                convertToProtoFormat(this.amResourceRequest));
        }
        //
        if (this.logAggregationContext != null) {
            builder.setLogAggregationContext(
                convertToProtoFormat(this.logAggregationContext));
        }
        if (this.reservationId != null) {
            builder.setReservationId(convertToProtoFormat(this.reservationId));
        }
        //设置应用超时时间
        if (this.applicationTimeouts != null) {
            addApplicationTimeouts();
        }
    }

    // TODO: 17/3/25 by zmyer
    private void mergeLocalToProto() {
        if (viaProto)
            maybeInitBuilder();
        mergeLocalToBuilder();
        proto = builder.build();
        viaProto = true;
    }

    // TODO: 17/3/25 by zmyer
    private void maybeInitBuilder() {
        if (viaProto || builder == null) {
            //创建构造应用提交上下文对象构造器
            builder = ApplicationSubmissionContextProto.newBuilder(proto);
        }
        viaProto = false;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public Priority getPriority() {
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        if (this.priority != null) {
            //返回优先级
            return this.priority;
        }

        if (!p.hasPriority()) {
            return null;
        }
        this.priority = convertFromProtoFormat(p.getPriority());
        return this.priority;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setPriority(Priority priority) {
        maybeInitBuilder();
        if (priority == null)
            builder.clearPriority();
        //设置优先级
        this.priority = priority;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public ApplicationId getApplicationId() {
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        if (this.applicationId != null) {
            return applicationId;
        } // Else via proto
        if (!p.hasApplicationId()) {
            return null;
        }
        //读取应用id
        applicationId = convertFromProtoFormat(p.getApplicationId());
        //返回应用id
        return applicationId;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setApplicationId(ApplicationId applicationId) {
        maybeInitBuilder();
        if (applicationId == null)
            builder.clearApplicationId();
        //设置应用id
        this.applicationId = applicationId;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public String getApplicationName() {
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasApplicationName()) {
            return null;
        }
        //返回应用名称
        return (p.getApplicationName());
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setApplicationName(String applicationName) {
        maybeInitBuilder();
        if (applicationName == null) {
            builder.clearApplicationName();
            return;
        }
        builder.setApplicationName((applicationName));
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public String getQueue() {
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasQueue()) {
            return null;
        }
        return (p.getQueue());
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public String getApplicationType() {
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasApplicationType()) {
            return null;
        }
        return (p.getApplicationType());
    }

    // TODO: 17/3/25 by zmyer
    private void initApplicationTags() {
        if (this.applicationTags != null) {
            return;
        }
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        this.applicationTags = new HashSet<>();
        this.applicationTags.addAll(p.getApplicationTagsList());
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public Set<String> getApplicationTags() {
        initApplicationTags();
        return this.applicationTags;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setQueue(String queue) {
        maybeInitBuilder();
        if (queue == null) {
            builder.clearQueue();
            return;
        }
        builder.setQueue((queue));
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setApplicationType(String applicationType) {
        maybeInitBuilder();
        if (applicationType == null) {
            builder.clearApplicationType();
            return;
        }
        builder.setApplicationType((applicationType));
    }

    // TODO: 17/3/25 by zmyer
    private void checkTags(Set<String> tags) {
        if (tags.size() > YarnConfiguration.APPLICATION_MAX_TAGS) {
            throw new IllegalArgumentException("Too many applicationTags, a maximum of only "
                + YarnConfiguration.APPLICATION_MAX_TAGS + " are allowed!");
        }
        for (String tag : tags) {
            if (tag.length() > YarnConfiguration.APPLICATION_MAX_TAG_LENGTH) {
                throw new IllegalArgumentException("Tag " + tag + " is too long, " +
                    "maximum allowed length of a tag is " +
                    YarnConfiguration.APPLICATION_MAX_TAG_LENGTH);
            }
            if (!CharMatcher.ASCII.matchesAllOf(tag)) {
                throw new IllegalArgumentException("A tag can only have ASCII " +
                    "characters! Invalid tag - " + tag);
            }
        }
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setApplicationTags(Set<String> tags) {
        maybeInitBuilder();
        if (tags == null || tags.isEmpty()) {
            builder.clearApplicationTags();
            this.applicationTags = null;
            return;
        }
        //检查tag
        checkTags(tags);
        // Convert applicationTags to lower case and add
        this.applicationTags = new HashSet<String>();
        for (String tag : tags) {
            this.applicationTags.add(StringUtils.toLowerCase(tag));
        }
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public ContainerLaunchContext getAMContainerSpec() {
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        if (this.amContainer != null) {
            return amContainer;
        } // Else via proto
        if (!p.hasAmContainerSpec()) {
            return null;
        }
        //创建容器加载上下文对象
        amContainer = convertFromProtoFormat(p.getAmContainerSpec());
        return amContainer;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setAMContainerSpec(ContainerLaunchContext amContainer) {
        maybeInitBuilder();
        if (amContainer == null) {
            builder.clearAmContainerSpec();
        }
        this.amContainer = amContainer;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public boolean getUnmanagedAM() {
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        return p.getUnmanagedAm();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setUnmanagedAM(boolean value) {
        maybeInitBuilder();
        builder.setUnmanagedAm(value);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public boolean getCancelTokensWhenComplete() {
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        //There is a default so cancelTokens should never be null
        return p.getCancelTokensWhenComplete();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setCancelTokensWhenComplete(boolean cancel) {
        maybeInitBuilder();
        builder.setCancelTokensWhenComplete(cancel);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public int getMaxAppAttempts() {
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        //读取应用最大的提交次数
        return p.getMaxAppAttempts();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setMaxAppAttempts(int maxAppAttempts) {
        maybeInitBuilder();
        //设置最大应用提交次数
        builder.setMaxAppAttempts(maxAppAttempts);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public Resource getResource() {
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        if (this.resource != null) {
            return this.resource;
        }
        if (!p.hasResource()) {
            return null;
        }
        this.resource = convertFromProtoFormat(p.getResource());
        //返回资源对象
        return this.resource;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setResource(Resource resource) {
        maybeInitBuilder();
        if (resource == null) {
            builder.clearResource();
        }
        this.resource = resource;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public ReservationId getReservationID() {
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        if (reservationId != null) {
            return reservationId;
        }
        if (!p.hasReservationId()) {
            return null;
        }
        reservationId = convertFromProtoFormat(p.getReservationId());
        return reservationId;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setReservationID(ReservationId reservationID) {
        maybeInitBuilder();
        if (reservationID == null) {
            builder.clearReservationId();
            return;
        }
        this.reservationId = reservationID;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void
    setKeepContainersAcrossApplicationAttempts(boolean keepContainers) {
        maybeInitBuilder();
        builder.setKeepContainersAcrossApplicationAttempts(keepContainers);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public boolean getKeepContainersAcrossApplicationAttempts() {
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        return p.getKeepContainersAcrossApplicationAttempts();
    }

    // TODO: 17/3/25 by zmyer
    private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
        return new PriorityPBImpl(p);
    }

    // TODO: 17/3/25 by zmyer
    private PriorityProto convertToProtoFormat(Priority t) {
        return ((PriorityPBImpl) t).getProto();
    }

    // TODO: 17/3/25 by zmyer
    private ResourceRequestPBImpl convertFromProtoFormat(ResourceRequestProto p) {
        return new ResourceRequestPBImpl(p);
    }

    // TODO: 17/3/25 by zmyer
    private ResourceRequestProto convertToProtoFormat(ResourceRequest t) {
        return ((ResourceRequestPBImpl) t).getProto();
    }

    // TODO: 17/3/25 by zmyer
    private ApplicationIdPBImpl convertFromProtoFormat(ApplicationIdProto p) {
        return new ApplicationIdPBImpl(p);
    }

    // TODO: 17/3/25 by zmyer
    private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
        return ((ApplicationIdPBImpl) t).getProto();
    }

    // TODO: 17/3/25 by zmyer
    private ContainerLaunchContextPBImpl convertFromProtoFormat(
        ContainerLaunchContextProto p) {
        return new ContainerLaunchContextPBImpl(p);
    }

    // TODO: 17/3/25 by zmyer
    private ContainerLaunchContextProto convertToProtoFormat(
        ContainerLaunchContext t) {
        return ((ContainerLaunchContextPBImpl) t).getProto();
    }

    // TODO: 17/3/25 by zmyer
    private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
        return new ResourcePBImpl(p);
    }

    // TODO: 17/3/25 by zmyer
    private ResourceProto convertToProtoFormat(Resource t) {
        return ((ResourcePBImpl) t).getProto();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public String getNodeLabelExpression() {
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        if (!p.hasNodeLabelExpression()) {
            return null;
        }
        return p.getNodeLabelExpression();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setNodeLabelExpression(String labelExpression) {
        maybeInitBuilder();
        if (labelExpression == null) {
            builder.clearNodeLabelExpression();
            return;
        }
        builder.setNodeLabelExpression(labelExpression);
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public ResourceRequest getAMContainerResourceRequest() {
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        if (this.amResourceRequest != null) {
            return amResourceRequest;
        } // Else via proto
        if (!p.hasAmContainerResourceRequest()) {
            return null;
        }
        amResourceRequest = convertFromProtoFormat(p.getAmContainerResourceRequest());
        return amResourceRequest;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setAMContainerResourceRequest(ResourceRequest request) {
        maybeInitBuilder();
        if (request == null) {
            builder.clearAmContainerResourceRequest();
        }
        this.amResourceRequest = request;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public long getAttemptFailuresValidityInterval() {
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        return p.getAttemptFailuresValidityInterval();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setAttemptFailuresValidityInterval(
        long attemptFailuresValidityInterval) {
        maybeInitBuilder();
        builder.setAttemptFailuresValidityInterval(attemptFailuresValidityInterval);
    }

    // TODO: 17/3/25 by zmyer
    private LogAggregationContextPBImpl convertFromProtoFormat(
        LogAggregationContextProto p) {
        return new LogAggregationContextPBImpl(p);
    }

    // TODO: 17/3/25 by zmyer
    private LogAggregationContextProto convertToProtoFormat(
        LogAggregationContext t) {
        return ((LogAggregationContextPBImpl) t).getProto();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public LogAggregationContext getLogAggregationContext() {
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        if (this.logAggregationContext != null) {
            return this.logAggregationContext;
        } // Else via proto
        if (!p.hasLogAggregationContext()) {
            return null;
        }
        logAggregationContext = convertFromProtoFormat(p.getLogAggregationContext());
        return logAggregationContext;
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setLogAggregationContext(
        LogAggregationContext logAggregationContext) {
        maybeInitBuilder();
        if (logAggregationContext == null)
            builder.clearLogAggregationContext();
        this.logAggregationContext = logAggregationContext;
    }

    // TODO: 17/3/25 by zmyer
    private ReservationIdPBImpl convertFromProtoFormat(ReservationIdProto p) {
        return new ReservationIdPBImpl(p);
    }

    // TODO: 17/3/25 by zmyer
    private ReservationIdProto convertToProtoFormat(ReservationId t) {
        return ((ReservationIdPBImpl) t).getProto();
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public Map<ApplicationTimeoutType, Long> getApplicationTimeouts() {
        initApplicationTimeout();
        return this.applicationTimeouts;
    }

    // TODO: 17/3/25 by zmyer
    private void initApplicationTimeout() {
        if (this.applicationTimeouts != null) {
            return;
        }
        ApplicationSubmissionContextProtoOrBuilder p = viaProto ? proto : builder;
        List<ApplicationTimeoutMapProto> lists = p.getApplicationTimeoutsList();
        this.applicationTimeouts =
            new HashMap<ApplicationTimeoutType, Long>(lists.size());
        for (ApplicationTimeoutMapProto timeoutProto : lists) {
            this.applicationTimeouts.put(
                ProtoUtils
                    .convertFromProtoFormat(timeoutProto.getApplicationTimeoutType()),
                timeoutProto.getTimeout());
        }
    }

    // TODO: 17/3/25 by zmyer
    @Override
    public void setApplicationTimeouts(
        Map<ApplicationTimeoutType, Long> appTimeouts) {
        if (appTimeouts == null) {
            return;
        }
        initApplicationTimeout();
        this.applicationTimeouts.clear();
        this.applicationTimeouts.putAll(appTimeouts);
    }

    // TODO: 17/3/25 by zmyer
    private void addApplicationTimeouts() {
        maybeInitBuilder();
        builder.clearApplicationTimeouts();
        if (applicationTimeouts == null) {
            return;
        }
        Iterable<? extends ApplicationTimeoutMapProto> values =
            new Iterable<ApplicationTimeoutMapProto>() {

                @Override
                public Iterator<ApplicationTimeoutMapProto> iterator() {
                    return new Iterator<ApplicationTimeoutMapProto>() {
                        private Iterator<ApplicationTimeoutType> iterator =
                            applicationTimeouts.keySet().iterator();

                        @Override
                        public boolean hasNext() {
                            return iterator.hasNext();
                        }

                        @Override
                        public ApplicationTimeoutMapProto next() {
                            ApplicationTimeoutType key = iterator.next();
                            return ApplicationTimeoutMapProto.newBuilder()
                                .setTimeout(applicationTimeouts.get(key))
                                .setApplicationTimeoutType(
                                    ProtoUtils.convertToProtoFormat(key))
                                .build();
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        this.builder.addAllApplicationTimeouts(values);
    }
}  
