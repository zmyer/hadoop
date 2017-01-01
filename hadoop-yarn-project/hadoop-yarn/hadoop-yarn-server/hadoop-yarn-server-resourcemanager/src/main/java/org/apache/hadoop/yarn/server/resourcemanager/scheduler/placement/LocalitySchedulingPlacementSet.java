/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LocalitySchedulingPlacementSet<N extends SchedulerNode>
    implements SchedulingPlacementSet<N> {
  private final Map<String, ResourceRequest> resourceRequestMap =
      new ConcurrentHashMap<>();
  private AppSchedulingInfo appSchedulingInfo;

  private final ReentrantReadWriteLock.ReadLock readLock;
  private final ReentrantReadWriteLock.WriteLock writeLock;

  public LocalitySchedulingPlacementSet(AppSchedulingInfo info) {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
    this.appSchedulingInfo = info;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<N> getPreferredNodeIterator(
      PlacementSet<N> clusterPlacementSet) {
    // Now only handle the case that single node in placementSet
    // TODO, Add support to multi-hosts inside placement-set which is passed in.

    N singleNode = PlacementSetUtils.getSingleNode(clusterPlacementSet);
    if (null != singleNode) {
      return IteratorUtils.singletonIterator(singleNode);
    }

    return IteratorUtils.emptyIterator();
  }

  private boolean hasRequestLabelChanged(ResourceRequest requestOne,
      ResourceRequest requestTwo) {
    String requestOneLabelExp = requestOne.getNodeLabelExpression();
    String requestTwoLabelExp = requestTwo.getNodeLabelExpression();
    // First request label expression can be null and second request
    // is not null then we have to consider it as changed.
    if ((null == requestOneLabelExp) && (null != requestTwoLabelExp)) {
      return true;
    }
    // If the label is not matching between both request when
    // requestOneLabelExp is not null.
    return ((null != requestOneLabelExp) && !(requestOneLabelExp
        .equals(requestTwoLabelExp)));
  }

  private void updateNodeLabels(ResourceRequest request) {
    String resourceName = request.getResourceName();
    if (resourceName.equals(ResourceRequest.ANY)) {
      ResourceRequest previousAnyRequest =
          getResourceRequest(resourceName);

      // When there is change in ANY request label expression, we should
      // update label for all resource requests already added of same
      // priority as ANY resource request.
      if ((null == previousAnyRequest) || hasRequestLabelChanged(
          previousAnyRequest, request)) {
        for (ResourceRequest r : resourceRequestMap.values()) {
          if (!r.getResourceName().equals(ResourceRequest.ANY)) {
            r.setNodeLabelExpression(request.getNodeLabelExpression());
          }
        }
      }
    } else{
      ResourceRequest anyRequest = getResourceRequest(ResourceRequest.ANY);
      if (anyRequest != null) {
        request.setNodeLabelExpression(anyRequest.getNodeLabelExpression());
      }
    }
  }

  @Override
  public ResourceRequestUpdateResult updateResourceRequests(
      Collection<ResourceRequest> requests,
      boolean recoverPreemptedRequestForAContainer) {
    try {
      this.writeLock.lock();

      ResourceRequestUpdateResult updateResult = null;

      // Update resource requests
      for (ResourceRequest request : requests) {
        String resourceName = request.getResourceName();

        // Update node labels if required
        updateNodeLabels(request);

        // Increment number of containers if recovering preempted resources
        ResourceRequest lastRequest = resourceRequestMap.get(resourceName);
        if (recoverPreemptedRequestForAContainer && lastRequest != null) {
          request.setNumContainers(lastRequest.getNumContainers() + 1);
        }

        // Update asks
        resourceRequestMap.put(resourceName, request);

        if (resourceName.equals(ResourceRequest.ANY)) {
          //update the applications requested labels set
          appSchedulingInfo.addRequestedPartition(
              request.getNodeLabelExpression() == null ?
                  RMNodeLabelsManager.NO_LABEL :
                  request.getNodeLabelExpression());

          updateResult = new ResourceRequestUpdateResult(lastRequest, request);
        }
      }
      return updateResult;
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public Map<String, ResourceRequest> getResourceRequests() {
    return resourceRequestMap;
  }

  @Override
  public ResourceRequest getResourceRequest(String resourceName) {
    return resourceRequestMap.get(resourceName);
  }

  private void decrementOutstanding(ResourceRequest offSwitchRequest) {
    int numOffSwitchContainers = offSwitchRequest.getNumContainers() - 1;

    // Do not remove ANY
    offSwitchRequest.setNumContainers(numOffSwitchContainers);

    // Do we have any outstanding requests?
    // If there is nothing, we need to deactivate this application
    if (numOffSwitchContainers == 0) {
      SchedulerRequestKey schedulerRequestKey = SchedulerRequestKey.create(
          offSwitchRequest);
      appSchedulingInfo.decrementSchedulerKeyReference(schedulerRequestKey);
      appSchedulingInfo.checkForDeactivation();
    }

    appSchedulingInfo.decPendingResource(
        offSwitchRequest.getNodeLabelExpression(),
        offSwitchRequest.getCapability());
  }

  private ResourceRequest cloneResourceRequest(ResourceRequest request) {
    ResourceRequest newRequest =
        ResourceRequest.newInstance(request.getPriority(),
            request.getResourceName(), request.getCapability(), 1,
            request.getRelaxLocality(), request.getNodeLabelExpression());
    return newRequest;
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   */
  private void allocateRackLocal(SchedulerNode node,
      ResourceRequest rackLocalRequest,
      List<ResourceRequest> resourceRequests) {
    // Update future requirements
    decResourceRequest(node.getRackName(), rackLocalRequest);

    ResourceRequest offRackRequest = resourceRequestMap.get(
        ResourceRequest.ANY);
    decrementOutstanding(offRackRequest);

    // Update cloned RackLocal and OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(rackLocalRequest));
    resourceRequests.add(cloneResourceRequest(offRackRequest));
  }

  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   */
  private void allocateOffSwitch(ResourceRequest offSwitchRequest,
      List<ResourceRequest> resourceRequests) {
    // Update future requirements
    decrementOutstanding(offSwitchRequest);
    // Update cloned OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(offSwitchRequest));
  }


  /**
   * The {@link ResourceScheduler} is allocating data-local resources to the
   * application.
   */
  private void allocateNodeLocal(SchedulerNode node,
      ResourceRequest nodeLocalRequest,
      List<ResourceRequest> resourceRequests) {
    // Update future requirements
    decResourceRequest(node.getNodeName(), nodeLocalRequest);

    ResourceRequest rackLocalRequest = resourceRequestMap.get(
        node.getRackName());
    decResourceRequest(node.getRackName(), rackLocalRequest);

    ResourceRequest offRackRequest = resourceRequestMap.get(
        ResourceRequest.ANY);
    decrementOutstanding(offRackRequest);

    // Update cloned NodeLocal, RackLocal and OffRack requests for recovery
    resourceRequests.add(cloneResourceRequest(nodeLocalRequest));
    resourceRequests.add(cloneResourceRequest(rackLocalRequest));
    resourceRequests.add(cloneResourceRequest(offRackRequest));
  }

  private void decResourceRequest(String resourceName,
      ResourceRequest request) {
    request.setNumContainers(request.getNumContainers() - 1);
    if (request.getNumContainers() == 0) {
      resourceRequestMap.remove(resourceName);
    }
  }

  @Override
  public boolean canAllocate(NodeType type, SchedulerNode node) {
    try {
      readLock.lock();
      ResourceRequest r = resourceRequestMap.get(
          ResourceRequest.ANY);
      if (r == null || r.getNumContainers() <= 0) {
        return false;
      }
      if (type == NodeType.RACK_LOCAL || type == NodeType.NODE_LOCAL) {
        r = resourceRequestMap.get(node.getRackName());
        if (r == null || r.getNumContainers() <= 0) {
          return false;
        }
        if (type == NodeType.NODE_LOCAL) {
          r = resourceRequestMap.get(node.getNodeName());
          if (r == null || r.getNumContainers() <= 0) {
            return false;
          }
        }
      }

      return true;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<ResourceRequest> allocate(NodeType type, SchedulerNode node,
      ResourceRequest request) {
    try {
      writeLock.lock();

      List<ResourceRequest> resourceRequests = new ArrayList<>();

      if (null == request) {
        if (type == NodeType.NODE_LOCAL) {
          request = resourceRequestMap.get(node.getNodeName());
        } else if (type == NodeType.RACK_LOCAL) {
          request = resourceRequestMap.get(node.getRackName());
        } else{
          request = resourceRequestMap.get(ResourceRequest.ANY);
        }
      }

      if (type == NodeType.NODE_LOCAL) {
        allocateNodeLocal(node, request, resourceRequests);
      } else if (type == NodeType.RACK_LOCAL) {
        allocateRackLocal(node, request, resourceRequests);
      } else{
        allocateOffSwitch(request, resourceRequests);
      }

      return resourceRequests;
    } finally {
      writeLock.unlock();
    }
  }
}
