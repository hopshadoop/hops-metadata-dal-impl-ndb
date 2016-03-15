/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2015  hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package io.hops.metadata.ndb.dalimpl.yarn;

import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.FullRMNodeDataAccess;
import io.hops.metadata.yarn.entity.ContainerId;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.FinishedApplications;
import io.hops.metadata.yarn.entity.JustLaunchedContainers;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.metadata.yarn.entity.Node;
import io.hops.metadata.yarn.entity.NodeHBResponse;
import io.hops.metadata.yarn.entity.PendingEvent;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.RMNodeComps;
import io.hops.metadata.yarn.entity.Resource;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;

import java.util.ArrayList;
import java.util.List;

public class FullRMNodeClusterJ implements FullRMNodeDataAccess<RMNodeComps> {

  private final ClusterjConnector connector = ClusterjConnector.getInstance();
  private final JustLaunchedContainersClusterJ justLaunchedDA =
      new JustLaunchedContainersClusterJ();
  private final ResourceClusterJ resourceDA = new ResourceClusterJ();
  private final PendingEventClusterJ pendingEventDA = new PendingEventClusterJ();
  
  private final ContainerIdToCleanClusterJ containerToCleanDA =
      new ContainerIdToCleanClusterJ();
  private final FinishedApplicationsClusterJ finishedApplicationsDA =
      new FinishedApplicationsClusterJ();
  private final UpdatedContainerInfoClusterJ updatedContainerDA =
      new UpdatedContainerInfoClusterJ();

  @Override
  public RMNodeComps findByNodeId(String nodeId) throws StorageException {

    HopsSession session = connector.obtainSession();
    List<JustLaunchedContainers> hopJustLaunchedContainers = justLaunchedDA.
            findByRMNode(nodeId);
    List<UpdatedContainerInfo> hopUpdatedContainerInfo
            = updatedContainerDA.findByRMNodeList(nodeId);
    Resource hopResource = resourceDA.findEntry(nodeId,
            Resource.TOTAL_CAPABILITY,
            Resource.RMNODE);
    List<ContainerId> hopContainerIdsToClean = containerToCleanDA.
            findByRMNode(nodeId);

    List<FinishedApplications> hopFinishedApplications = finishedApplicationsDA.
            findByRMNode(nodeId);
    List<RMNodeComponentDTO> components = new ArrayList<RMNodeComponentDTO>();

    RMNodeClusterJ.RMNodeDTO rmnodeDTO =
        session.newInstance(RMNodeClusterJ.RMNodeDTO.class, nodeId);
    rmnodeDTO = session.load(rmnodeDTO);
    components.add(rmnodeDTO);

    NextHeartbeatClusterJ.NextHeartbeatDTO nextHBDTO = session
        .newInstance(NextHeartbeatClusterJ.NextHeartbeatDTO.class, nodeId);
    nextHBDTO = session.load(nextHBDTO);
    components.add(nextHBDTO);

    NodeClusterJ.NodeDTO nodeDTO =
        session.newInstance(NodeClusterJ.NodeDTO.class, nodeId);
    nodeDTO = session.load(nodeDTO);
    components.add(nodeDTO);
 
    NodeHBResponseClusterJ.NodeHBResponseDTO nodeHBResponseDTO = session.
            newInstance(NodeHBResponseClusterJ.NodeHBResponseDTO.class, nodeId);
    if (nodeHBResponseDTO != null) {
      nodeHBResponseDTO = session.load(nodeHBResponseDTO);
      components.add(nodeHBResponseDTO);
    }

    List<ContainerStatusClusterJ.ContainerStatusDTO> containerStatusDTOs =
        new ArrayList<ContainerStatusClusterJ.ContainerStatusDTO>();
    if (hopJustLaunchedContainers != null) {
      for (JustLaunchedContainers hop : hopJustLaunchedContainers) {
        Object[] pk = new Object[]{hop.getContainerId(), hop.getRmnodeid(),
        ContainerStatus.Type.JUST_LAUNCHED.name()};
        ContainerStatusClusterJ.ContainerStatusDTO containerStatusDTO = session.
                newInstance(ContainerStatusClusterJ.ContainerStatusDTO.class, pk);
        containerStatusDTO = session.load(containerStatusDTO);
        containerStatusDTOs.add(containerStatusDTO);
      }
    }


    if (hopUpdatedContainerInfo != null) {
      
        for (UpdatedContainerInfo hop : hopUpdatedContainerInfo) {
          Object[] pk = new Object[]{hop.getContainerId(), hop.getRmnodeid(),
          ContainerStatus.Type.UCI.name()};
          ContainerStatusClusterJ.ContainerStatusDTO containerStatusDTO =
              session.
                  newInstance(ContainerStatusClusterJ.ContainerStatusDTO.class,
                          pk);
          containerStatusDTO = session.load(containerStatusDTO);
          containerStatusDTOs.add(containerStatusDTO);
        }
      }
    

    session.flush();
    
    RMNode hopRMNode = null;
    Node hopNode = null;
    NodeHBResponse hopNodeHBResponse = null;
    NextHeartbeat hopNextHeartbeat = null;
      
    for (RMNodeComponentDTO comp : components) {
      if (comp instanceof RMNodeClusterJ.RMNodeDTO) {
        hopRMNode =
            RMNodeClusterJ.createHopRMNode((RMNodeClusterJ.RMNodeDTO) comp);
        //If commandport is zero, node was not found so return null
        //This is due to ClusterJ issue with returning a DTO object even if
        //the row was not found in the DB!
        if (hopRMNode.getHostName() == null) {
          session.release(components);
          session.release(containerStatusDTOs);
          session.release(nextHBDTO);
          session.release(nodeDTO);
          session.release(nodeHBResponseDTO);
          session.release(rmnodeDTO);
          return null;
        }
      } else if (comp instanceof NodeClusterJ.NodeDTO) {
        hopNode = NodeClusterJ.
                createHopNode((NodeClusterJ.NodeDTO) comp);
      } else if (comp instanceof NodeHBResponseClusterJ.NodeHBResponseDTO) {
        hopNodeHBResponse = NodeHBResponseClusterJ.
                createHopNodeHBResponse(
                        (NodeHBResponseClusterJ.NodeHBResponseDTO) comp);
      } else if (comp instanceof NextHeartbeatClusterJ.NextHeartbeatDTO) {
        hopNextHeartbeat = NextHeartbeatClusterJ.createHopNextHeartbeat(
                (NextHeartbeatClusterJ.NextHeartbeatDTO) comp);
      }
    
    }
    PendingEvent hopPendingEvent = pendingEventDA.findEntry(hopRMNode.getPendingEventId(), nodeId);
    String rmNodeId = null;
    if(hopPendingEvent!=null){
      rmNodeId = hopPendingEvent.getId().getNodeId();
    }else if(hopRMNode !=null){
      rmNodeId = hopRMNode.getNodeId();
    }
    RMNodeComps result = new RMNodeComps(hopRMNode, hopNextHeartbeat, hopNode,
        hopNodeHBResponse, hopResource, hopPendingEvent, hopJustLaunchedContainers,
        hopUpdatedContainerInfo, hopContainerIdsToClean,
        hopFinishedApplications,
        ContainerStatusClusterJ.createList(containerStatusDTOs), rmNodeId);
    session.release(components);
    session.release(containerStatusDTOs);
    session.release(nextHBDTO);
    session.release(nodeDTO);
    session.release(nodeHBResponseDTO);
    session.release(rmnodeDTO);
    return result;
  }

}
