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
import io.hops.metadata.ndb.dalimpl.ClusterjDataAccess;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.dal.FullRMNodeDataAccess;
import io.hops.metadata.yarn.entity.*;

import java.util.ArrayList;
import java.util.List;

public class FullRMNodeClusterJ extends ClusterjDataAccess
    implements FullRMNodeDataAccess<RMNodeComps> {

  public FullRMNodeClusterJ(ClusterjConnector connector) {
    super(connector);
    containerToCleanDA = new ContainerIdToCleanClusterJ(connector);
    finishedApplicationsDA = new FinishedApplicationsClusterJ(connector);
    updatedContainerDA = new UpdatedContainerInfoClusterJ(connector);
    resourceDA = new ResourceClusterJ(connector);
    pendingEventDA = new PendingEventClusterJ(connector);
  }


  private final ResourceClusterJ resourceDA;
  private final PendingEventClusterJ pendingEventDA;

  private final ContainerIdToCleanClusterJ containerToCleanDA;
  private final FinishedApplicationsClusterJ finishedApplicationsDA;
  private final UpdatedContainerInfoClusterJ updatedContainerDA;

  @Override
  public RMNodeComps findByNodeId(String nodeId) throws StorageException {
    HopsSession session = getConnector().obtainSession();
    List<UpdatedContainerInfo> hopUpdatedContainerInfo
        = updatedContainerDA.findByRMNodeList(nodeId);
    Resource hopResource = resourceDA.findEntry(nodeId);
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

    List<ContainerStatusClusterJ.ContainerStatusDTO> containerStatusDTOs =
        new ArrayList<>();

    if (hopUpdatedContainerInfo != null) {

      for (UpdatedContainerInfo hop : hopUpdatedContainerInfo) {
        Object[] pk = new Object[]{hop.getContainerId(), hop.getRmnodeid(),
            hop.getUpdatedContainerInfoId()};
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
          session.release(rmnodeDTO);
          return null;
        }
      } else if (comp instanceof NextHeartbeatClusterJ.NextHeartbeatDTO) {
        hopNextHeartbeat = NextHeartbeatClusterJ.createHopNextHeartbeat(
            (NextHeartbeatClusterJ.NextHeartbeatDTO) comp);
      }

    }
    PendingEvent hopPendingEvent = pendingEventDA.findEntry(hopRMNode.getPendingEventId(), nodeId);
    String rmNodeId = null;
    if (hopPendingEvent != null) {
      rmNodeId = hopPendingEvent.getId().getNodeId();
    } else if (hopRMNode != null) {
      rmNodeId = hopRMNode.getNodeId();
    }
    RMNodeComps result = new RMNodeComps(hopRMNode,
        hopResource, hopPendingEvent,
        hopUpdatedContainerInfo,
        ContainerStatusClusterJ.createList(containerStatusDTOs));
    session.release(components);
    session.release(containerStatusDTOs);
    session.release(nextHBDTO);
    session.release(rmnodeDTO);
    return result;
  }

}
