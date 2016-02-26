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

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.entity.FinishedApplications;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implements connection of RMNodeImpl to NDB.
 */
public class RMNodeClusterJ
    implements TablesDef.RMNodeTableDef, RMNodeDataAccess<RMNode> {

  private static final Log LOG = LogFactory.getLog(RMNodeClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface RMNodeDTO extends RMNodeComponentDTO {

    @PrimaryKey
    @Column(name = NODEID)
    String getNodeid();

    void setNodeid(String nodeid);

    @Column(name = HOST_NAME)
    String getHostname();

    void setHostname(String hostName);

    @Column(name = COMMAND_PORT)
    int getCommandport();

    void setCommandport(int commandport);

    @Column(name = HTTP_PORT)
    int getHttpport();

    void setHttpport(int httpport);

    @Column(name = NODE_ADDRESS)
    String getNodeaddress();

    void setNodeaddress(String nodeAddress);

    @Column(name = HTTP_ADDRESS)
    String getHttpaddress();

    void setHttpaddress(String httpAddress);

    @Column(name = HEALTH_REPORT)
    String getHealthreport();

    void setHealthreport(String healthreport);

    @Column(name = LAST_HEALTH_REPORT_TIME)
    long getLasthealthreporttime();

    void setLasthealthreporttime(long lasthealthreporttime);

    @Column(name = CURRENT_STATE)
    String getcurrentstate();

    void setcurrentstate(String currentstate);

    @Column(name = OVERCOMMIT_TIMEOUT)
    int getovercommittimeout();

    void setovercommittimeout(int overcommittimeout);

    @Column(name = NODEMANAGER_VERSION)
    String getnodemanagerversion();

    void setnodemanagerversion(String nodemanagerversion);

    @Column(name = UCI_ID)
    int getuciId();

    void setuciId(int uciId);
    
    @Column(name = PENDING_EVENT_ID)
    int getpendingeventid();

    void setpendingeventid(int pendingeventid);
    
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public RMNode findByNodeId(String nodeid) throws StorageException {
    LOG.debug("HOP :: ClusterJ RMNode.findByNodeId - START:" + nodeid);
    HopsSession session = connector.obtainSession();
    RMNodeDTO rmnodeDTO = session.find(RMNodeDTO.class, nodeid);
    RMNode result = null;
    if (rmnodeDTO != null) {
      LOG.debug("HOP :: ClusterJ RMNode.findByNodeId - FINISH:" + nodeid);
      result = createHopRMNode(rmnodeDTO);
    }
    LOG.debug("HOP :: ClusterJ RMNode.findByNodeId - FINISH:" + nodeid);
    session.release(rmnodeDTO);
    return result;
  }

  @Override
  public Map<String, RMNode> getAll() throws StorageException {
    LOG.debug("HOP :: ClusterJ RMNode.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<RMNodeDTO> dobj =
        qb.createQueryDefinition(RMNodeDTO.class);
    HopsQuery<RMNodeDTO> query = session.createQuery(dobj);
    List<RMNodeDTO> queryResults = query.getResultList();
    LOG.debug("HOP :: ClusterJ RMNode.getAll - FINISH");
    Map<String, RMNode> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public void addAll(List<RMNode> toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<RMNodeDTO> toPersist = new ArrayList<RMNodeDTO>();
    Collections.sort(toAdd);
    for (RMNode req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }
    session.savePersistentAll(toPersist);
    session.flush();
    session.release(toPersist);
  }

  @Override
  public void removeAll(Collection<RMNode> toRemove) throws StorageException {
    NextHeartbeatClusterJ nextHBClusterJ = new NextHeartbeatClusterJ();
    FinishedApplicationsClusterJ finishedAppsClusterJ = new FinishedApplicationsClusterJ();
    NodeClusterJ nodeClusterJ = new NodeClusterJ();
    UpdatedContainerInfoClusterJ updatedContClusterJ = new UpdatedContainerInfoClusterJ();
    HopsSession session = connector.obtainSession();
    List<RMNodeDTO> toPersist = new ArrayList<RMNodeDTO>();
    List<String> nodesId = new ArrayList<String>();
    List<FinishedApplications> finishedToRemove = new ArrayList<FinishedApplications>();
    List<UpdatedContainerInfo> updatedContainersToRemove = new ArrayList<UpdatedContainerInfo>();
    List<FinishedApplications> tmpFinishedApps = null;
    List<UpdatedContainerInfo> tmpUpdatedCont = null;

    for (RMNode entry : toRemove) {
      String rmNodeId = entry.getNodeId();
      toPersist.add(session.newInstance(RMNodeDTO.class, rmNodeId));
      nodesId.add(rmNodeId);

      tmpFinishedApps = finishedAppsClusterJ.findByRMNode(rmNodeId);
      if (tmpFinishedApps != null) {
        finishedToRemove.addAll(tmpFinishedApps);
      }

      tmpUpdatedCont = updatedContClusterJ.findByRMNodeList(rmNodeId);
      if (tmpUpdatedCont != null) {
        updatedContainersToRemove.addAll(tmpUpdatedCont);
      }
    }

    // Remove all depended entries from ndb
    nextHBClusterJ.removeAllById(nodesId);
    nodeClusterJ.removeAllById(nodesId);
    if (!finishedToRemove.isEmpty()) {
      finishedAppsClusterJ.removeAll(finishedToRemove);
    }
    if (!updatedContainersToRemove.isEmpty()) {
      updatedContClusterJ.removeAll(updatedContainersToRemove);
    }

    session.deletePersistentAll(toPersist);
    session.release(toPersist);
  }

  @Override
  public void add(RMNode rmNode) throws StorageException {
    HopsSession session = connector.obtainSession();
    RMNodeDTO dto = createPersistable(rmNode, session);
    session.savePersistent(dto);
    session.flush();
    session.release(dto);
  }

  private Map<String, RMNode> createMap(List<RMNodeDTO> results) {
    Map<String, RMNode> map = new HashMap<String, RMNode>();
    for (RMNodeDTO persistable : results) {
      RMNode hop = createHopRMNode(persistable);
      map.put(hop.getNodeId(), hop);
    }
    return map;
  }


  private RMNodeDTO createPersistable(RMNode hopRMNode, HopsSession session)
      throws StorageException {
    RMNodeDTO rmDTO = session.newInstance(RMNodeDTO.class);
    //Set values to persist new rmnode
    rmDTO.setNodeid(hopRMNode.getNodeId());
    rmDTO.setHostname(hopRMNode.getHostName());
    rmDTO.setCommandport(hopRMNode.getCommandPort());
    rmDTO.setHttpport(hopRMNode.getHttpPort());
    rmDTO.setNodeaddress(hopRMNode.getNodeAddress());
    rmDTO.setHttpaddress(hopRMNode.getHttpAddress());
    rmDTO.setHealthreport(hopRMNode.getHealthReport());
    rmDTO.setLasthealthreporttime(hopRMNode.getLastHealthReportTime());
    rmDTO.setcurrentstate(hopRMNode.getCurrentState());
    rmDTO.setovercommittimeout(hopRMNode.getOvercommittimeout());
    rmDTO.setnodemanagerversion(hopRMNode.getNodemanagerVersion());
    rmDTO.setuciId(hopRMNode.getUciId());
    rmDTO.setpendingeventid(hopRMNode.getPendingEventId());
    return rmDTO;
  }

  /**
   * Transforms a DTO to Hop object.
   *
   * @param rmDTO
   * @return HopRMNode
   */
  public static RMNode createHopRMNode(RMNodeDTO rmDTO) {
    return new RMNode(rmDTO.getNodeid(), rmDTO.getHostname(),
        rmDTO.getCommandport(), rmDTO.getHttpport(), rmDTO.getNodeaddress(),
        rmDTO.getHttpaddress(), rmDTO.getHealthreport(),
        rmDTO.getLasthealthreporttime(), rmDTO.getcurrentstate(),
        rmDTO.getnodemanagerversion(), rmDTO.getovercommittimeout(),
        rmDTO.getuciId(),rmDTO.getpendingeventid());
  }
}
