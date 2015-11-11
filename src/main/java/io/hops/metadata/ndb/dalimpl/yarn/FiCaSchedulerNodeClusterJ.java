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
import io.hops.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import io.hops.metadata.yarn.entity.FiCaSchedulerNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FiCaSchedulerNodeClusterJ implements
    TablesDef.FiCaSchedulerNodeTableDef,
    FiCaSchedulerNodeDataAccess<FiCaSchedulerNode> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface FiCaSchedulerNodeDTO {

    @PrimaryKey
    @Column(name = RMNODEID)
    String getrmnodeid();

    void setrmnodeid(String rmnodeid);

    @Column(name = NODENAME)
    String getnodename();

    void setnodename(String nodename);

    @Column(name = NUMCONTAINERS)
    int getnumcontainers();

    void setnumcontainers(int numcontainers);
      
    @Column(name = RMCONTAINERID)
    String getrmcontainerid();

    void setrmcontainerid(String rmcontainerid);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void add(FiCaSchedulerNode toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    FiCaSchedulerNodeDTO persistable = createPersistable(toAdd, session);
    session.savePersistent(persistable);
    session.release(persistable);
    session.flush();
  }

  @Override
  public void addAll(Collection<FiCaSchedulerNode> toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FiCaSchedulerNodeDTO> toPersist =
        new ArrayList<FiCaSchedulerNodeDTO>();
    for (FiCaSchedulerNode hop : toAdd) {
      FiCaSchedulerNodeDTO persistable = createPersistable(hop, session);
      toPersist.add(persistable);
    }
      session.savePersistentAll(toPersist);
      session.release(toPersist);
      session.flush();
  }

  @Override
  public void removeAll(Collection<FiCaSchedulerNode> toRemove)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FiCaSchedulerNodeDTO> toPersist =
        new ArrayList<FiCaSchedulerNodeDTO>();
    for (FiCaSchedulerNode hop : toRemove) {
      FiCaSchedulerNodeDTO persistable =
          session.newInstance(FiCaSchedulerNodeDTO.class, hop.getRmnodeId());
      toPersist.add(persistable);
    }
    session.deletePersistentAll(toPersist);
    session.release(toPersist);
  }

  @Override
  public Map<String, FiCaSchedulerNode> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<FiCaSchedulerNodeDTO> dobj = qb.createQueryDefinition(
            FiCaSchedulerNodeClusterJ.FiCaSchedulerNodeDTO.class);
    HopsQuery<FiCaSchedulerNodeClusterJ.FiCaSchedulerNodeDTO> query = session.
            createQuery(dobj);

    List<FiCaSchedulerNodeClusterJ.FiCaSchedulerNodeDTO> queryResults = query.
            getResultList();
    Map<String, FiCaSchedulerNode> result = createFiCaSchedulerNodeMap(
            queryResults);
    session.release(queryResults);
    return result;
  }


  private FiCaSchedulerNodeDTO createPersistable(FiCaSchedulerNode hop,
      HopsSession session) throws StorageException {
    FiCaSchedulerNodeDTO ficaDTO =
        session.newInstance(FiCaSchedulerNodeDTO.class);
    ficaDTO.setrmnodeid(hop.getRmnodeId());
    ficaDTO.setnodename(hop.getNodeName());
    ficaDTO.setnumcontainers(hop.getNumOfContainers());
    ficaDTO.setrmcontainerid(hop.getReservedContainerId());
    return ficaDTO;
  }

  private Map<String, FiCaSchedulerNode> createFiCaSchedulerNodeMap(
      List<FiCaSchedulerNodeClusterJ.FiCaSchedulerNodeDTO> results) {
    Map<String, FiCaSchedulerNode> fifoSchedulerNodes =
        new HashMap<String, FiCaSchedulerNode>();
    for (FiCaSchedulerNodeClusterJ.FiCaSchedulerNodeDTO persistable : results) {
      FiCaSchedulerNode node = createHopFiCaSchedulerNode(persistable);
      fifoSchedulerNodes.put(node.getRmnodeId(), node);
    }
    return fifoSchedulerNodes;
  }

  private FiCaSchedulerNode createHopFiCaSchedulerNode(
      FiCaSchedulerNodeDTO entry) {
    FiCaSchedulerNode hop =new FiCaSchedulerNode(entry.getrmnodeid(),
            entry.getnodename(), entry.getnumcontainers(), entry.
            getrmcontainerid());

    return hop;
  }
}
