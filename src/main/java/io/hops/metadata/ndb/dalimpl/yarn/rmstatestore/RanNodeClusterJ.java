/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.*;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.rmstatestore.RanNodeDataAccess;
import io.hops.metadata.yarn.entity.rmstatestore.RanNode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RanNodeClusterJ implements
        TablesDef.RanNodeTableDef,
        RanNodeDataAccess<RanNode> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface RanNodeDTO {

    @PrimaryKey
    @Column(name = APPLICATIONATTEMPTID)
    String getapplicationattemptid();

    void setapplicationattemptid(String applicationid);

    @PrimaryKey
    @Column(name = NODEID)
    String getnodeid();

    void setnodeid(String appstate);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void addAll(Collection<Map<Integer, RanNode>> toAdd)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<RanNodeDTO> toPersist = new ArrayList<RanNodeDTO>();
    for (Map<Integer, RanNode> l : toAdd) {
      for (RanNode n : l.values()) {
        toPersist.add(createPersistable(n, session));
      }
    }
    session.savePersistentAll(toPersist);
    session.release(toPersist);
  }

  @Override
  public Map<String,List<RanNode>> getAll() throws StorageException{
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<RanNodeDTO> dobj = qb.
        createQueryDefinition(RanNodeDTO.class);
    HopsQuery<RanNodeDTO> query = session.createQuery(dobj);
    List<RanNodeDTO> queryResults = query.getResultList();

    Map<String, List<RanNode>> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  public void removeAllByRMNodeId(List<String> rmNodesId) throws StorageException {
    List<RanNodeDTO> toBeRemoved = new ArrayList<RanNodeDTO>();
    List<RanNodeDTO> ranNodesRemove = null;
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder queryBuilder = session.getQueryBuilder();

    HopsQueryDomainType<RanNodeDTO> dto =
            queryBuilder.createQueryDefinition(RanNodeDTO.class);

    HopsPredicate pred = dto.get(NODEID).equal(dto.param(NODEID));
    dto.where(pred);
    HopsQuery<RanNodeDTO> query = session.createQuery(dto);

    for (String rmNodeId : rmNodesId) {
      query.setParameter(NODEID, rmNodeId);
      ranNodesRemove = query.getResultList();

      if (!ranNodesRemove.isEmpty()) {
        toBeRemoved.addAll(ranNodesRemove);
      }
    }

    if (!toBeRemoved.isEmpty()) {
      session.deletePersistentAll(toBeRemoved);
      session.release(toBeRemoved);
    }
  }
  
  private RanNodeDTO createPersistable(RanNode hop,
          HopsSession session) throws StorageException {
    RanNodeDTO updatedNodeDTO = session.newInstance(RanNodeDTO.class);
    updatedNodeDTO.setapplicationattemptid(hop.getApplicationAttemptId());
    updatedNodeDTO.setnodeid(hop.getNodeId());

    return updatedNodeDTO;
  }
  
   private Map<String, List<RanNode>> createMap(
      List<RanNodeDTO> results) throws StorageException {
    Map<String, List<RanNode>> map =
        new HashMap<String, List<RanNode>>();
    for (RanNodeDTO persistable : results) {
      RanNode hop =
          createHopRanNode(persistable);
      if (map.get(hop.getApplicationAttemptId()) == null) {
        map.put(hop.getApplicationAttemptId(),
            new ArrayList<RanNode>());
      }
      map.get(hop.getApplicationAttemptId()).add(hop);
    }
    return map;
  }
   
     private RanNode createHopRanNode(
      RanNodeDTO entry) throws StorageException {
      return new RanNode(entry.getapplicationattemptid(),entry.getnodeid());
  }
}
