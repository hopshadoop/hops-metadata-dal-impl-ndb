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
import io.hops.metadata.yarn.dal.rmstatestore.UpdatedNodeDataAccess;
import io.hops.metadata.yarn.entity.rmstatestore.UpdatedNode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class UpdatedNodeClusterJ implements
    TablesDef.UpdatedNodeTableDef, UpdatedNodeDataAccess<UpdatedNode> {
  private static final Log LOG = LogFactory.getLog(UpdatedNodeClusterJ.class);
  @PersistenceCapable(table = TABLE_NAME)
  public interface UpdatedNodeDTO {

    @PrimaryKey
    @Column(name = APPLICATIONID)
    String getapplicationid();

    void setapplicationid(String applicationid);

    @PrimaryKey
    @Column(name = NODEID)
    String getnodeid();

    void setnodeid(String appstate);

  }
  
  private final ClusterjConnector connector = ClusterjConnector.getInstance();
  @Override
  public void addAll(Collection<List<UpdatedNode>> toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<UpdatedNodeDTO> toPersist = new ArrayList<UpdatedNodeDTO>();
    for(List<UpdatedNode> l : toAdd){
      for (UpdatedNode n : l) {
        toPersist.add(createPersistable(n, session));
      }
    }
    session.savePersistentAll(toPersist);
    session.release(toPersist);
  }
  
  @Override
  public Map<String,List<UpdatedNode>> getAll() throws StorageException{
    HopsSession session = connector.obtainSession();
     HopsQueryBuilder qb = session.getQueryBuilder();
     HopsQueryDomainType<UpdatedNodeDTO> dobj = qb.
        createQueryDefinition(UpdatedNodeDTO.class);
     HopsQuery<UpdatedNodeDTO> query = session.createQuery(dobj);
    List<UpdatedNodeDTO> queryResults = query.getResultList();

    Map<String, List<UpdatedNode>> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  public void removeAllByRMNodeId(List<String> rmNodeIds) throws StorageException {
    List<UpdatedNodeDTO> toBeRemoved = new ArrayList<UpdatedNodeDTO>();
    List<UpdatedNodeDTO> updateNodesRemove = null;
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder queryBuilder = session.getQueryBuilder();

    HopsQueryDomainType<UpdatedNodeDTO> dto =
            queryBuilder.createQueryDefinition(UpdatedNodeDTO.class);
    HopsPredicate pred = dto.get(NODEID).equal(dto.param(NODEID));
    dto.where(pred);
    HopsQuery<UpdatedNodeDTO> query = session.createQuery(dto);

    for (String rmNodeId : rmNodeIds) {
      query.setParameter(NODEID, rmNodeId);
      updateNodesRemove = query.getResultList();

      if (!updateNodesRemove.isEmpty()) {
        toBeRemoved.addAll(updateNodesRemove);
      }
    }

    if (!toBeRemoved.isEmpty()) {
      session.deletePersistentAll(toBeRemoved);
      session.release(toBeRemoved);
    }
  }

  private UpdatedNodeDTO createPersistable(UpdatedNode hop,
      HopsSession session) throws StorageException {
    UpdatedNodeDTO updatedNodeDTO =
        session.newInstance(UpdatedNodeDTO.class);
    updatedNodeDTO.setapplicationid(hop.getApplicationId());
    updatedNodeDTO.setnodeid(hop.getNodeId());

    return updatedNodeDTO;
  }
  
   private Map<String, List<UpdatedNode>> createMap(
      List<UpdatedNodeDTO> results) throws StorageException {
    Map<String, List<UpdatedNode>> map =
        new HashMap<String, List<UpdatedNode>>();
    for (UpdatedNodeDTO persistable : results) {
      UpdatedNode hop =
          createHopUpdatedNode(persistable);
      if (map.get(hop.getApplicationId()) == null) {
        map.put(hop.getApplicationId(),
            new ArrayList<UpdatedNode>());
      }
      map.get(hop.getApplicationId()).add(hop);
    }
    return map;
  }
   
     private UpdatedNode createHopUpdatedNode(
      UpdatedNodeDTO entry) throws StorageException {
      return new UpdatedNode(entry.getapplicationid(),entry.getnodeid());
  }
}
