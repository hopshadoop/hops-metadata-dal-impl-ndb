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

package io.hops.metadata.ndb.dalimpl.yarn.rmstatestore;

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
  public void addAll(Collection<UpdatedNode> toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<UpdatedNodeDTO> toPersist = new ArrayList<UpdatedNodeDTO>();
    for (UpdatedNode n : toAdd) {
      toPersist.add(createPersistable(n, session));
    }
    session.savePersistentAll(toPersist);
    session.release(toPersist);
  }
  
  @Override
  public void removeAll(Collection<UpdatedNode> toRemove)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<UpdatedNodeDTO> toPersist = new ArrayList<UpdatedNodeDTO>();
    for (UpdatedNode n : toRemove) {
      toPersist.add(createPersistable(n, session));
    }
    session.deletePersistentAll(toPersist);
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
