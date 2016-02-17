/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package io.hops.metadata.ndb.dalimpl.yarn.capacity;

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
import io.hops.metadata.yarn.dal.capacity.CSLeafQueuesPendingAppsDataAccess;
import io.hops.metadata.yarn.entity.capacity.LeafQueuePendingApp;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CSLeafQueuesPendingAppsClusterJ implements TablesDef.CSLeafQueuesPendingAppsTableDef,
        CSLeafQueuesPendingAppsDataAccess<LeafQueuePendingApp>{

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface LeafQueuePendingAppDTO {

    @PrimaryKey
    @Column(name = APPATTEMPTID)
    String getAppAttemptId();

    void setAppAttemptId(String appAttemptId);

    
    @Column(name = PATH)
    String getpath();

    void setpath(String path);
  }
  
  @Override
  public Map<String, Set<String>> getAll() throws StorageException, IOException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<LeafQueuePendingAppDTO> dobj = qb.
            createQueryDefinition(LeafQueuePendingAppDTO.class);
    HopsQuery<LeafQueuePendingAppDTO> query = session.createQuery(dobj);
    List<LeafQueuePendingAppDTO> queryResults = query.getResultList();

    Map<String, Set<String>> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }
  
  @Override
  public void addAll(Collection<LeafQueuePendingApp> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    if (modified != null) {
      List<LeafQueuePendingAppDTO> toModify = new ArrayList<LeafQueuePendingAppDTO>(modified.size());
      for (LeafQueuePendingApp hop : modified) {
        toModify.add(createPersistable(hop, session));
      }
      session.savePersistentAll(toModify);
      session.release(toModify);
    }
  }

  @Override
  public void removeAll(Collection<LeafQueuePendingApp> removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    if (removed != null) {
      List<LeafQueuePendingAppDTO> toRemove = new ArrayList<LeafQueuePendingAppDTO>(removed.size());
      for (LeafQueuePendingApp hop : removed) {
        toRemove.add(createPersistable(hop, session));
      }
      session.deletePersistentAll(toRemove);
      session.release(toRemove);
    }
  }
  
  private LeafQueuePendingAppDTO createPersistable(LeafQueuePendingApp hop, HopsSession session) throws
          StorageException {
    LeafQueuePendingAppDTO dto = session.newInstance(
            LeafQueuePendingAppDTO.class);

    dto.setAppAttemptId(hop.getAppAttemptId());
    dto.setpath(hop.getQueuePath());

    return dto;
  }
  
  private Map<String,Set<String>> createMap(List<LeafQueuePendingAppDTO> list)
          throws IOException {
    Map<String, Set<String>> map = new HashMap<String,Set<String>>();
    for (LeafQueuePendingAppDTO persistable : list) {
      Set<String> set = map.get(persistable.getpath());
      if(set==null){
        set = new HashSet<String>();
        map.put(persistable.getpath(), set);
      }
      set.add(persistable.getAppAttemptId());
    }
    return map;
  }
}
