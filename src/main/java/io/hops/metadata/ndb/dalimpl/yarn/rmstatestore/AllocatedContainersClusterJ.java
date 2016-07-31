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
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.rmstatestore.AllocatedContainersDataAccess;
import io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.hops.metadata.yarn.entity.rmstatestore.ApplicationAttemptState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AllocatedContainersClusterJ implements
        TablesDef.AllocatedContainersTableDef,
        AllocatedContainersDataAccess<AllocateResponse> {
public static final Log LOG = LogFactory.getLog(AllocatedContainersClusterJ.class);
  @PersistenceCapable(table = TABLE_NAME)
  public interface AllocatedContainerDTO {

    @PrimaryKey
    @Column(name = APPLICATIONATTEMPTID)
    String getapplicationattemptid();

    void setapplicationattemptid(String applicationattemptid);

    @PrimaryKey
    @Column(name = CONTAINERID)
    String getcontainerid();

    void setcontainerid(String containerid);
    
    @PrimaryKey
    @Column(name = RESPONSEID)
    int getresponseid();
    
    void setresponseid(int id);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  public static int add =0;
  public static int remove =0;
  
  static double nbPersist =0;
  static double tt1=0;
  static double tt2=0;
  static double tt3 = 0;
  public void update(Collection<AllocateResponse> entries) throws
          StorageException {
    long start = System.currentTimeMillis();
    HopsSession session = connector.obtainSession();
    session.flush();
    List<AllocatedContainerDTO> toPersist
            = new ArrayList<AllocatedContainerDTO>();
    for (AllocateResponse resp : entries) {
      //put new values
      toPersist.addAll(createPersistable(resp, session));
      //remove old values
      HopsQueryBuilder qb = session.getQueryBuilder();
      HopsQueryDomainType<AllocatedContainerDTO> dobj = qb.
              createQueryDefinition(AllocatedContainerDTO.class);
      HopsPredicate pred1 = dobj.get(APPLICATIONATTEMPTID).equal(dobj.param(
              APPLICATIONATTEMPTID));
      dobj.where(pred1);
      HopsQuery<AllocatedContainerDTO> query = session.createQuery(dobj);
      query.setParameter(APPLICATIONATTEMPTID, resp.getApplicationattemptid());
      remove++;
      query.deletePersistentAll();
    }
    tt2 = tt2 + System.currentTimeMillis() - start;
    add += toPersist.size();
    session.savePersistentAll(toPersist);
    tt3 = tt3 + System.currentTimeMillis() - start;
    session.release(toPersist);
    nbPersist++;
    if (nbPersist % 100 == 0) {
      double avgt1 = tt1 / nbPersist;
      double avgt2 = tt2 / nbPersist;
      double avgt3 = tt3 / nbPersist;
      LOG.info("allocated containers update avg time: " + avgt1 + ", " + avgt2
              + ", " + avgt3);
    }
  }

  @Override
  public void removeAll(Collection<AllocateResponse> entries) throws
          StorageException {
    if (entries.isEmpty()) {
      return;
    }

    HopsSession session = connector.obtainSession();
    List<AllocatedContainerDTO> toRemove =
            new ArrayList<AllocatedContainerDTO>();
    for (AllocateResponse hop : entries) {
      List<AllocatedContainerDTO> pers =
              createPersistable(hop, session);
      if (!pers.isEmpty()) {
        toRemove.addAll(pers);
      }
    }

    if (!toRemove.isEmpty()) {
      session.deletePersistentAll(toRemove);
      session.release(toRemove);
    }
  }

  public Map<String, List<String>> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<AllocatedContainerDTO> dobj = qb.createQueryDefinition(
            AllocatedContainerDTO.class);
    HopsQuery<AllocatedContainerDTO> query = session.createQuery(dobj);
    List<AllocatedContainerDTO> queryResults = query.getResultList();
    Map<String, List<String>> result = createHopAllocatedContainersMap(
            queryResults);
    session.release(queryResults);
    return result;
  }

  private List<AllocatedContainerDTO> createPersistable(AllocateResponse hop,
          HopsSession session) throws StorageException {
    List<AllocatedContainerDTO> result = new ArrayList<AllocatedContainerDTO>();
    for (String containerId : hop.getAllocatedContainers()) {
      AllocatedContainerDTO allocatedContainerDTO = session.newInstance(
              AllocatedContainerDTO.class);
      allocatedContainerDTO.setapplicationattemptid(hop.
              getApplicationattemptid());
      allocatedContainerDTO.setcontainerid(containerId);
      allocatedContainerDTO.setresponseid(hop.getResponseId());
      result.add(allocatedContainerDTO);
    }
    return result;
  }

  private Map<String, List<String>> createHopAllocatedContainersMap(
          List<AllocatedContainerDTO> list) throws StorageException {
    Map<String, List<String>> allocatedContainersMap
            = new HashMap<String, List<String>>();

    for (AllocatedContainerDTO dto : list) {
      if (allocatedContainersMap.get(dto.getapplicationattemptid()) == null) {
        allocatedContainersMap.put(dto.getapplicationattemptid(),
                new ArrayList<String>());
      }
      allocatedContainersMap.get(dto.getapplicationattemptid()).add(dto.
              getcontainerid());
    }
    return allocatedContainersMap;
  }
}
