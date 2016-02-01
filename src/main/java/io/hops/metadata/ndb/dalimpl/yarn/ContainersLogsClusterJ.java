/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
import io.hops.metadata.yarn.dal.ContainersLogsDataAccess;
import io.hops.metadata.yarn.entity.ContainersLogs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ContainersLogsClusterJ implements
        TablesDef.ContainersLogsTableDef,
        ContainersLogsDataAccess<ContainersLogs> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ContainersLogsDTO {

    @PrimaryKey
    @Column(name = CONTAINERID)
    String getcontainerid();

    void setcontainerid(String containerid);

    @Column(name = START)
    long getstart();

    void setstart(long start);

    @Column(name = STOP)
    long getstop();

    void setstop(long stop);

    @Column(name = EXITSTATUS)
    int getexitstatus();

    void setexitstatus(int exitstate);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void addAll(Collection<ContainersLogs> containersLogs) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainersLogsDTO> toAdd = new ArrayList<ContainersLogsDTO>();

    for (ContainersLogs entry : containersLogs) {
      toAdd.add(createPersistable(entry, session));
    }

    session.savePersistentAll(toAdd);
    session.release(toAdd);
  }

  @Override
  public void removeAll(Collection<ContainersLogs> containersLogs) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainersLogsDTO> toRemove = new ArrayList<ContainersLogsDTO>();

    for (ContainersLogs entry : containersLogs) {
      toRemove.add(createPersistable(entry, session));
    }

    session.deletePersistentAll(toRemove);
    session.flush();
    session.release(toRemove);
  }
  
  @Override
  public Map<String, ContainersLogs> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<ContainersLogsDTO> dobj = qb.createQueryDefinition(
            ContainersLogsDTO.class);
    HopsQuery<ContainersLogsDTO> query = session.createQuery(dobj);

    List<ContainersLogsDTO> queryResults = query.getResultList();
    Map<String, ContainersLogs> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public Map<String, ContainersLogs> getByExitStatus(int exitstatus) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<ContainersLogsDTO> dobj = qb.createQueryDefinition(
            ContainersLogsDTO.class);
    HopsQuery<ContainersLogsDTO> query = session.createQuery(dobj);
    // Search by unfinished container statuses, to avoid retrieving all
    query.setParameter(EXITSTATUS, exitstatus);

    List<ContainersLogsDTO> queryResults = query.getResultList();
    Map<String, ContainersLogs> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  private ContainersLogsDTO createPersistable(ContainersLogs hopCL,
          HopsSession session) throws StorageException {
    ContainersLogsDTO clDTO = session.newInstance(ContainersLogsDTO.class);

    //Set values to persist new ContainersLogs
    clDTO.setcontainerid(hopCL.getContainerid());
    clDTO.setstart(hopCL.getStart());
    clDTO.setstop(hopCL.getStop());
    clDTO.setexitstatus(hopCL.getExitstatus());
    return clDTO;
  }

  private static Map<String, ContainersLogs> createMap(
          List<ContainersLogsDTO> results) {
    Map<String, ContainersLogs> map = new HashMap<String, ContainersLogs>();
    for (ContainersLogsDTO persistable : results) {
      ContainersLogs hop = createHopContainersLogs(persistable);
      map.put(hop.getContainerid(), hop);
    }
    return map;
  }

  private static ContainersLogs createHopContainersLogs(
          ContainersLogsDTO clDTO) {
    ContainersLogs hop = new ContainersLogs(
            clDTO.getcontainerid(),
            clDTO.getstart(),
            clDTO.getstop(),
            clDTO.getexitstatus()
    );
    return hop;
  }
}
