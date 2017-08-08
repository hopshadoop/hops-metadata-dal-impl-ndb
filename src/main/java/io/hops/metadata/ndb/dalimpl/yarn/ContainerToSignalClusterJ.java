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
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.ContainerToSignalDataAccess;
import io.hops.metadata.yarn.entity.ContainerToSignal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class ContainerToSignalClusterJ implements TablesDef.ContainerToSignalTableDef, ContainerToSignalDataAccess<ContainerToSignal> {
  
  @PersistenceCapable(table = TABLE_NAME)
  public interface ContainerToSignalDTO {

    @PrimaryKey
    @Column(name = RMNODEID)
    String getRmnodeid();

    void setRmnodeid(String rmnodeid);

    @PrimaryKey
    @Column(name = CONTAINERID)
    String getContainerid();

    void setContainerid(String containerid);
    
    @PrimaryKey
    @Column(name = COMMAND)
    String getCommand();

    void setCommand(String command);
    
  }
  
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<ContainerToSignal> findByRMNode(String rmnodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<ContainerToSignalDTO> dobj = qb.createQueryDefinition(ContainerToSignalDTO.class);
    HopsPredicate pred = dobj.get(RMNODEID).equal(dobj.param(RMNODEID));
    dobj.where(pred);
    HopsQuery<ContainerToSignalDTO> query = session.createQuery(dobj);
    query.setParameter(RMNODEID, rmnodeId);
    List<ContainerToSignalDTO> queryResults = query.getResultList();
    
    if (queryResults != null && !queryResults.isEmpty()) {
      List<ContainerToSignal> results = createContainersToSignalList(queryResults);
      session.release(queryResults);
      return results;
    }
    return null;
  }

  @Override
  public Map<String, Set<ContainerToSignal>> getAll() throws StorageException {

    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ContainerToSignalDTO> dobj = qb.createQueryDefinition(ContainerToSignalDTO.class);
    HopsQuery<ContainerToSignalDTO> query = session.createQuery(dobj);
    List<ContainerToSignalDTO> queryResults = query.getResultList();
    Map<String, Set<ContainerToSignal>> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public void addAll(Collection<ContainerToSignal> containers) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerToSignalDTO> toModify = new ArrayList<>();
    for (ContainerToSignal hop : containers) {
      toModify.add(createPersistable(hop, session));
    }
    session.savePersistentAll(toModify);
    session.release(toModify);
  }
  
  @Override
  public void add(ContainerToSignal container) throws StorageException {
    HopsSession session = connector.obtainSession();
   ContainerToSignalDTO toModify = createPersistable(container, session);
    session.savePersistent(toModify);
    session.release(toModify);
  }
  
  @Override
  public void removeAll(Collection<ContainerToSignal> containers)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerToSignalDTO> toRemove = new ArrayList<>();
    for (ContainerToSignal hop : containers) {
      toRemove.add(createPersistable(hop, session));
    }
    session.deletePersistentAll(toRemove);
    session.release(toRemove);
  }

  private ContainerToSignalDTO createPersistable(ContainerToSignal hop, HopsSession session) throws StorageException {
    ContainerToSignalDTO dto = session.newInstance(ContainerToSignalDTO.class);
    //Set values to persist new ContainerStatus
    dto.setRmnodeid(hop.getRmnodeid());
    dto.setContainerid(hop.getContainerId());
    dto.setCommand(hop.getCommand());

    return dto;
  }

  private ContainerToSignal createHopContainerIdToClean(ContainerToSignalDTO dto) {
    ContainerToSignal hop = new ContainerToSignal(dto.getRmnodeid(), dto.getContainerid(), dto.getCommand());
    return hop;
  }

  private List<ContainerToSignal> createContainersToSignalList(List<ContainerToSignalDTO> results) {
    List<ContainerToSignal> containersToClean = new ArrayList<>();
    for (ContainerToSignalDTO persistable : results) {
      containersToClean.add(createHopContainerIdToClean(persistable));
    }
    return containersToClean;
  }

  private Map<String, Set<ContainerToSignal>> createMap(List<ContainerToSignalDTO> results) {
    Map<String, Set<ContainerToSignal>> map = new HashMap<>();
    for (ContainerToSignalDTO dto : results) {
      ContainerToSignal hop = createHopContainerIdToClean(dto);
      if (map.get(hop.getRmnodeid()) == null) {
        map.put(hop.getRmnodeid(), new HashSet<ContainerToSignal>());
      }
      map.get(hop.getRmnodeid()).add(hop);
    }
    return map;
  }
}
