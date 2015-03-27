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
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.entity.ContainerId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ContainerIdToCleanClusterJ implements
    TablesDef.ContainerIdToCleanTableDef,
    ContainerIdToCleanDataAccess<ContainerId> {

  private static final Log LOG =
      LogFactory.getLog(ContainerIdToCleanClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface ContainerIdToCleanDTO {

    @PrimaryKey
    @Column(name = RMNODEID)
    String getrmnodeid();

    void setrmnodeid(String rmnodeid);

    @PrimaryKey
    @Column(name = CONTAINERID)
    String getcontainerid();

    void setcontainerid(String containerid);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<ContainerId> findByRMNode(String rmnodeId)
      throws StorageException {
    LOG.debug(
        "HOP :: ClusterJ ContainerIdToClean.findByRMNode - START:" + rmnodeId);
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<ContainerIdToCleanDTO> dobj =
        qb.createQueryDefinition(ContainerIdToCleanDTO.class);
    HopsPredicate pred = dobj.get(RMNODEID).equal(dobj.param(RMNODEID));
    dobj.where(pred);
    HopsQuery<ContainerIdToCleanDTO> query = session.createQuery(dobj);
    query.setParameter(RMNODEID, rmnodeId);
    List<ContainerIdToCleanDTO> results = query.getResultList();
    LOG.debug(
        "HOP :: ClusterJ ContainerIdToClean.findByRMNode - FINISH:" + rmnodeId);
    if (results != null && !results.isEmpty()) {
      return createContainersToCleanList(results);
    }
    return null;
  }

  @Override
  public Map<String, Set<ContainerId>> getAll() throws StorageException {
    LOG.debug("HOP :: ClusterJ ContainerIdToClean.getAll - START");

    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ContainerIdToCleanDTO> dobj =
        qb.createQueryDefinition(ContainerIdToCleanDTO.class);
    HopsQuery<ContainerIdToCleanDTO> query = session.
        createQuery(dobj);
    List<ContainerIdToCleanDTO> results = query.
        getResultList();
    LOG.debug("HOP :: ClusterJ ContainerIdToClean.findByRMNode - FINISH");

    return createMap(results);
  }

  @Override
  public void addAll(Collection<ContainerId> containers)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerIdToCleanDTO> toModify =
        new ArrayList<ContainerIdToCleanDTO>();
    for (ContainerId hop : containers) {
      toModify.add(createPersistable(hop, session));
    }
    session.savePersistentAll(toModify);
    session.flush();
  }

  @Override

  public void removeAll(Collection<ContainerId> containers)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerIdToCleanDTO> toRemove =
        new ArrayList<ContainerIdToCleanDTO>();
    for (ContainerId hop : containers) {
      toRemove.add(createPersistable(hop, session));
    }
    session.deletePersistentAll(toRemove);
    session.flush();
  }

  private ContainerIdToCleanDTO createPersistable(ContainerId hop,
      HopsSession session) throws StorageException {
    ContainerIdToCleanDTO dto =
        session.newInstance(ContainerIdToCleanDTO.class);
    //Set values to persist new ContainerStatus
    dto.setrmnodeid(hop.getRmnodeid());
    dto.setcontainerid(hop.getContainerId());
    return dto;
  }

  private ContainerId createHopContainerIdToClean(ContainerIdToCleanDTO dto) {
    ContainerId hop = new ContainerId(dto.getrmnodeid(), dto.
        getcontainerid());
    return hop;
  }

  private List<ContainerId> createContainersToCleanList(
      List<ContainerIdToCleanDTO> results) {
    List<ContainerId> containersToClean = new ArrayList<ContainerId>();
    for (ContainerIdToCleanDTO persistable : results) {
      containersToClean.add(createHopContainerIdToClean(persistable));
    }
    return containersToClean;
  }

  private Map<String, Set<ContainerId>> createMap(
      List<ContainerIdToCleanDTO> results) {
    Map<String, Set<ContainerId>> map = new HashMap<String, Set<ContainerId>>();
    for (ContainerIdToCleanDTO dto : results) {
      ContainerId hop = createHopContainerIdToClean(dto);
      if (map.get(hop.getRmnodeid()) == null) {
        map.put(hop.getRmnodeid(), new HashSet<ContainerId>());
      }
      map.get(hop.getRmnodeid()).add(hop);
    }
    return map;
  }
}
