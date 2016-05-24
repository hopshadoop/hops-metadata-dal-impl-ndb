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
import io.hops.metadata.yarn.dal.capacity.FiCaSchedulerAppReservedContainersDataAccess;
import io.hops.metadata.yarn.entity.capacity.FiCaSchedulerAppReservedContainers;
import java.util.ArrayList;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FiCaSchedulerAppReservedContainersClusterJ
        implements TablesDef.FiCaSchedulerAppReservedContainersTableDef,
        FiCaSchedulerAppReservedContainersDataAccess<FiCaSchedulerAppReservedContainers> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface FiCaSchedulerAppReservedContainersDTO {

    @PrimaryKey
    @Column(name = SCHEDULERAPP_ID)
    String getschedulerappid();

    void setschedulerappid(String schedulerappid);

    @Column(name = PRIORITY_ID)
    int getpriorityid();

    void setpriorityid(int priorityid);

    @Column(name = NODEID)
    String getnodeid();

    void setnodeid(String nodeid);

    @Column(name = RMCONTAINER_ID)
    String getrmcontainerid();

    void setrmcontainerid(String rmcontainerid);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public FiCaSchedulerAppReservedContainers findById(int id)
          throws StorageException {
    HopsSession session = connector.obtainSession();

    FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO fiCaSchedulerAppReservedContainersDTO
            = null;
    if (session != null) {
      fiCaSchedulerAppReservedContainersDTO = session.find(
              FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO.class,
              id);
    }
    if (fiCaSchedulerAppReservedContainersDTO == null) {
      return null;
    }

    FiCaSchedulerAppReservedContainers result
            = createHopFiCaSchedulerAppReservedContainers(
                    fiCaSchedulerAppReservedContainersDTO);
    session.release(fiCaSchedulerAppReservedContainersDTO);
    return result;
  }

  @Override
  public void addAll(Collection<FiCaSchedulerAppReservedContainers> modified)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    if (modified != null) {
      List<FiCaSchedulerAppReservedContainersDTO> toAdd
              = new ArrayList<FiCaSchedulerAppReservedContainersDTO>();
      for (FiCaSchedulerAppReservedContainers hop : modified) {
        FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO persistable
                = createPersistable(hop, session);
        toAdd.add(persistable);
      }
      session.savePersistentAll(toAdd);
      session.release(toAdd);
    }
  }

  @Override
  public void removeAll(Collection<FiCaSchedulerAppReservedContainers> removed)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    if (removed != null) {
      List<FiCaSchedulerAppReservedContainersDTO> toRemove
              = new ArrayList<FiCaSchedulerAppReservedContainersDTO>();
      for (FiCaSchedulerAppReservedContainers hop : removed) {
        FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO persistable
                = session.newInstance(
                        FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO.class,
                        hop.getSchedulerapp_id());
        toRemove.add(persistable);
      }
      session.deletePersistentAll(toRemove);
      session.release(toRemove);
    }
  }

  @Override
  public Map<String, List<FiCaSchedulerAppReservedContainers>> getAll() throws
          StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO> dobj
            = qb.createQueryDefinition(
                    FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO.class);
    HopsQuery<FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO> query
            = session.
            createQuery(dobj);
    List<FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO> queryResults
            = query.
            getResultList();
    Map<String, List<FiCaSchedulerAppReservedContainers>> result = createMap(
            queryResults);
    session.release(queryResults);
    return result;
  }

  private FiCaSchedulerAppReservedContainers createHopFiCaSchedulerAppReservedContainers(
          FiCaSchedulerAppReservedContainersDTO fiCaSchedulerAppReservedContainersDTO) {
    return new FiCaSchedulerAppReservedContainers(
            fiCaSchedulerAppReservedContainersDTO.getschedulerappid(),
            fiCaSchedulerAppReservedContainersDTO.getpriorityid(),
            fiCaSchedulerAppReservedContainersDTO.getnodeid(),
            fiCaSchedulerAppReservedContainersDTO.getrmcontainerid());
  }

  private FiCaSchedulerAppReservedContainersDTO createPersistable(
          FiCaSchedulerAppReservedContainers hop, HopsSession session)
          throws StorageException {
    FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO fiCaSchedulerAppReservedContainersDTO
            = session.newInstance(
                    FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO.class);

    fiCaSchedulerAppReservedContainersDTO
            .setschedulerappid(hop.getSchedulerapp_id());
    fiCaSchedulerAppReservedContainersDTO.setpriorityid(hop.getPriority_id());
    fiCaSchedulerAppReservedContainersDTO.setnodeid(hop.getNodeid());
    fiCaSchedulerAppReservedContainersDTO
            .setrmcontainerid(hop.getRmcontainer_id());

    return fiCaSchedulerAppReservedContainersDTO;
  }

  private Map<String, List<FiCaSchedulerAppReservedContainers>> createMap(
          List<FiCaSchedulerAppReservedContainersDTO> results) {
    Map<String, List<FiCaSchedulerAppReservedContainers>> map
            = new HashMap<String, List<FiCaSchedulerAppReservedContainers>>();
    for (FiCaSchedulerAppReservedContainersDTO dto : results) {
      FiCaSchedulerAppReservedContainers hop
              = createHopFiCaSchedulerAppReservedContainers(dto);
      if (map.get(hop.getSchedulerapp_id()) == null) {
        map.put(hop.getSchedulerapp_id(),
                new ArrayList<FiCaSchedulerAppReservedContainers>());
      }
      map.get(hop.getSchedulerapp_id()).add(hop);
    }
    return map;
  }
}
