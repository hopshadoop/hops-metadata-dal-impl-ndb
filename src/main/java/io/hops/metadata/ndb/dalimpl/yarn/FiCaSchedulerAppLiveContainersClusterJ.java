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
import io.hops.metadata.yarn.dal.FiCaSchedulerAppLiveContainersDataAccess;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppContainer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FiCaSchedulerAppLiveContainersClusterJ
    implements TablesDef.FiCaSchedulerAppLiveContainersTableDef,
    FiCaSchedulerAppLiveContainersDataAccess<FiCaSchedulerAppContainer> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface FiCaSchedulerAppLiveContainersDTO {

    @PrimaryKey
    @Column(name = SCHEDULERAPP_ID)
    String getschedulerapp_id();

    void setschedulerapp_id(String schedulerapp_id);

    @PrimaryKey
    @Column(name = RMCONTAINER_ID)
    String getrmcontainerid();

    void setrmcontainerid(String rmcontainerid);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();


  @Override
  public Map<String, List<FiCaSchedulerAppContainer>> getAll()
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<FiCaSchedulerAppLiveContainersDTO> dobj =
        qb.createQueryDefinition(FiCaSchedulerAppLiveContainersDTO.class);
    HopsQuery<FiCaSchedulerAppLiveContainersDTO> query = session.
        createQuery(dobj);
    List<FiCaSchedulerAppLiveContainersDTO> queryResults = query.
        getResultList();
    Map<String, List<FiCaSchedulerAppContainer>> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public void addAll(Collection<FiCaSchedulerAppContainer> toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FiCaSchedulerAppLiveContainersDTO> toPersist =
        new ArrayList<FiCaSchedulerAppLiveContainersDTO>();
    for (FiCaSchedulerAppContainer container : toAdd) {
      FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO
          persistable = createPersistable(container, session);
      toPersist.add(persistable);
    }
    session.savePersistentAll(toPersist);
    session.release(toPersist);
  }

  @Override
  public void removeAll(Collection<FiCaSchedulerAppContainer> toRemove)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FiCaSchedulerAppLiveContainersDTO> toPersist =
        new ArrayList<FiCaSchedulerAppLiveContainersDTO>();
    for (FiCaSchedulerAppContainer container : toRemove) {
      Object[] objarr = new Object[2];
      objarr[0] = container.getSchedulerapp_id();
      objarr[1] = container.getRmcontainer_id();
      toPersist.add(session.newInstance(
          FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO.class,
          objarr));
    }
    session.deletePersistentAll(toPersist);
    session.release(toPersist);
  }

  private FiCaSchedulerAppContainer createHopFiCaSchedulerAppLiveContainers(
      FiCaSchedulerAppLiveContainersDTO fiCaSchedulerAppLiveContainersDTO) {
    return new FiCaSchedulerAppContainer(
        fiCaSchedulerAppLiveContainersDTO.getschedulerapp_id(),
        fiCaSchedulerAppLiveContainersDTO.getrmcontainerid());
  }

  private FiCaSchedulerAppLiveContainersDTO createPersistable(
      FiCaSchedulerAppContainer hop, HopsSession session)
      throws StorageException {
    FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO
        fiCaSchedulerAppLiveContainersDTO = session.newInstance(
        FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO.class);

    fiCaSchedulerAppLiveContainersDTO.setschedulerapp_id(hop.
        getSchedulerapp_id());
    fiCaSchedulerAppLiveContainersDTO.setrmcontainerid(hop.getRmcontainer_id());

    return fiCaSchedulerAppLiveContainersDTO;
  }

  private List<FiCaSchedulerAppContainer> createFiCaSchedulerAppLiveContainersList(
      List<FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO> results) {
    List<FiCaSchedulerAppContainer> ficaSchedulerAppLiveContainers =
        new ArrayList<FiCaSchedulerAppContainer>();
    for (FiCaSchedulerAppLiveContainersClusterJ.FiCaSchedulerAppLiveContainersDTO persistable : results) {
      ficaSchedulerAppLiveContainers
          .add(createHopFiCaSchedulerAppLiveContainers(persistable));
    }
    return ficaSchedulerAppLiveContainers;
  }

  private Map<String, List<FiCaSchedulerAppContainer>> createMap(
      List<FiCaSchedulerAppLiveContainersDTO> results) {
    Map<String, List<FiCaSchedulerAppContainer>> map =
        new HashMap<String, List<FiCaSchedulerAppContainer>>();
    for (FiCaSchedulerAppLiveContainersDTO dto : results) {
      FiCaSchedulerAppContainer hop =
          createHopFiCaSchedulerAppLiveContainers(dto);
      if (map.get(hop.getSchedulerapp_id()) == null) {
        map.put(hop.getSchedulerapp_id(),
            new ArrayList<FiCaSchedulerAppContainer>());
      }
      map.get(hop.getSchedulerapp_id()).add(hop);
    }
    return map;
  }
}
