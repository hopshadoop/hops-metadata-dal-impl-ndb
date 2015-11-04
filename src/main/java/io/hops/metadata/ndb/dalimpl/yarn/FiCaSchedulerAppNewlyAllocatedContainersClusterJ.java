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
import io.hops.metadata.yarn.dal.FiCaSchedulerAppNewlyAllocatedContainersDataAccess;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FiCaSchedulerAppNewlyAllocatedContainersClusterJ
    implements TablesDef.FiCaSchedulerAppNewlyAllocatedContainersTableDef,
    FiCaSchedulerAppNewlyAllocatedContainersDataAccess<FiCaSchedulerAppContainer> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface FiCaSchedulerAppNewlyAllocatedContainersDTO {

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
  public List<FiCaSchedulerAppContainer> findById(String ficaId)
      throws StorageException {

    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<FiCaSchedulerAppNewlyAllocatedContainersDTO> dobj =
        qb.createQueryDefinition(
            FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO.class);
    HopsPredicate pred1 =
        dobj.get("schedulerapp_id").equal(dobj.param("schedulerapp_id"));
    dobj.where(pred1);
    HopsQuery<FiCaSchedulerAppNewlyAllocatedContainersDTO> query =
        session.createQuery(dobj);
    query.setParameter("schedulerapp_id", ficaId);

    List<FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO>
        queryResults = query.getResultList();
    List<FiCaSchedulerAppContainer> result =
            createFiCaSchedulerAppNewlyAllocatedContainersList(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public Map<String, List<FiCaSchedulerAppContainer>> getAll()
      throws IOException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<FiCaSchedulerAppNewlyAllocatedContainersDTO> dobj =
        qb.createQueryDefinition(
            FiCaSchedulerAppNewlyAllocatedContainersDTO.class);
    HopsQuery<FiCaSchedulerAppNewlyAllocatedContainersDTO> query = session.
        createQuery(dobj);
    List<FiCaSchedulerAppNewlyAllocatedContainersDTO> queryResults = query.
        getResultList();
    Map<String, List<FiCaSchedulerAppContainer>> result = 
            createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public void addAll(Collection<FiCaSchedulerAppContainer> toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FiCaSchedulerAppNewlyAllocatedContainersDTO> toPersist =
        new ArrayList<FiCaSchedulerAppNewlyAllocatedContainersDTO>();
    for (FiCaSchedulerAppContainer hop : toAdd) {
      FiCaSchedulerAppNewlyAllocatedContainersClusterJ.FiCaSchedulerAppNewlyAllocatedContainersDTO
          persistable = createPersistable(hop, session);
      toPersist.add(persistable);
    }
    session.savePersistentAll(toPersist);
    session.release(toPersist);
  }

  @Override
  public void removeAll(
      Collection<FiCaSchedulerAppContainer> toRemove)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FiCaSchedulerAppNewlyAllocatedContainersDTO> toPersist =
        new ArrayList<FiCaSchedulerAppNewlyAllocatedContainersDTO>();
    for (FiCaSchedulerAppContainer hop : toRemove) {

      Object[] objarr = new Object[2];
      objarr[0] = hop.getSchedulerapp_id();
      objarr[1] = hop.getRmcontainer_id();
      toPersist.add(session
          .newInstance(FiCaSchedulerAppNewlyAllocatedContainersDTO.class,
              objarr));
    }
    session.deletePersistentAll(toPersist);
    session.release(toPersist);
  }

  private FiCaSchedulerAppContainer createHopFiCaSchedulerAppNewlyAllocatedContainers(
      FiCaSchedulerAppNewlyAllocatedContainersDTO fiCaSchedulerAppNewlyAllocatedContainersDTO) {
    return new FiCaSchedulerAppContainer(
        fiCaSchedulerAppNewlyAllocatedContainersDTO.getschedulerapp_id(),
        fiCaSchedulerAppNewlyAllocatedContainersDTO.getrmcontainerid());
  }

  private FiCaSchedulerAppNewlyAllocatedContainersDTO createPersistable(
      FiCaSchedulerAppContainer hop, HopsSession session)
      throws StorageException {
    FiCaSchedulerAppNewlyAllocatedContainersDTO
        fiCaSchedulerAppNewlyAllocatedContainersDTO =
        session.newInstance(FiCaSchedulerAppNewlyAllocatedContainersDTO.class);

    fiCaSchedulerAppNewlyAllocatedContainersDTO.setschedulerapp_id(hop.
        getSchedulerapp_id());
    fiCaSchedulerAppNewlyAllocatedContainersDTO.setrmcontainerid(hop.
        getRmcontainer_id());

    return fiCaSchedulerAppNewlyAllocatedContainersDTO;
  }

  private List<FiCaSchedulerAppContainer> createFiCaSchedulerAppNewlyAllocatedContainersList(
      List<FiCaSchedulerAppNewlyAllocatedContainersDTO> results) {
    List<FiCaSchedulerAppContainer>
        ficaSchedulerAppNewlyAllocatedContainers =
        new ArrayList<FiCaSchedulerAppContainer>();
    for (FiCaSchedulerAppNewlyAllocatedContainersDTO persistable : results) {
      ficaSchedulerAppNewlyAllocatedContainers
          .add(createHopFiCaSchedulerAppNewlyAllocatedContainers(persistable));
    }
    return ficaSchedulerAppNewlyAllocatedContainers;
  }

  private Map<String, List<FiCaSchedulerAppContainer>> createMap(
      List<FiCaSchedulerAppNewlyAllocatedContainersDTO> results) {
    Map<String, List<FiCaSchedulerAppContainer>> map =
        new HashMap<String, List<FiCaSchedulerAppContainer>>();
    for (FiCaSchedulerAppNewlyAllocatedContainersDTO persistable : results) {
      FiCaSchedulerAppContainer hop =
          createHopFiCaSchedulerAppNewlyAllocatedContainers(persistable);
      if (map.get(hop.getSchedulerapp_id()) == null) {
        map.put(hop.getSchedulerapp_id(),
            new ArrayList<FiCaSchedulerAppContainer>());
      }
      map.get(hop.getSchedulerapp_id()).add(hop);
    }
    return map;
  }
}
