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
import io.hops.metadata.yarn.dal.FiCaSchedulerAppLastScheduledContainerDataAccess;
import io.hops.metadata.yarn.entity.FiCaSchedulerAppLastScheduledContainer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FiCaSchedulerAppLastScheduledContainerClusterJ implements
        TablesDef.FiCaSchedulerAppLastScheduledContainerTableDef,
        FiCaSchedulerAppLastScheduledContainerDataAccess<FiCaSchedulerAppLastScheduledContainer> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface SchedulerAppLastScheduledContainerDTO {

    @PrimaryKey
    @Column(name = SCHEDULERAPP_ID)
    String getschedulerapp_id();

    void setschedulerapp_id(String schedulerappid);

    @Column(name = PRIORITY_ID)
    int getpriorityid();

    void setpriorityid(int priorityid);

    @Column(name = TIME)
    long gettime();

    void settime(long time);
  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<FiCaSchedulerAppLastScheduledContainer> findById(
          String appAttemptId)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<FiCaSchedulerAppLastScheduledContainerClusterJ.SchedulerAppLastScheduledContainerDTO> dobj
            = qb.createQueryDefinition(
                    FiCaSchedulerAppLastScheduledContainerClusterJ.SchedulerAppLastScheduledContainerDTO.class);
    HopsPredicate pred1 = dobj.get("schedulerapp_id").equal(dobj.param(
            "schedulerapp_id"));
    dobj.where(pred1);
    HopsQuery<FiCaSchedulerAppLastScheduledContainerClusterJ.SchedulerAppLastScheduledContainerDTO> query
            = session.createQuery(dobj);
    query.setParameter("schedulerapp_id", appAttemptId);

    List<FiCaSchedulerAppLastScheduledContainerClusterJ.SchedulerAppLastScheduledContainerDTO> queryResults
            = query.getResultList();
    List<FiCaSchedulerAppLastScheduledContainer> result
            = createReservationsList(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public void addAll(Collection<FiCaSchedulerAppLastScheduledContainer> modified)
          throws StorageException {
    HopsSession session = connector.obtainSession();

      if (modified != null) {
        List<FiCaSchedulerAppLastScheduledContainerClusterJ.SchedulerAppLastScheduledContainerDTO> toAdd
                = new ArrayList<FiCaSchedulerAppLastScheduledContainerClusterJ.SchedulerAppLastScheduledContainerDTO>();
        for (FiCaSchedulerAppLastScheduledContainer hop : modified) {
          FiCaSchedulerAppLastScheduledContainerClusterJ.SchedulerAppLastScheduledContainerDTO persistable
                  = createPersistable(hop, session);
          toAdd.add(persistable);
        }
        session.savePersistentAll(toAdd);
        session.release(toAdd);
      }
  }

  @Override
  public void removeAll(
          Collection<FiCaSchedulerAppLastScheduledContainer> removed)
          throws StorageException {
    HopsSession session = connector.obtainSession();
      if (removed != null) {
        List<FiCaSchedulerAppLastScheduledContainerClusterJ.SchedulerAppLastScheduledContainerDTO> toRemove
                = new ArrayList<FiCaSchedulerAppLastScheduledContainerClusterJ.SchedulerAppLastScheduledContainerDTO>();
        for (FiCaSchedulerAppLastScheduledContainer hop : removed) {
          Object[] objarr = new Object[2];
          objarr[0] = hop.getSchedulerapp_id();
          objarr[1] = hop.getPriority_id();
          toRemove.add(session.newInstance(
                  FiCaSchedulerAppLastScheduledContainerClusterJ.SchedulerAppLastScheduledContainerDTO.class,
                  objarr));
        }
        session.deletePersistentAll(toRemove);
        session.release(toRemove);
      }
  }

  @Override
  public Map<String, List<FiCaSchedulerAppLastScheduledContainer>> getAll()
          throws
          StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<FiCaSchedulerAppLastScheduledContainerClusterJ.SchedulerAppLastScheduledContainerDTO> dobj
            = qb.createQueryDefinition(
                    FiCaSchedulerAppLastScheduledContainerClusterJ.SchedulerAppLastScheduledContainerDTO.class);
    HopsQuery<FiCaSchedulerAppLastScheduledContainerClusterJ.SchedulerAppLastScheduledContainerDTO> query
            = session.
            createQuery(dobj);
    List<FiCaSchedulerAppLastScheduledContainerClusterJ.SchedulerAppLastScheduledContainerDTO> queryResults
            = query.
            getResultList();
    Map<String, List<FiCaSchedulerAppLastScheduledContainer>> result =
            createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  private FiCaSchedulerAppLastScheduledContainer createHopFiCaSchedulerAppLastScheduledContainer(
          SchedulerAppLastScheduledContainerDTO fiCaSchedulerAppLastScheduledContainerDTO) {
    return new FiCaSchedulerAppLastScheduledContainer(
            fiCaSchedulerAppLastScheduledContainerDTO.getschedulerapp_id(),
            fiCaSchedulerAppLastScheduledContainerDTO.getpriorityid(),
            fiCaSchedulerAppLastScheduledContainerDTO.gettime());
  }

  private SchedulerAppLastScheduledContainerDTO createPersistable(
          FiCaSchedulerAppLastScheduledContainer hop, HopsSession session)
          throws
          StorageException {
    SchedulerAppLastScheduledContainerDTO fiCaSchedulerAppLastScheduledContainerDTO
            = session.newInstance(SchedulerAppLastScheduledContainerDTO.class);

    fiCaSchedulerAppLastScheduledContainerDTO.setschedulerapp_id(hop.
            getSchedulerapp_id());
    fiCaSchedulerAppLastScheduledContainerDTO.
            setpriorityid(hop.getPriority_id());
    fiCaSchedulerAppLastScheduledContainerDTO.settime(hop.getTime());

    return fiCaSchedulerAppLastScheduledContainerDTO;
  }

  private List<FiCaSchedulerAppLastScheduledContainer> createReservationsList(
          List<SchedulerAppLastScheduledContainerDTO> results) {
    List<FiCaSchedulerAppLastScheduledContainer> appLastScheduledContainers
            = new ArrayList<FiCaSchedulerAppLastScheduledContainer>();
    for (SchedulerAppLastScheduledContainerDTO persistable : results) {
      appLastScheduledContainers.add(
              createHopFiCaSchedulerAppLastScheduledContainer(persistable));
    }
    return appLastScheduledContainers;
  }

  private Map<String, List<FiCaSchedulerAppLastScheduledContainer>> createMap(
          List<SchedulerAppLastScheduledContainerDTO> results) {
    Map<String, List<FiCaSchedulerAppLastScheduledContainer>> map
            = new HashMap<String, List<FiCaSchedulerAppLastScheduledContainer>>();
    for (SchedulerAppLastScheduledContainerDTO dto : results) {
      FiCaSchedulerAppLastScheduledContainer hop
              = createHopFiCaSchedulerAppLastScheduledContainer(dto);
      if (map.get(hop.getSchedulerapp_id()) == null) {
        map.put(hop.getSchedulerapp_id(),
                new ArrayList<FiCaSchedulerAppLastScheduledContainer>());
      }
      map.get(hop.getSchedulerapp_id()).add(hop);
    }
    return map;
  }
}
