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
import io.hops.metadata.yarn.entity.FiCaSchedulerAppSchedulingOpportunities;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppSchedulingOpportunitiesDataAccess;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FiCaSchedulerAppSchedulingOpportunitiesClusterJ implements
        TablesDef.FiCaSchedulerAppSchedulingOpportunitiesTableDef,
        FiCaSchedulerAppSchedulingOpportunitiesDataAccess<FiCaSchedulerAppSchedulingOpportunities> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface SchedulerAppSchedulingOpportunitiesDTO {

    @PrimaryKey
    @Column(name = SCHEDULERAPP_ID)
    String getschedulerapp_id();

    void setschedulerapp_id(String schedulerapp_id);

    @Column(name = PRIORITY_ID)
    int getpriorityid();

    void setpriorityid(int priorityid);

    @Column(name = COUNTER)
    int getcounter();

    void setcounter(int counter);
  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<FiCaSchedulerAppSchedulingOpportunities> findById(
          String appAttemptId)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<FiCaSchedulerAppSchedulingOpportunitiesClusterJ.SchedulerAppSchedulingOpportunitiesDTO> dobj
            = qb.createQueryDefinition(
                    FiCaSchedulerAppSchedulingOpportunitiesClusterJ.SchedulerAppSchedulingOpportunitiesDTO.class);
    HopsPredicate pred1 = dobj.get("schedulerapp_id").equal(dobj.param(
            "schedulerapp_id"));
    dobj.where(pred1);
    HopsQuery<FiCaSchedulerAppSchedulingOpportunitiesClusterJ.SchedulerAppSchedulingOpportunitiesDTO> query
            = session.createQuery(dobj);
    query.setParameter("schedulerapp_id", appAttemptId);

    List<FiCaSchedulerAppSchedulingOpportunitiesClusterJ.SchedulerAppSchedulingOpportunitiesDTO> queryResults
            = query.getResultList();
    List<FiCaSchedulerAppSchedulingOpportunities> result
            = createSchedulingOpportunitiesList(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public void addAll(
          Collection<FiCaSchedulerAppSchedulingOpportunities> modified)
          throws StorageException {
    HopsSession session = connector.obtainSession();
      if (modified != null) {
        List<FiCaSchedulerAppSchedulingOpportunitiesClusterJ.SchedulerAppSchedulingOpportunitiesDTO> toAdd
                = new ArrayList<FiCaSchedulerAppSchedulingOpportunitiesClusterJ.SchedulerAppSchedulingOpportunitiesDTO>();
        for (FiCaSchedulerAppSchedulingOpportunities hop : modified) {
          FiCaSchedulerAppSchedulingOpportunitiesClusterJ.SchedulerAppSchedulingOpportunitiesDTO persistable
                  = createPersistable(hop, session);
          toAdd.add(persistable);
        }
        session.savePersistentAll(toAdd);
        session.release(toAdd);
      }
  }

  @Override
  public void removeAll(
          Collection<FiCaSchedulerAppSchedulingOpportunities> removed)
          throws StorageException {
    HopsSession session = connector.obtainSession();
      if (removed != null) {
        List<FiCaSchedulerAppSchedulingOpportunitiesClusterJ.SchedulerAppSchedulingOpportunitiesDTO> toRemove
                = new ArrayList<FiCaSchedulerAppSchedulingOpportunitiesClusterJ.SchedulerAppSchedulingOpportunitiesDTO>();
        for (FiCaSchedulerAppSchedulingOpportunities hop : removed) {
          Object[] objarr = new Object[2];
          objarr[0] = hop.getSchedulerapp_id();
          objarr[1] = hop.getPriority_id();
          toRemove.add(session.newInstance(
                  FiCaSchedulerAppSchedulingOpportunitiesClusterJ.SchedulerAppSchedulingOpportunitiesDTO.class,
                  objarr));
        }
        session.deletePersistentAll(toRemove);
        session.release(toRemove);
      }
  }

  @Override
  public Map<String, List<FiCaSchedulerAppSchedulingOpportunities>> getAll()
          throws
          StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<FiCaSchedulerAppSchedulingOpportunitiesClusterJ.SchedulerAppSchedulingOpportunitiesDTO> dobj
            = qb.createQueryDefinition(
                    FiCaSchedulerAppSchedulingOpportunitiesClusterJ.SchedulerAppSchedulingOpportunitiesDTO.class);
    HopsQuery<FiCaSchedulerAppSchedulingOpportunitiesClusterJ.SchedulerAppSchedulingOpportunitiesDTO> query
            = session.
            createQuery(dobj);
    List<FiCaSchedulerAppSchedulingOpportunitiesClusterJ.SchedulerAppSchedulingOpportunitiesDTO> queryResults
            = query.
            getResultList();
    Map<String, List<FiCaSchedulerAppSchedulingOpportunities>> result = 
            createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  private FiCaSchedulerAppSchedulingOpportunities createSchedulerAppSchedulingOpportunities(
          SchedulerAppSchedulingOpportunitiesDTO schedulerAppSchedulingOpportunitiesDTO) {
    return new FiCaSchedulerAppSchedulingOpportunities(
            schedulerAppSchedulingOpportunitiesDTO.getschedulerapp_id(),
            schedulerAppSchedulingOpportunitiesDTO.getpriorityid(),
            schedulerAppSchedulingOpportunitiesDTO.getcounter());
  }

  private SchedulerAppSchedulingOpportunitiesDTO createPersistable(
          FiCaSchedulerAppSchedulingOpportunities hop, HopsSession session)
          throws
          StorageException {
    FiCaSchedulerAppSchedulingOpportunitiesClusterJ.SchedulerAppSchedulingOpportunitiesDTO schedulerAppSchedulingOpportunitiesDTO
            = session.newInstance(
                    FiCaSchedulerAppSchedulingOpportunitiesClusterJ.SchedulerAppSchedulingOpportunitiesDTO.class);

    schedulerAppSchedulingOpportunitiesDTO.setschedulerapp_id(hop.
            getSchedulerapp_id());
    schedulerAppSchedulingOpportunitiesDTO.setpriorityid(hop.getPriority_id());
    schedulerAppSchedulingOpportunitiesDTO.setcounter(hop.getCounter());

    return schedulerAppSchedulingOpportunitiesDTO;
  }

  private List<FiCaSchedulerAppSchedulingOpportunities> createSchedulingOpportunitiesList(
          List<SchedulerAppSchedulingOpportunitiesDTO> results) {
    List<FiCaSchedulerAppSchedulingOpportunities> appSchedulingOpportunitieses
            = new ArrayList<FiCaSchedulerAppSchedulingOpportunities>();
    for (SchedulerAppSchedulingOpportunitiesDTO persistable : results) {
      appSchedulingOpportunitieses.add(
              createSchedulerAppSchedulingOpportunities(persistable));
    }
    return appSchedulingOpportunitieses;
  }

  private Map<String, List<FiCaSchedulerAppSchedulingOpportunities>> createMap(
          List<SchedulerAppSchedulingOpportunitiesDTO> results) {
    Map<String, List<FiCaSchedulerAppSchedulingOpportunities>> map
            = new HashMap<String, List<FiCaSchedulerAppSchedulingOpportunities>>();
    for (SchedulerAppSchedulingOpportunitiesDTO dto : results) {
      FiCaSchedulerAppSchedulingOpportunities hop
              = createSchedulerAppSchedulingOpportunities(dto);
      if (map.get(hop.getSchedulerapp_id()) == null) {
        map.put(hop.getSchedulerapp_id(),
                new ArrayList<FiCaSchedulerAppSchedulingOpportunities>());
      }
      map.get(hop.getSchedulerapp_id()).add(hop);
    }
    return map;
  }
}
