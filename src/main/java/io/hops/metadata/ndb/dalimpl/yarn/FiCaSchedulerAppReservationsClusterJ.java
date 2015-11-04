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
import io.hops.metadata.yarn.dal.FiCaSchedulerAppReservationsDataAccess;
import io.hops.metadata.yarn.entity.SchedulerAppReservations;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FiCaSchedulerAppReservationsClusterJ implements
        TablesDef.FiCaSchedulerAppReservationsTableDef,
        FiCaSchedulerAppReservationsDataAccess<SchedulerAppReservations> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface SchedulerAppReservationsDTO {

    @PrimaryKey
    @Column(name = SCHEDULERAPP_ID)
    String getschedulerapp_id();

    void setschedulerapp_id(String schedulerapp_id);

    @Column(name = PRIORITY_ID)
    int getpriority_id();

    void setpriority_id(int priority_id);

    @Column(name = COUNTER)
    int getcounter();

    void setcounter(int counter);
  }
  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<SchedulerAppReservations> findById(String appAttemptId) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<FiCaSchedulerAppReservationsClusterJ.SchedulerAppReservationsDTO> dobj
            = qb.createQueryDefinition(
                    FiCaSchedulerAppReservationsClusterJ.SchedulerAppReservationsDTO.class);
    HopsPredicate pred1 = dobj.get("schedulerapp_id").equal(dobj.param(
            "schedulerapp_id"));
    dobj.where(pred1);
    HopsQuery<FiCaSchedulerAppReservationsClusterJ.SchedulerAppReservationsDTO> query
            = session.createQuery(dobj);
    query.setParameter("schedulerapp_id", appAttemptId);

    List<FiCaSchedulerAppReservationsClusterJ.SchedulerAppReservationsDTO> queryResults
            = query.getResultList();
    List<SchedulerAppReservations> result = createReservationsList(queryResults);
    session.release(queryResults);
    return result;
  }

  public static int add=0;
  @Override
  public void addAll(Collection<SchedulerAppReservations> modified) throws
          StorageException {
    HopsSession session = connector.obtainSession();
      if (modified != null) {
        List<FiCaSchedulerAppReservationsClusterJ.SchedulerAppReservationsDTO> toAdd
                = new ArrayList<SchedulerAppReservationsDTO>();
        for (SchedulerAppReservations hop : modified) {
          FiCaSchedulerAppReservationsClusterJ.SchedulerAppReservationsDTO persistable
                  = createPersistable(hop, session);
          toAdd.add(persistable);
        }
        add+=toAdd.size();
        session.savePersistentAll(toAdd);
        session.release(toAdd);
      }
  }

  public static int remove=0;
  @Override
  public void removeAll(Collection<SchedulerAppReservations> removed) throws
          StorageException {
    HopsSession session = connector.obtainSession();
      if (removed != null) {
        List<FiCaSchedulerAppReservationsClusterJ.SchedulerAppReservationsDTO> toRemove
                = new ArrayList<FiCaSchedulerAppReservationsClusterJ.SchedulerAppReservationsDTO>();
        for (SchedulerAppReservations hop : removed) {
          Object[] objarr = new Object[2];
          objarr[0] = hop.getSchedulerapp_id();
          objarr[1] = hop.getPriority_id();
          toRemove.add(session.newInstance(
                  FiCaSchedulerAppReservationsClusterJ.SchedulerAppReservationsDTO.class,
                  objarr));
        }
        remove+=toRemove.size();
        session.deletePersistentAll(toRemove);
        session.release(toRemove);
      }

  }

  @Override
  public Map<String, List<SchedulerAppReservations>> getAll() throws
          StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<FiCaSchedulerAppReservationsClusterJ.SchedulerAppReservationsDTO> dobj
            = qb.createQueryDefinition(
                    FiCaSchedulerAppReservationsClusterJ.SchedulerAppReservationsDTO.class);
    HopsQuery<FiCaSchedulerAppReservationsClusterJ.SchedulerAppReservationsDTO> query
            = session.
            createQuery(dobj);
    List<FiCaSchedulerAppReservationsClusterJ.SchedulerAppReservationsDTO> queryResults
            = query.
            getResultList();
    Map<String, List<SchedulerAppReservations>> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  private SchedulerAppReservations createHopFiCaSchedulerAppReservations(
          SchedulerAppReservationsDTO fiCaSchedulerAppReservationsDTO) {
    return new SchedulerAppReservations(fiCaSchedulerAppReservationsDTO.
            getschedulerapp_id(),
            fiCaSchedulerAppReservationsDTO.getpriority_id(),
            fiCaSchedulerAppReservationsDTO.getcounter());
  }

  private SchedulerAppReservationsDTO createPersistable(
          SchedulerAppReservations hop, HopsSession session) throws
          StorageException {
    FiCaSchedulerAppReservationsClusterJ.SchedulerAppReservationsDTO fiCaSchedulerAppReservationsDTO
            = session.newInstance(
                    FiCaSchedulerAppReservationsClusterJ.SchedulerAppReservationsDTO.class);

    fiCaSchedulerAppReservationsDTO.setschedulerapp_id(hop.getSchedulerapp_id());
    fiCaSchedulerAppReservationsDTO.setpriority_id(hop.getPriority_id());
    fiCaSchedulerAppReservationsDTO.setcounter(hop.getCounter());

    return fiCaSchedulerAppReservationsDTO;
  }

  private List<SchedulerAppReservations> createReservationsList(
          List<SchedulerAppReservationsDTO> results) {
    List<SchedulerAppReservations> appReservationses
            = new ArrayList<SchedulerAppReservations>();
    for (SchedulerAppReservationsDTO persistable : results) {
      appReservationses.add(createHopFiCaSchedulerAppReservations(persistable));
    }
    return appReservationses;
  }

  private Map<String, List<SchedulerAppReservations>> createMap(
          List<SchedulerAppReservationsDTO> results) {
    Map<String, List<SchedulerAppReservations>> map
            = new HashMap<String, List<SchedulerAppReservations>>();
    for (SchedulerAppReservationsDTO dto : results) {
      SchedulerAppReservations hop
              = createHopFiCaSchedulerAppReservations(dto);
      if (map.get(hop.getSchedulerapp_id()) == null) {
        map.put(hop.getSchedulerapp_id(),
                new ArrayList<SchedulerAppReservations>());
      }
      map.get(hop.getSchedulerapp_id()).add(hop);
    }
    return map;
  }
}
