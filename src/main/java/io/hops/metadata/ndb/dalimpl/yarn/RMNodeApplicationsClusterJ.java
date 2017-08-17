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
import io.hops.metadata.yarn.entity.RMNodeApplication;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import io.hops.metadata.yarn.dal.RMNodeApplicationsDataAccess;

public class RMNodeApplicationsClusterJ
    implements TablesDef.FinishedApplicationsTableDef,
    RMNodeApplicationsDataAccess<RMNodeApplication> {

  private static final Log LOG = LogFactory.getLog(RMNodeApplicationsClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface FinishedApplicationsDTO {

    @PrimaryKey
    @Column(name = RMNODEID)
    String getrmnodeid();

    void setrmnodeid(String rmnodeid);

    @PrimaryKey
    @Column(name = APPLICATIONID)
    String getapplicationid();

    void setapplicationid(String applicationid);

    @PrimaryKey
    @Column(name = STATUS)
    String getStatus();

    void setStatus(String status);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<RMNodeApplication> findByRMNode(String rmnodeid)
      throws StorageException {
    LOG.debug("HOP :: ClusterJ FinishedApplications.findByRMNode - START:" + rmnodeid);
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<FinishedApplicationsDTO> dobj = qb.
        createQueryDefinition(FinishedApplicationsDTO.class);
    HopsPredicate pred1 = dobj.get(RMNODEID).equal(dobj.param(RMNODEID));
    dobj.where(pred1);

    HopsQuery<FinishedApplicationsDTO> query = session.createQuery(dobj);
    query.setParameter(RMNODEID, rmnodeid);
    List<FinishedApplicationsDTO> results = query.getResultList();
    LOG.debug("HOP :: ClusterJ FinishedApplications.findByRMNode - FINISH:" + rmnodeid);
    if (results != null && !results.isEmpty()) {
      return createUpdatedContainerInfoList(results);
    }
    return null;

  }

  @Override
  public Map<String, List<RMNodeApplication>> getAll()
      throws StorageException {
    LOG.debug("HOP :: ClusterJ FinishedApplications.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<FinishedApplicationsDTO> dobj = qb.createQueryDefinition(FinishedApplicationsDTO.class);
    HopsQuery<FinishedApplicationsDTO> query = session.
        createQuery(dobj);
    List<FinishedApplicationsDTO> queryResults = query.
        getResultList();
    LOG.debug("HOP :: ClusterJ FinishedApplications.getAll - FINISH");
    Map<String, List<RMNodeApplication>> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public void addAll(Collection<RMNodeApplication> applications)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FinishedApplicationsDTO> toModify = new ArrayList<>();
    for (RMNodeApplication entry : applications) {
      toModify.add(createPersistable(entry, session));
    }
    session.savePersistentAll(toModify);
    session.release(toModify);
  }

  @Override
  public void add(RMNodeApplication application)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    FinishedApplicationsDTO toPersist = createPersistable(application, session);
    session.savePersistent(toPersist);

    session.release(toPersist);
  }

  @Override
  public void removeAll(Collection<RMNodeApplication> applications) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FinishedApplicationsDTO> toRemove = new ArrayList<>();
    for (RMNodeApplication entry : applications) {
      toRemove.add(createPersistable(entry, session));
    }
    session.deletePersistentAll(toRemove);
    session.release(toRemove);
  }

  @Override
  public void remove(RMNodeApplication application) throws StorageException {
    HopsSession session = connector.obtainSession();
    FinishedApplicationsDTO toRemove = createPersistable(application, session);
    session.deletePersistent(toRemove);
    session.release(toRemove);
  }

  private RMNodeApplication createHopFinishedApplications(
      FinishedApplicationsDTO dto) {
    return new RMNodeApplication(dto.getrmnodeid(), dto.getapplicationid(), RMNodeApplication.RMNodeApplicationStatus.
        valueOf(dto.getStatus()));
  }

  private FinishedApplicationsDTO createPersistable(RMNodeApplication hop,
      HopsSession session) throws StorageException {
    FinishedApplicationsDTO dto = session.newInstance(FinishedApplicationsDTO.class);
    dto.setrmnodeid(hop.getRMNodeID());
    dto.setapplicationid(hop.getApplicationId());
    dto.setStatus(hop.getStatus().toString());
    return dto;
  }

  private List<RMNodeApplication> createUpdatedContainerInfoList(
      List<FinishedApplicationsDTO> list) {
    List<RMNodeApplication> finishedApps = new ArrayList<>();
    for (FinishedApplicationsDTO persistable : list) {
      finishedApps.add(createHopFinishedApplications(persistable));
    }
    return finishedApps;
  }

  private Map<String, List<RMNodeApplication>> createMap(
      List<FinishedApplicationsDTO> results) {
    Map<String, List<RMNodeApplication>> map = new HashMap<>();
    for (FinishedApplicationsDTO dto : results) {
      RMNodeApplication hop = createHopFinishedApplications(dto);
      if (map.get(hop.getRMNodeID()) == null) {
        map.put(hop.getRMNodeID(), new ArrayList<RMNodeApplication>());
      }
      map.get(hop.getRMNodeID()).add(hop);
    }
    return map;
  }
}
