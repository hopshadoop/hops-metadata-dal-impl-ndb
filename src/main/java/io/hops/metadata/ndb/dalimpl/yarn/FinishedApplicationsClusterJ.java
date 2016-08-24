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
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.entity.FinishedApplications;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FinishedApplicationsClusterJ
    implements TablesDef.FinishedApplicationsTableDef,
    FinishedApplicationsDataAccess<FinishedApplications> {

  private static final Log LOG =
      LogFactory.getLog(FinishedApplicationsClusterJ.class);

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
    
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<FinishedApplications> findByRMNode(String rmnodeid)
      throws StorageException {
    LOG.debug("HOP :: ClusterJ FinishedApplications.findByRMNode - START:" +
        rmnodeid);
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<FinishedApplicationsDTO> dobj = qb.
        createQueryDefinition(FinishedApplicationsDTO.class);
    HopsPredicate pred1 = dobj.get(RMNODEID).equal(dobj.param(RMNODEID));
    dobj.where(pred1);

    HopsQuery<FinishedApplicationsDTO> query = session.createQuery(dobj);
    query.setParameter(RMNODEID, rmnodeid);
    List<FinishedApplicationsDTO> results = query.getResultList();
    LOG.debug("HOP :: ClusterJ FinishedApplications.findByRMNode - FINISH:" +
        rmnodeid);
    if (results != null && !results.isEmpty()) {
      return createUpdatedContainerInfoList(results);
    }
    return null;

  }

  @Override
  public Map<String, List<FinishedApplications>> getAll()
      throws StorageException {
    LOG.debug("HOP :: ClusterJ FinishedApplications.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<FinishedApplicationsDTO> dobj =
        qb.createQueryDefinition(FinishedApplicationsDTO.class);
    HopsQuery<FinishedApplicationsDTO> query = session.
        createQuery(dobj);
    List<FinishedApplicationsDTO> queryResults = query.
        getResultList();
    LOG.debug("HOP :: ClusterJ FinishedApplications.getAll - FINISH");
    Map<String, List<FinishedApplications>> result = createMap(queryResults);
    session.release(queryResults);
      return result;
  }

  @Override
  public void addAll(Collection<FinishedApplications> applications)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FinishedApplicationsDTO> toModify =
        new ArrayList<FinishedApplicationsDTO>();
    for (FinishedApplications entry : applications) {
      toModify.add(createPersistable(entry, session));
    }
    session.savePersistentAll(toModify);
    session.flush();
    session.release(toModify);
  }

  @Override
  public void add(FinishedApplications application)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    FinishedApplicationsDTO toPersist = createPersistable(application, session);
    session.savePersistent(toPersist);
    
    session.release(toPersist);
  }
  
  @Override
  public void removeAll(Collection<FinishedApplications> applications)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<FinishedApplicationsDTO> toRemove =
        new ArrayList<FinishedApplicationsDTO>();
    for (FinishedApplications entry : applications) {
      toRemove.add(createPersistable(entry, session));
    }
    session.deletePersistentAll(toRemove);
    session.flush();
    session.release(toRemove);
  }

  private FinishedApplications createHopFinishedApplications(
          FinishedApplicationsDTO dto) {
    return new FinishedApplications(dto.getrmnodeid(), dto.getapplicationid());
  }

  private FinishedApplicationsDTO createPersistable(FinishedApplications hop,
      HopsSession session) throws StorageException {
    FinishedApplicationsDTO dto =
        session.newInstance(FinishedApplicationsDTO.class);
    dto.setrmnodeid(hop.getRMNodeID());
    dto.setapplicationid(hop.getApplicationId());
    return dto;
  }

  private List<FinishedApplications> createUpdatedContainerInfoList(
      List<FinishedApplicationsDTO> list) {
    List<FinishedApplications> finishedApps =
        new ArrayList<FinishedApplications>();
    for (FinishedApplicationsDTO persistable : list) {
      finishedApps.add(createHopFinishedApplications(persistable));
    }
    return finishedApps;
  }

  private Map<String, List<FinishedApplications>> createMap(
      List<FinishedApplicationsDTO> results) {
    Map<String, List<FinishedApplications>> map =
        new HashMap<String, List<FinishedApplications>>();
    for (FinishedApplicationsDTO dto : results) {
      FinishedApplications hop = createHopFinishedApplications(dto);
      if (map.get(hop.getRMNodeID()) == null) {
        map.put(hop.getRMNodeID(), new ArrayList<FinishedApplications>());
      }
      map.get(hop.getRMNodeID()).add(hop);
    }
    return map;
  }
}
