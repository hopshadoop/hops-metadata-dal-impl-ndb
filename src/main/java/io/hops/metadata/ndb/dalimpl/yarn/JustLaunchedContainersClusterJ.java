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
import io.hops.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import io.hops.metadata.yarn.entity.JustLaunchedContainers;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JustLaunchedContainersClusterJ
    implements TablesDef.JustLaunchedContainersTableDef,
    JustLaunchedContainersDataAccess<JustLaunchedContainers> {

  private static final Log LOG =
      LogFactory.getLog(JustLaunchedContainersClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface JustLaunchedContainersDTO {

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
  public void addAll(Collection<JustLaunchedContainers> containers)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<JustLaunchedContainersDTO> toModify =
        new ArrayList<JustLaunchedContainersDTO>(containers.size());
    for (JustLaunchedContainers hop : containers) {
      toModify.add(createPersistable(hop, session));
    }
    session.savePersistentAll(toModify);
    session.flush();
    session.release(toModify);
  }

  @Override
  public void removeAll(Collection<JustLaunchedContainers> containers)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<JustLaunchedContainersDTO> toRemove =
        new ArrayList<JustLaunchedContainersDTO>(containers.size());
    for (JustLaunchedContainers hopContainerId : containers) {
      Object[] objarr = new Object[2];
      objarr[0] = hopContainerId.getRmnodeid();
      objarr[1] = hopContainerId.getContainerId();
      toRemove
          .add(session.newInstance(JustLaunchedContainersDTO.class, objarr));
    }
    session.deletePersistentAll(toRemove);
//    session.flush();
    session.release(toRemove);
  }

  @Override
  public List<JustLaunchedContainers> findByRMNode(String rmnodeId)
      throws StorageException {
    LOG.debug("HOP :: ClusterJ JustLaunchedContainers.findByRMNode - START:" +
        rmnodeId);
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<JustLaunchedContainersDTO> dobj = qb.
        createQueryDefinition(JustLaunchedContainersDTO.class);
    HopsPredicate pred = dobj.get(RMNODEID).equal(dobj.param(RMNODEID));
    dobj.where(pred);
    HopsQuery<JustLaunchedContainersDTO> query = session.createQuery(dobj);
    query.setParameter(RMNODEID, rmnodeId);
    List<JustLaunchedContainersDTO> queryResults = query.getResultList();
    LOG.debug("HOP :: ClusterJ JustLaunchedContainers.findByRMNode - FINISH:" +
        rmnodeId);
    List<JustLaunchedContainers> result = null;
    if (queryResults != null && !queryResults.isEmpty()) {
      result = createJustLaunchedContainersList(queryResults);
    }
    session.release(queryResults);
    return result;
  }

  @Override
  public Map<String, List<JustLaunchedContainers>> getAll()
      throws StorageException {
    LOG.debug("HOP :: ClusterJ JustLaunchedContainers.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<JustLaunchedContainersDTO> dobj = qb.
        createQueryDefinition(JustLaunchedContainersDTO.class);
    HopsQuery<JustLaunchedContainersDTO> query = session.createQuery(dobj);
    List<JustLaunchedContainersDTO> QueryResults = query.getResultList();
    LOG.debug("HOP :: ClusterJ JustLaunchedContainers.getAll - FINISH");
    Map<String, List<JustLaunchedContainers>> result = null;
    if (QueryResults != null && !QueryResults.isEmpty()) {
      result = createMap(QueryResults);
    } 
    session.release(QueryResults);
    return result;
  }

  private JustLaunchedContainers createJustLaunchedContainers(
      JustLaunchedContainersDTO dto) {
    JustLaunchedContainers hop = new JustLaunchedContainers(dto.
        getrmnodeid(), dto.getcontainerid());
    return hop;
  }

  /**
   * Persist new map entry.
   *
   * @param entry
   * @param session
   * @return
   */
  private JustLaunchedContainersDTO createPersistable(
      JustLaunchedContainers entry, HopsSession session)
      throws StorageException {
    JustLaunchedContainersDTO dto =
        session.newInstance(JustLaunchedContainersDTO.class);
    dto.setcontainerid(entry.getContainerId());
    dto.setrmnodeid(entry.getRmnodeid());
    return dto;
  }

  private List<JustLaunchedContainers> createJustLaunchedContainersList(
      List<JustLaunchedContainersDTO> results) {
    List<JustLaunchedContainers> justLaunchedContainers =
        new ArrayList<JustLaunchedContainers>();
    for (JustLaunchedContainersDTO persistable : results) {
      justLaunchedContainers.add(createJustLaunchedContainers(persistable));
    }
    return justLaunchedContainers;
  }

  private Map<String, List<JustLaunchedContainers>> createMap(
      List<JustLaunchedContainersDTO> results) {
    Map<String, List<JustLaunchedContainers>> map =
        new HashMap<String, List<JustLaunchedContainers>>();
    for (JustLaunchedContainersDTO persistable : results) {
      JustLaunchedContainers hop = createJustLaunchedContainers(persistable);
      if (map.get(hop.getRmnodeid()) == null) {
        map.put(hop.getRmnodeid(), new ArrayList<JustLaunchedContainers>());
      }
      map.get(hop.getRmnodeid()).add(hop);
    }
    return map;
  }
}
