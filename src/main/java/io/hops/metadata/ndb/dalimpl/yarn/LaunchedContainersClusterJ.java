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
import io.hops.metadata.yarn.dal.LaunchedContainersDataAccess;
import io.hops.metadata.yarn.entity.LaunchedContainers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LaunchedContainersClusterJ implements
    TablesDef.LaunchedContainersTableDef,
    LaunchedContainersDataAccess<LaunchedContainers> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface LaunchedContainersDTO {

    @PrimaryKey
    @Column(name = SCHEDULERNODE_ID)
    String getschedulernode_id();

    void setschedulernode_id(String schedulernode_id);

    @PrimaryKey
    @Column(name = CONTAINERID_ID)
    String getcontaineridid();

    void setcontaineridid(String containeridid);

    @Column(name = RMCONTAINER_ID)
    String getrmcontainerid();

    void setrmcontainerid(String rmcontainerid);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Map<String, List<LaunchedContainers>> getAll()
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<LaunchedContainersDTO> dobj =
        qb.createQueryDefinition(LaunchedContainersDTO.class);
    HopsQuery<LaunchedContainersDTO> query = session.
        createQuery(dobj);
    List<LaunchedContainersDTO> queryResults = query.
        getResultList();
    Map<String, List<LaunchedContainers>> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }


  @Override
  public void addAll(Collection<LaunchedContainers> toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<LaunchedContainersDTO> toPersist =
        new ArrayList<LaunchedContainersDTO>();
    for (LaunchedContainers id : toAdd) {
      toPersist.add(createPersistable(id, session));
    }
    session.savePersistentAll(toPersist);
    session.release(toPersist);
  }

  @Override
  public void removeAll(Collection<LaunchedContainers> toRemove)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<LaunchedContainersDTO> toPersist =
        new ArrayList<LaunchedContainersDTO>();
    for (LaunchedContainers hopContainerId : toRemove) {
      LaunchedContainersDTO persistable = session.newInstance(LaunchedContainersDTO.class);
      persistable.setschedulernode_id(hopContainerId.getSchedulerNodeID());
      persistable.setcontaineridid(hopContainerId.getContainerIdID());
      toPersist.add(persistable);
    }
    session.deletePersistentAll(toPersist);
    session.release(toPersist);
  }

  private LaunchedContainers createLaunchedContainersEntry(
      LaunchedContainersDTO dto) {
    LaunchedContainers hop = new LaunchedContainers(dto.getschedulernode_id(),
        dto.getcontaineridid(), dto.getrmcontainerid());
    return hop;
  }

  private Map<String, List<LaunchedContainers>> createMap(
      List<LaunchedContainersDTO> dtos) {
    Map<String, List<LaunchedContainers>> map =
        new HashMap<String, List<LaunchedContainers>>();
    for (LaunchedContainersDTO dto : dtos) {
      LaunchedContainers hop = createLaunchedContainersEntry(dto);
      if (map.get(hop.getSchedulerNodeID()) == null) {
        map.
            put(hop.getSchedulerNodeID(), new ArrayList<LaunchedContainers>());
      }
      map.get(hop.getSchedulerNodeID()).add(hop);
    }
    return map;
  }

  private LaunchedContainersDTO createPersistable(LaunchedContainers entry,
      HopsSession session) throws StorageException {
    Object[] objarr = new Object[2];
    objarr[0] = entry.getSchedulerNodeID();
    objarr[1] = entry.getContainerIdID();
    LaunchedContainersDTO persistable =
        session.newInstance(LaunchedContainersDTO.class, objarr);
    persistable.setschedulernode_id(entry.getSchedulerNodeID());
    persistable.setcontaineridid(entry.getContainerIdID());
    persistable.setrmcontainerid(entry.getRmContainerID());
    return persistable;
  }

  private List<LaunchedContainers> createLaunchedContainersList(
      List<LaunchedContainersDTO> results) {
    List<LaunchedContainers> launchedContainers =
        new ArrayList<LaunchedContainers>();
    for (LaunchedContainersDTO persistable : results) {
      launchedContainers.add(createLaunchedContainersEntry(persistable));
    }
    return launchedContainers;
  }
}
