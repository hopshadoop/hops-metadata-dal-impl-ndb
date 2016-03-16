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
import io.hops.metadata.yarn.dal.JustFinishedContainersDataAccess;
import io.hops.metadata.yarn.entity.JustFinishedContainer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JustFinishedContainersClusterJ implements
        TablesDef.JustFinishedContainersTableDef,
        JustFinishedContainersDataAccess<JustFinishedContainer> {

  private static final Log LOG = LogFactory.getLog(
          JustFinishedContainersClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface JustFinishedContainersDTO {

    @PrimaryKey
    @Column(name = CONTAINERID)
    String getcontainerid();

    void setcontainerid(String containerid);

    @PrimaryKey
    @Column(name = APPATTEMPTID)
    String getAppAttemptId();

    void setAppAttemptId(String rmnodeid);

    @Column(name = CONTAINER)
    byte[] getContainer();

    void setContainer(byte[] container);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void addAll(Collection<JustFinishedContainer> containers)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<JustFinishedContainersDTO> toAdd
            = new ArrayList<JustFinishedContainersDTO>(containers.size());
    for (JustFinishedContainer hop : containers) {
      toAdd.add(createPersistable(hop, session));
    }
    session.savePersistentAll(toAdd);
    session.release(toAdd);
  }

  @Override
  public void removeAll(Collection<JustFinishedContainer> containers)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<JustFinishedContainersDTO> toRemove
            = new ArrayList<JustFinishedContainersDTO>(containers.size());
    for (JustFinishedContainer hopContainer : containers) {
      Object[] objarr = new Object[2];
      objarr[0] = hopContainer.getContainerId();
      objarr[1] = hopContainer.getAppAttemptId();
      toRemove
              .add(session.newInstance(JustFinishedContainersDTO.class, objarr));
    }
    session.deletePersistentAll(toRemove);
    session.release(toRemove);
  }

  @Override
  public Map<String, List<JustFinishedContainer>> getAll()
          throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<JustFinishedContainersDTO> dobj = qb.
            createQueryDefinition(JustFinishedContainersDTO.class);
    HopsQuery<JustFinishedContainersDTO> query = session.createQuery(dobj);
    List<JustFinishedContainersDTO> QueryResults = query.getResultList();
    Map<String, List<JustFinishedContainer>> result = null;
    if (QueryResults != null && !QueryResults.isEmpty()) {
      result = createMap(QueryResults);
    }
    session.release(QueryResults);
    return result;
  }

  private Map<String, List<JustFinishedContainer>> createMap(
          List<JustFinishedContainersDTO> results) {
    Map<String, List<JustFinishedContainer>> map
            = new HashMap<String, List<JustFinishedContainer>>();
    for (JustFinishedContainersDTO persistable : results) {
      JustFinishedContainer hop = createJustFinishedContainer(persistable);
      if (map.get(hop.getAppAttemptId()) == null) {
        map.put(hop.getAppAttemptId(), new ArrayList<JustFinishedContainer>());
      }
      map.get(hop.getAppAttemptId()).add(hop);
    }
    return map;
  }

  private JustFinishedContainer createJustFinishedContainer(
          JustFinishedContainersDTO dto) {
    JustFinishedContainer hop = new JustFinishedContainer(dto.getcontainerid(),
            dto.
            getAppAttemptId(), dto.getContainer());
    return hop;
  }

  private JustFinishedContainersDTO createPersistable(
          JustFinishedContainer entry, HopsSession session)
          throws StorageException {
    JustFinishedContainersDTO dto = session.newInstance(
            JustFinishedContainersDTO.class);
    dto.setcontainerid(entry.getContainerId());
    dto.setAppAttemptId(entry.getAppAttemptId());
    dto.setContainer(entry.getContainer());
    return dto;
  }
}
