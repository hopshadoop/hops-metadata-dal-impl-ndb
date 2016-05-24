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
package io.hops.metadata.ndb.dalimpl.yarn.fair;

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
import io.hops.metadata.yarn.dal.fair.LocalityLevelDataAccess;
import io.hops.metadata.yarn.entity.fair.LocalityLevel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class LocalityLevelClusterJ implements TablesDef.LocalityLevelTableDef,
        LocalityLevelDataAccess<LocalityLevel> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface LocalityLevelDTO {

    @PrimaryKey
    @Column(name = SCHEDULERAPP_ID)
    String getschedulerapp_id();

    void setschedulerapp_id(String schedulerapp_id);

    @Column(name = PRIORITY_ID)
    int getpriority_id();

    void setpriority_id(int priority_id);

    @Column(name = NODETYPE)
    String getnodetype();

    void setnodetype(String nodetype);

  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<LocalityLevel> findById(String appAttemptId) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<LocalityLevelClusterJ.LocalityLevelDTO> dobj = qb.
            createQueryDefinition(LocalityLevelClusterJ.LocalityLevelDTO.class);
    HopsPredicate pred1 = dobj.get("schedulerapp_id").equal(dobj.param(
            "schedulerapp_id"));
    dobj.where(pred1);
    HopsQuery<LocalityLevelClusterJ.LocalityLevelDTO> query = session.
            createQuery(dobj);
    query.setParameter("schedulerapp_id", appAttemptId);

    List<LocalityLevelClusterJ.LocalityLevelDTO> results = query.
            getResultList();
    return createLocalityLevelList(results);
  }

  @Override
  public void addAll(Collection<LocalityLevel> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    if (modified != null) {
      List<LocalityLevelClusterJ.LocalityLevelDTO> toModify
              = new ArrayList<LocalityLevelClusterJ.LocalityLevelDTO>();
      for (LocalityLevel hop : modified) {
        LocalityLevelClusterJ.LocalityLevelDTO persistable
                = createPersistable(hop, session);
        toModify.add(persistable);
      }
      session.savePersistentAll(toModify);
    }
  }

  @Override
  public void removeAll(Collection<LocalityLevel> removed) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    if (removed != null) {
      List<LocalityLevelClusterJ.LocalityLevelDTO> toRemove
              = new ArrayList<LocalityLevelClusterJ.LocalityLevelDTO>();
      for (LocalityLevel hop : removed) {
        Object[] objarr = new Object[2];
        objarr[0] = hop.getSchedulerappId();
        objarr[1] = hop.getPriorityId();
        toRemove.add(session.newInstance(
                LocalityLevelClusterJ.LocalityLevelDTO.class, objarr));
      }
      session.deletePersistentAll(toRemove);
    }
  }

  private LocalityLevel createLocalityLevel(LocalityLevelDTO localityLevelDTO) {
    return new LocalityLevel(localityLevelDTO.getschedulerapp_id(),
            localityLevelDTO.getpriority_id(), localityLevelDTO.getnodetype());
  }

  private LocalityLevelDTO createPersistable(LocalityLevel hop,
          HopsSession session) throws StorageException {
    LocalityLevelClusterJ.LocalityLevelDTO localityLevelDTO = session.
            newInstance(LocalityLevelClusterJ.LocalityLevelDTO.class);

    localityLevelDTO.setschedulerapp_id(hop.getSchedulerappId());
    localityLevelDTO.setpriority_id(hop.getPriorityId());
    localityLevelDTO.setnodetype(hop.getNodeType());

    return localityLevelDTO;
  }

  private List<LocalityLevel> createLocalityLevelList(
          List<LocalityLevelDTO> results) {
    List<LocalityLevel> localityLevels = new ArrayList<LocalityLevel>();
    for (LocalityLevelDTO persistable : results) {
      localityLevels.add(createLocalityLevel(persistable));
    }
    return localityLevels;
  }
}
