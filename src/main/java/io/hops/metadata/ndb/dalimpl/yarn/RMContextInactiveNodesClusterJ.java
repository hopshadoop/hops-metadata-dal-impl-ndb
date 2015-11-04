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
import io.hops.metadata.yarn.dal.RMContextInactiveNodesDataAccess;
import io.hops.metadata.yarn.entity.RMContextInactiveNodes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RMContextInactiveNodesClusterJ
    implements TablesDef.RMContextInactiveNodesTableDef,
    RMContextInactiveNodesDataAccess<RMContextInactiveNodes> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface RMContextInactiveNodesDTO {

    @PrimaryKey
    @Column(name = RMNODEID)
    String getrmnodeid();

    void setrmnodeid(String rmnodeid);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<RMContextInactiveNodes> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<RMContextInactiveNodesDTO> dobj = qb.
        createQueryDefinition(RMContextInactiveNodesDTO.class);
    HopsQuery<RMContextInactiveNodesDTO> query = session.createQuery(dobj);

    List<RMContextInactiveNodesDTO> queryResults = query.getResultList();
    List<RMContextInactiveNodes> result = 
            createRMContextInactiveNodesList(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public void addAll(Collection<RMContextInactiveNodes> toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<RMContextInactiveNodesDTO> toPersist =
        new ArrayList<RMContextInactiveNodesDTO>();
    for (RMContextInactiveNodes req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }
    session.savePersistentAll(toPersist);
    session.release(toPersist);
  }

  @Override
  public void removeAll(Collection<RMContextInactiveNodes> toRemove)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<RMContextInactiveNodesDTO> toPersist =
        new ArrayList<RMContextInactiveNodesDTO>();
    for (RMContextInactiveNodes entry : toRemove) {
      toPersist.add(session.newInstance(RMContextInactiveNodesDTO.class, entry.
          getRmnodeid()));
    }
    session.deletePersistentAll(toPersist);
    session.release(toPersist);
  }

  private RMContextInactiveNodes createRMContextInactiveNodesEntry(
      RMContextInactiveNodesDTO entry) {
    return new RMContextInactiveNodes(entry.getrmnodeid());
  }

  private RMContextInactiveNodesDTO createPersistable(
      RMContextInactiveNodes entry, HopsSession session)
      throws StorageException {
    RMContextInactiveNodesDTO persistable =
        session.newInstance(RMContextInactiveNodesDTO.class);
    persistable.setrmnodeid(entry.getRmnodeid());
    return persistable;
  }

  private List<RMContextInactiveNodes> createRMContextInactiveNodesList(
      List<RMContextInactiveNodesDTO> results) {
    List<RMContextInactiveNodes> rmcontextInactiveNodes =
        new ArrayList<RMContextInactiveNodes>();
    for (RMContextInactiveNodesDTO persistable : results) {
      rmcontextInactiveNodes
          .add(createRMContextInactiveNodesEntry(persistable));
    }
    return rmcontextInactiveNodes;
  }
}
