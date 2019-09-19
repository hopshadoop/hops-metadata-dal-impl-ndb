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
import io.hops.metadata.yarn.dal.RMLoadDataAccess;
import io.hops.metadata.yarn.entity.Load;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RMLoadClusterJ implements TablesDef.RMLoadTableDef, RMLoadDataAccess<Load> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface RMLoadDTO {

    @PrimaryKey
    @Column(name = RMHOSTNAME)
    String getrmhostname();

    void setrmhostname(String rmhostname);

    @Column(name = LOAD)
    long getload();

    void setload(long load);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void update(Load entry) throws StorageException {
    HopsSession session = connector.obtainSession();
    RMLoadDTO dto = createPersistable(entry, session);
    session.savePersistent(dto);
    session.release(dto);
  }

  @Override
  public Map<String, Load> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<RMLoadDTO> dobj =
        qb.createQueryDefinition(RMLoadDTO.class);
    HopsQuery<RMLoadDTO> query = session.
        createQuery(dobj);
    List<RMLoadDTO> queryResults = query.
        getResultList();
    Map<String, Load> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  private RMLoadDTO createPersistable(Load entry, HopsSession session)
      throws StorageException {
    RMLoadDTO persistable = session.newInstance(RMLoadDTO.class);
    persistable.setrmhostname(entry.getRmHostName());
    persistable.setload(entry.getLoad());
    return persistable;
  }

  private Map<String, Load> createMap(List<RMLoadDTO> results) {
    Map<String, Load> map = new HashMap<>();
    for (RMLoadDTO dto : results) {
      if(dto==null){
        continue;
      }
      Load hop = createHopLoad(dto);
      map.put(hop.getRmHostName(), hop);
    }
    return map;
  }

  private Load createHopLoad(RMLoadDTO loadDTO) {
    return new Load(loadDTO.getrmhostname(), loadDTO.getload());
  }
}
