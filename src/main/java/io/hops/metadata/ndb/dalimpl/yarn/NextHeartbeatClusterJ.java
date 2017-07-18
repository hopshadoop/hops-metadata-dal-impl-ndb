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
import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import java.util.ArrayList;
import java.util.Collection;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NextHeartbeatClusterJ
    implements TablesDef.NextHeartbeatTableDef, NextHeartbeatDataAccess<NextHeartbeat> {

  private static final Log LOG = LogFactory.getLog(NextHeartbeatClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface NextHeartbeatDTO extends RMNodeComponentDTO {

    @PrimaryKey
    @Column(name = RMNODEID)
    String getrmnodeid();

    void setrmnodeid(String rmnodeid);

    @Column(name = NEXTHEARTBEAT)
    int getNextheartbeat();

    void setNextheartbeat(int Nextheartbeat);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Map<String, Boolean> getAll() throws StorageException {

    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<NextHeartbeatDTO> dobj =
        qb.createQueryDefinition(NextHeartbeatDTO.class);
    HopsQuery<NextHeartbeatDTO> query = session.createQuery(dobj);
    List<NextHeartbeatDTO> queryResults = query.getResultList();

    Map<String, Boolean> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public boolean findEntry(String rmnodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    NextHeartbeatDTO nextHBDTO = session.find(NextHeartbeatDTO.class, rmnodeId);
    boolean result = false;
    if (nextHBDTO != null) {
      result = createHopNextHeartbeat(nextHBDTO).isNextheartbeat();
    }
    session.release(nextHBDTO);
    return result;
  }

  @Override
  public void updateAll(Collection<NextHeartbeat> toUpdate)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<NextHeartbeatDTO> toPersist = new ArrayList<>();
    List<NextHeartbeatDTO> toRemove = new ArrayList<>();

    for (NextHeartbeat hb : toUpdate) {
      NextHeartbeatDTO hbDTO = createPersistable(hb,
              session);

      if (hb.isNextheartbeat()) {
        toPersist.add(hbDTO);
      } else {
        toRemove.add(hbDTO);
      }
    }
    session.savePersistentAll(toPersist);
    session.deletePersistentAll(toRemove);
    session.release(toPersist);
    session.release(toRemove);
  }

  @Override
  public void update(NextHeartbeat toUpdate)
          throws StorageException {
    HopsSession session = connector.obtainSession();

    NextHeartbeatDTO hbDTO = createPersistable(toUpdate,
            session);

    if (toUpdate.isNextheartbeat()) {
      session.savePersistent(hbDTO);
    } else {
      session.deletePersistent(hbDTO);
    }
    session.release(hbDTO);
  }
  
  private NextHeartbeatDTO createPersistable(NextHeartbeat hopNextHeartbeat,
          HopsSession session) throws StorageException {
    NextHeartbeatDTO DTO = session.newInstance(NextHeartbeatDTO.class);
    //Set values to persist new persistedEvent
    DTO.setrmnodeid(hopNextHeartbeat.getRmnodeid());
    DTO.setNextheartbeat(NextHeartbeat.booleanToInt(hopNextHeartbeat.isNextheartbeat()));
    return DTO;
  }

  public static NextHeartbeat createHopNextHeartbeat(
          NextHeartbeatDTO nextHBDTO) {
    return new NextHeartbeat(nextHBDTO.getrmnodeid(), NextHeartbeat.intToBoolean(nextHBDTO.
            getNextheartbeat()));
  }

  private Map<String, Boolean> createMap(List<NextHeartbeatDTO> results) {
    Map<String, Boolean> map = new HashMap<>();
    for (NextHeartbeatDTO persistable : results) {
      map.put(persistable.getrmnodeid(), NextHeartbeat.intToBoolean(persistable.
              getNextheartbeat()));
    }
    return map;
  }



}
