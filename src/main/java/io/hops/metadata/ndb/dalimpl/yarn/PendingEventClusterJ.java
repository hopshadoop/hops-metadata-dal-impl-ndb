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
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.entity.PendingEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Implements persistence of PersistedEvents to NDB to be retrieved
 * by the scheduler.
 * <p/>
 */
public class PendingEventClusterJ
    implements TablesDef.PendingEventTableDef, PendingEventDataAccess<PendingEvent> {

  private static final Log LOG = LogFactory.getLog(PendingEventClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface PendingEventDTO extends RMNodeComponentDTO  {

    @PrimaryKey
    @Column(name = ID)
    int getId();

    void setId(int id);

    @PrimaryKey
    @Column(name = RMNODEID)
    String getrmnodeid();

    void setrmnodeid(String rmnodeid);

    @Column(name = TYPE)
    int getType();

    void setType(int type);

    @Column(name = STATUS)
    int getStatus();

    void setStatus(int status);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void createPendingEvent(PendingEvent persistedEvent)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    PendingEventDTO dto = createPersistable(persistedEvent, session);
    session.makePersistent(dto);
    session.release(dto);
  }

  @Override
  public void removePendingEvent(PendingEvent persistedEvent)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    PendingEventDTO dto = createPersistable(persistedEvent, session);
    session.deletePersistent(dto);
    session.release(dto);
  }

  @Override
  public void addAll(Collection<PendingEvent> toAddPendingEvent)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<PendingEventClusterJ.PendingEventDTO> toPersist
            = new ArrayList<PendingEventClusterJ.PendingEventDTO>();
    for (PendingEvent pendEvent : toAddPendingEvent) {
      PendingEventClusterJ.PendingEventDTO pendingEventDTO = createPersistable(
              new PendingEvent(pendEvent.getRmnodeId(), pendEvent.
                      getType(), pendEvent.getStatus(), pendEvent.getId()),
              session);
      toPersist.add(pendingEventDTO);
    }
    session.savePersistentAll(toPersist);
    session.release(toPersist);
  }


  @Override
  public void removeAll(Collection<PendingEvent> toRemovePendingEvents)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<PendingEventClusterJ.PendingEventDTO> toRemove
            = new ArrayList<PendingEventClusterJ.PendingEventDTO>();
    for (PendingEvent pendEvent : toRemovePendingEvents) {
      toRemove.add(createPersistable(pendEvent, session));
    }
    session.deletePersistentAll(toRemove);
//    session.flush();
    session.release(toRemove);
  }

  @Override
  public List<PendingEvent> getAll() throws StorageException {
    LOG.debug("HOP :: ClusterJ PendingEvent.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<PendingEventDTO> dobj =
        qb.createQueryDefinition(PendingEventDTO.class);
    HopsQuery<PendingEventDTO> query = session.createQuery(dobj);

    List<PendingEventDTO> queryResults = query.getResultList();
    LOG.debug("HOP :: ClusterJ PendingEvent.getAll - FINISH");
    List<PendingEvent> result = createPendingEventList(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public List<PendingEvent> getAll(int status) throws StorageException {
    // LOG.debug("HOP :: ClusterJ PendingEvent.getAll(" + status + ") - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<PendingEventDTO> dobj =
        qb.createQueryDefinition(PendingEventDTO.class);
    HopsPredicate pred1 = dobj.get(STATUS).equal(dobj.param(STATUS));
    dobj.where(pred1);
    HopsQuery<PendingEventDTO> query = session.createQuery(dobj);
    query.setParameter(STATUS, status);
    List<PendingEventDTO> queryResults = query.getResultList();
    //LOG.debug("HOP :: ClusterJ PendingEvent.getAll(" + status + ") - FINISH");
    List<PendingEvent> result = createPendingEventList(queryResults);
    session.release(queryResults);
    return result;
  }

  
  public PendingEvent findEntry(int pendingEventId, String nodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    PendingEventDTO pendingEventDTO;
    Object[] pk = new Object[3];
    pk[0] = pendingEventId;
    pk[1] = nodeId;
    pendingEventDTO = session.find(PendingEventDTO.class, pk);
    PendingEvent result = null;
    if (pendingEventDTO != null) {
      result = new PendingEvent(pendingEventDTO.getrmnodeid(), pendingEventDTO.
            getType(), pendingEventDTO.getStatus(), pendingEventDTO.getId());
    }
    session.release(pendingEventDTO);
    return result;
  }
  /**
   * <p/>
   *
   * @param hopPersistedEvent
   * @param session
   * @return
   */
  private PendingEventDTO createPersistable(PendingEvent hopPersistedEvent,
      HopsSession session) throws StorageException {
    PendingEventDTO DTO = session.newInstance(PendingEventDTO.class);
    //Set values to persist new persistedEvent
    DTO.setrmnodeid(hopPersistedEvent.getRmnodeId());
    DTO.setType(hopPersistedEvent.getType());
    DTO.setStatus(hopPersistedEvent.getStatus());
    DTO.setId(hopPersistedEvent.getId());
    return DTO;
  }

  /**
   * Create a list with HOP objects from DTO.
   * <p/>
   *
   * @param results
   * @return
   */
  private List<PendingEvent> createPendingEventList(
      List<PendingEventDTO> results) {
    List<PendingEvent> hopList = null;
    if (results != null && !results.isEmpty()) {
      hopList = new ArrayList<PendingEvent>(results.size());
      for (PendingEventDTO DTO : results) {
        PendingEvent hop = new PendingEvent(DTO.getrmnodeid(), DTO.
            getType(), DTO.getStatus(), DTO.getId());
        hopList.add(hop);
      }
    }
    return hopList;
  }
}
