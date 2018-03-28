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
package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.*;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.RetryCacheEntryDataAccess;
import io.hops.metadata.hdfs.entity.RetryCacheEntry;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.*;

import java.util.*;

public class RetryCacheEntryClusterj
    implements TablesDef.RetryCacheEntryTableDef, RetryCacheEntryDataAccess {

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface RetryCacheEntryDTO {
    
    @PrimaryKey
    @Column(name = CLIENTID)
    byte[] getClientId();

    void setClientId(byte[] clientId);
    
    @PrimaryKey
    @Column(name = CALLID)
    int getCallId();

    void setCallId(int callId);
    
    @Column(name = PAYLOAD)
    byte[] getPayload();

    void setPayload(byte[] payload);
    
    @Column(name = EXPIRATION_TIME)
    long getExpirationTime();

    void setExpirationTime(long expirationTime);
    
    @Column(name = STATE)
    byte getState();

    void setState(byte state);
  }

  @Override
  public RetryCacheEntry find(RetryCacheEntry.PrimaryKey key) throws
      StorageException {
    HopsSession session = connector.obtainSession();
    Object[] pk = new Object[2];
    pk[0] = key.getClientId();
    pk[1] = key.getCallId();

    RetryCacheEntryDTO result = session.find(RetryCacheEntryDTO.class, pk);
    if (result != null) {
      RetryCacheEntry retryCacheEntry = convert(result);
      session.release(result);
      return retryCacheEntry;
    } else {
      return null;
    }
  }
  
  @Override
  public void prepare(Collection<RetryCacheEntry> removed,
      Collection<RetryCacheEntry> modified) throws StorageException {
    
    List<RetryCacheEntryDTO> changes = new ArrayList<>();
    List<RetryCacheEntryDTO> deletions = new ArrayList<>();
    HopsSession session = connector.obtainSession();
    for (RetryCacheEntry retryCacheEntry : removed) {
      RetryCacheEntryDTO newInstance = session.newInstance(RetryCacheEntryDTO.class);
      createPersistable(retryCacheEntry, newInstance);
      deletions.add(newInstance);
    }

    for (RetryCacheEntry retryCacheEntry : modified) {
      RetryCacheEntryDTO newInstance = session.newInstance(RetryCacheEntryDTO.class);
      createPersistable(retryCacheEntry, newInstance);
      changes.add(newInstance);
    }
    session.deletePersistentAll(deletions);
    session.savePersistentAll(changes);

    session.release(deletions);
    session.release(changes);
  }
  
  public void removeOlds(long time) throws StorageException{
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<RetryCacheEntryDTO> dobj = qb.createQueryDefinition(RetryCacheEntryDTO.class);
    HopsPredicate pred1 = dobj.get("expirationTime").lessEqual(dobj.param("ExpirationTime"));
    dobj.where(pred1);
    HopsQuery<RetryCacheEntryDTO> query = session.createQuery(dobj);
    query.setParameter("ExpirationTime", time);
    query.deletePersistentAll();
  }

  private RetryCacheEntry convert(RetryCacheEntryDTO result) {
    return new RetryCacheEntry(result.getClientId(), result.getCallId(), result.getPayload(), result.getExpirationTime(), result.getState());
  }

  private void createPersistable(RetryCacheEntry retryCacheEntry, RetryCacheEntryDTO newInstance) {
    newInstance.setClientId(retryCacheEntry.getClientId());
    newInstance.setCallId(retryCacheEntry.getCallId());
    newInstance.setPayload(retryCacheEntry.getPayload());
    newInstance.setExpirationTime(retryCacheEntry.getExpirationTime());
    newInstance.setState(retryCacheEntry.getState());
  }
}
