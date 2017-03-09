/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.AccessTimeLogDataAccess;
import io.hops.metadata.hdfs.entity.AccessTimeLogEntry;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.dalimpl.ClusterjDataAccess;
import io.hops.metadata.ndb.wrapper.*;

import java.util.ArrayList;
import java.util.Collection;

public class AccessTimeLogClusterj extends ClusterjDataAccess implements
    TablesDef.AccessTimeLogTableDef, AccessTimeLogDataAccess<AccessTimeLogEntry> {

  public AccessTimeLogClusterj(ClusterjConnector connector) {
    super(connector);
  }

  @PersistenceCapable(table = TABLE_NAME)
  public interface AccessTimeLogEntryDto {
    @PrimaryKey
    @Column(name = INODE_ID)
    int getInodeId();

    void setInodeId(int inodeId);

    @PrimaryKey
    @Column(name = USER_ID)
    int getUserId();

    void setUserId(int userId);

    @PrimaryKey
    @Column(name = ACCESS_TIME)
    long getAccessTime();

    void setAccessTime(long accessTime);
  }

  @Override
  public void add(AccessTimeLogEntry logEntry) throws StorageException {
    HopsSession session = getConnector().obtainSession();
    AccessTimeLogEntryDto dto =
        session.newInstance(AccessTimeLogEntryDto.class);
    dto.setInodeId(logEntry.getInodeId());
    dto.setUserId(logEntry.getUserId());
    dto.setAccessTime(logEntry.getAccessTime());
    session.savePersistent(dto);
    session.release(dto);
  }

  @Override
  public Collection<AccessTimeLogEntry> find(int fileId) throws StorageException {
    HopsSession session = getConnector().obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<AccessTimeLogEntryDto> dobj =
        qb.createQueryDefinition(AccessTimeLogEntryDto.class);
    HopsPredicate pred1 = dobj.get("inodeId").equal(dobj.param("inodeIdParam"));
    dobj.where(pred1);
    HopsQuery<AccessTimeLogEntryDto> query = session.createQuery(dobj);
    query.setParameter("inodeIdParam", fileId);

    Collection<AccessTimeLogEntryDto> dtos = query.getResultList();
    Collection<AccessTimeLogEntry> ivl = createCollection(dtos);
    session.release(dtos);
    return ivl;
  }

  private Collection<AccessTimeLogEntry> createCollection(
      Collection<AccessTimeLogEntryDto> dtos) {
    ArrayList<AccessTimeLogEntry> list =
        new ArrayList<AccessTimeLogEntry>(dtos.size());
    for (AccessTimeLogEntryDto dto : dtos) {
      AccessTimeLogEntry logEntry = new AccessTimeLogEntry(dto.getInodeId(),
          dto.getUserId(), dto.getAccessTime());
      list.add(logEntry);
    }
    return list;
  }
}
