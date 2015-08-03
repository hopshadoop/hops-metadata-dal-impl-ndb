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
import io.hops.metadata.hdfs.dal.SizeLogDataAccess;
import io.hops.metadata.hdfs.entity.SizeLogEntry;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;

public class SizeLogClusterj implements TablesDef.SizeLogTableDef,
    SizeLogDataAccess<SizeLogEntry> {

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface SizeLogEntryDto {
    @PrimaryKey
    @Column(name = INODE_ID)
    int getInodeId();

    void setInodeId(int inodeId);

    @PrimaryKey
    @Column(name = SIZE)
    long getSize();

    void setSize(long size);
  }

  @Override
  public void add(SizeLogEntry logEntry) throws StorageException {
    HopsSession session = connector.obtainSession();
    SizeLogEntryDto dto = session.newInstance(SizeLogEntryDto.class);
    dto.setInodeId(logEntry.getInodeId());
    dto.setSize(logEntry.getSize());
    session.savePersistent(dto);
    session.release(dto);
  }

  @Override
  public Collection<SizeLogEntry> find(int fileId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<SizeLogEntryDto> dobj =
        qb.createQueryDefinition(SizeLogEntryDto.class);
    HopsPredicate pred1 = dobj.get("inodeId").equal(dobj.param("inodeIdParam"));
    dobj.where(pred1);
    HopsQuery<SizeLogEntryDto> query = session.createQuery(dobj);
    query.setParameter("inodeIdParam", fileId);
    return convertAndRelease(session, query.getResultList());
  }

  private Collection<SizeLogEntry> convertAndRelease(HopsSession session,
      Collection<SizeLogEntryDto> dtos) throws StorageException {
    ArrayList<SizeLogEntry> list = new ArrayList<SizeLogEntry>(dtos.size());
    for (SizeLogEntryDto dto : dtos) {
      SizeLogEntry logEntry = new SizeLogEntry(dto.getInodeId(), dto.getSize());
      list.add(logEntry);
      session.release(dto);
    }
    return list;
  }
}
