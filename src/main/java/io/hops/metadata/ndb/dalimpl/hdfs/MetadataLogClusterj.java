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

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.MetadataLogDataAccess;
import io.hops.metadata.hdfs.entity.MetadataLogEntry;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;

public class MetadataLogClusterj implements TablesDef.MetadataLogTableDef,
    MetadataLogDataAccess<MetadataLogEntry> {

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface MetadataLogEntryDto {
    @PrimaryKey
    @Column(name = DATASET_ID)
    int getDatasetId();

    void setDatasetId(int datasetId);

    @PrimaryKey
    @Column(name = INODE_ID)
    int getInodeId();

    void setInodeId(int inodeId);

    @PrimaryKey
    @Column(name = LOGICAL_TIME)
    int getLogicalTime();

    void setLogicalTime(int logicalTime);

    @Column(name = OPERATION)
    short getOperation();

    void setOperation(short operation);
  }

  @Override
  public void addAll(Collection<MetadataLogEntry> logEntries)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    ArrayList<MetadataLogEntryDto> added = new ArrayList<MetadataLogEntryDto>(
        logEntries.size());
    for (MetadataLogEntry logEntry : logEntries) {
      added.add(createPersistable(logEntry));
    }
    session.savePersistentAll(added);
    session.release(added);
  }

  @Override
  public void add(MetadataLogEntry metadataLogEntry) throws StorageException {
    HopsSession session = connector.obtainSession();
    MetadataLogEntryDto dto = createPersistable(metadataLogEntry);
    session.makePersistent(dto);
    session.release(dto);
    
  }

  private MetadataLogEntryDto createPersistable(MetadataLogEntry logEntry)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    MetadataLogEntryDto dto = session.newInstance(MetadataLogEntryDto.class);
    dto.setDatasetId(logEntry.getDatasetId());
    dto.setInodeId(logEntry.getInodeId());
    dto.setLogicalTime(logEntry.getLogicalTime());
    dto.setOperation(logEntry.getOperationOrdinal());
    return dto;
  }

  @Override
  public Collection<MetadataLogEntry> find(int fileId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<MetadataLogEntryDto> dobj =
        qb.createQueryDefinition(MetadataLogEntryDto.class);
    HopsPredicate pred1 = dobj.get("inodeId").equal(dobj.param("inodeIdParam"));
    dobj.where(pred1);
    HopsQuery<MetadataLogEntryDto> query = session.createQuery(dobj);
    query.setParameter("inodeIdParam", fileId);
    
    Collection<MetadataLogEntryDto> dtos = query.getResultList();
    Collection<MetadataLogEntry> mlel = createCollection(dtos);
    session.release(dtos);
    return mlel;
  }

  private Collection<MetadataLogEntry> createCollection(
      Collection<MetadataLogEntryDto> collection) {
    ArrayList<MetadataLogEntry> list =
        new ArrayList<MetadataLogEntry>(collection.size());
    for (MetadataLogEntryDto dto : collection) {
      list.add(createMetadataLogEntry(dto));
    }
    return list;
  }

  private MetadataLogEntry createMetadataLogEntry(MetadataLogEntryDto dto) {
    return new MetadataLogEntry(
        dto.getDatasetId(),
        dto.getInodeId(),
        dto.getLogicalTime(),
        MetadataLogEntry.Operation.values()[dto.getOperation()]);
  }

  @Override
  public Collection<MetadataLogEntry> readExisting(
      Collection<MetadataLogEntry> logEntries) throws StorageException {
    HopsSession session = connector.obtainSession();
    final ArrayList<MetadataLogEntryDto> dtos =
        new ArrayList<MetadataLogEntryDto>();
    for (MetadataLogEntry logEntry : logEntries) {
      Object[] pk = new Object[]{logEntry.getDatasetId(), logEntry.getInodeId(),
          logEntry.getLogicalTime()};
      MetadataLogEntryDto dto =
          session.newInstance(MetadataLogEntryDto.class, pk);
      dto = session.load(dto);
      dtos.add(dto);
    }
    session.flush();
    Collection<MetadataLogEntry> mlel = createCollection(dtos);
    session.release(dtos);
    return mlel;
  }
}
