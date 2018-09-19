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
    long getDatasetId();

    void setDatasetId(long datasetId);

    @PrimaryKey
    @Column(name = INODE_ID)
    long getInodeId();

    void setInodeId(long inodeId);

    @PrimaryKey
    @Column(name = Logical_TIME)
    int getLogicalTime();

    void setLogicalTime(int logicalTime);

    @Column(name = INODE_PARTITION_ID)
    long getInodePartitionId();

    void setInodePartitionId(long inodePartitionId);

    @Column(name = INODE_PARENT_ID)
    long getInodeParentId();

    void setInodeParentId(long inodeParentId);

    @Column(name = INODE_NAME)
    String getInodeName();

    void setInodeName(String inodeName);

    @Column(name = OPERATION)
    short getOperation();

    void setOperation(short operation);
  }


  @PersistenceCapable(table = LOOKUP_TABLE_NAME)
  public interface DatasetINodeLookupDTO{

    @PrimaryKey
    @Column(name = INODE_ID)
    long getInodeId();

    void setInodeId(long inodeId);

    @Column(name = DATASET_ID)
    long getDatasetId();

    void setDatasetId(long datasetId);
  }

  @Override
  public void addAll(Collection<MetadataLogEntry> logEntries)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    ArrayList<MetadataLogEntryDto> added = new ArrayList<>(
        logEntries.size());
    ArrayList<DatasetINodeLookupDTO> newLookupDTOS = new
        ArrayList<>(logEntries.size());
    try {
      for (MetadataLogEntry logEntry : logEntries) {
        added.add(createPersistable(logEntry));
        DatasetINodeLookupDTO lookupDTO = createLookupPersistable(logEntry);
        if (logEntry.getOperation() == MetadataLogEntry.Operation.ADD) {
          newLookupDTOS.add(lookupDTO);
        } else if (logEntry.getOperation() ==
            MetadataLogEntry.Operation.DELETE) {
          session.deletePersistent(lookupDTO);
          session.release(lookupDTO);
        }
      }

      session.makePersistentAll(added);
      session.savePersistentAll(newLookupDTOS);
    }finally {
      session.release(added);
      session.release(newLookupDTOS);
    }
  }

  @Override
  public void add(MetadataLogEntry metadataLogEntry) throws StorageException {
    HopsSession session = connector.obtainSession();
    MetadataLogEntryDto dto = null;
    DatasetINodeLookupDTO lookupDTO = null;
    try {
      dto = createPersistable(metadataLogEntry);
      lookupDTO = createLookupPersistable(metadataLogEntry);

      session.makePersistent(dto);

      if (metadataLogEntry.getOperation() == MetadataLogEntry.Operation.ADD) {
        session.savePersistent(lookupDTO);
      } else if (metadataLogEntry.getOperation() ==
          MetadataLogEntry.Operation.DELETE) {
        session.deletePersistent(lookupDTO);
      }
    }finally {
      session.release(dto);
      session.release(lookupDTO);
    }
  }

  private MetadataLogEntryDto createPersistable(MetadataLogEntry logEntry)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    MetadataLogEntryDto dto = session.newInstance(MetadataLogEntryDto.class);
    dto.setDatasetId(logEntry.getDatasetId());
    dto.setInodeId(logEntry.getInodeId());
    dto.setInodePartitionId(logEntry.getInodePartitionId());
    dto.setInodeParentId(logEntry.getInodeParentId());
    dto.setInodeName(logEntry.getInodeName());
    dto.setLogicalTime(logEntry.getLogicalTime());
    dto.setOperation(logEntry.getOperationOrdinal());
    return dto;
  }

  private DatasetINodeLookupDTO createLookupPersistable(MetadataLogEntry
      logEntry) throws StorageException {
    HopsSession session = connector.obtainSession();
    DatasetINodeLookupDTO dto = session.newInstance(DatasetINodeLookupDTO
        .class);
    dto.setDatasetId(logEntry.getDatasetId());
    dto.setInodeId(logEntry.getInodeId());
    return dto;
  }

  @Override
  public Collection<MetadataLogEntry> find(long fileId) throws StorageException {
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
        new ArrayList<>(collection.size());
    for (MetadataLogEntryDto dto : collection) {
      list.add(createMetadataLogEntry(dto));
    }
    return list;
  }

  private MetadataLogEntry createMetadataLogEntry(MetadataLogEntryDto dto) {
    return new MetadataLogEntry(
        dto.getDatasetId(),
        dto.getInodeId(),
        dto.getInodePartitionId(),
        dto.getInodeParentId(),
        dto.getInodeName(),
        dto.getLogicalTime(),
        MetadataLogEntry.Operation.values()[dto.getOperation()]);
  }

  @Override
  public Collection<MetadataLogEntry> readExisting(
      Collection<MetadataLogEntry> logEntries) throws StorageException {
    HopsSession session = connector.obtainSession();
    final ArrayList<MetadataLogEntryDto> dtos =
        new ArrayList<>();
    try {
      for (MetadataLogEntry logEntry : logEntries) {
        Object[] pk =
            new Object[]{logEntry.getDatasetId(), logEntry.getInodeId(),
                logEntry.getLogicalTime()};
        MetadataLogEntryDto dto =
            session.newInstance(MetadataLogEntryDto.class, pk);
        dto = session.load(dto);
        dtos.add(dto);
      }
      session.flush();
      Collection<MetadataLogEntry> mlel = createCollection(dtos);
      return mlel;
    }finally {
      session.release(dtos);
    }
  }
}
