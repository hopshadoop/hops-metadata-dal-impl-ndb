/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2019  hops.io
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
import io.hops.metadata.hdfs.dal.ProvidedBlockCacheLocDataAccess;
import io.hops.metadata.hdfs.entity.ProvidedBlockCacheLoc;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.*;

public class ProvidedBlockCacheLocClusterj
        implements TablesDef.ProvidedBlockCacheLocTabDef,
        ProvidedBlockCacheLocDataAccess<ProvidedBlockCacheLoc> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ProvidedBlockLocationDTO {

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();
    void setBlockId(long bid);

    @Column(name = STORAGE_ID)
    int getStorageId();
    void setStorageId(int iNodeID);

  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private final static int NOT_FOUND_ROW = -1000;

  @Override
  public void update(Collection<ProvidedBlockCacheLoc> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ProvidedBlockLocationDTO> changes = new ArrayList<>();

    try {
      for (ProvidedBlockCacheLoc loc : modified) {
        ProvidedBlockLocationDTO dto = convert(session, loc);
        changes.add(dto);
      }

      session.savePersistentAll(changes);
    } finally {
      session.release(changes);
    }
  }

  @Override
  public void delete(Collection<Long> removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ProvidedBlockLocationDTO> deletions = new ArrayList<>();

    try {
      for (Long blkID : removed) {
        ProvidedBlockCacheLocClusterj.ProvidedBlockLocationDTO dto = session
                .newInstance(ProvidedBlockCacheLocClusterj.ProvidedBlockLocationDTO.class, blkID);
        deletions.add(dto);
      }

      session.deletePersistentAll(deletions);
    } finally {
      session.release(deletions);
    }
  }

  @Override
  public Map<Long, ProvidedBlockCacheLoc> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    Map<Long, ProvidedBlockCacheLoc> map = new HashMap<>();
    HopsQuery<ProvidedBlockLocationDTO> query =
            session.createQuery(qb.createQueryDefinition(ProvidedBlockLocationDTO.class));
    List<ProvidedBlockLocationDTO> dtos = query.getResultList();
    for(ProvidedBlockLocationDTO dto : dtos){
      ProvidedBlockCacheLoc pbcl = convert(dto);
      map.put(pbcl.getBlockID(), pbcl);
    }
    session.release(dtos);
    return map;
  }

  @Override
  public ProvidedBlockCacheLoc findByBlockID(long blockId) throws StorageException {
    HopsSession session = connector.obtainSession();
    ProvidedBlockCacheLocClusterj.ProvidedBlockLocationDTO dto =
            session.find(ProvidedBlockCacheLocClusterj.ProvidedBlockLocationDTO.class, blockId);
    if (dto == null) {
      return null;
    }

    ProvidedBlockCacheLoc blu = convert(dto);
    session.release(dto);
    return blu;
  }

  @Override
  public Map<Long, ProvidedBlockCacheLoc> findByBlockIDs(final long[] blockIds)
          throws StorageException {
    final HopsSession session = connector.obtainSession();
    return batchRead(session, blockIds);
  }

  protected Map<Long, ProvidedBlockCacheLoc> batchRead(final HopsSession session,
                                                       final long[] blockIds) throws StorageException {
    final List<ProvidedBlockLocationDTO> bldtos = new ArrayList<>();
    final Map<Long, ProvidedBlockCacheLoc> cacheLocationMap = new HashMap<>();
    try {
      for (long blockId : blockIds) {
        ProvidedBlockLocationDTO bldto =
                session.newInstance(ProvidedBlockLocationDTO.class, blockId);
        bldto.setStorageId(NOT_FOUND_ROW);
        bldto = session.load(bldto);
        bldtos.add(bldto);
      }
      session.flush();

      for (ProvidedBlockLocationDTO dto : bldtos) {
        if (dto.getStorageId() != NOT_FOUND_ROW) {
          cacheLocationMap.put(dto.getBlockId(), convert(dto));
        }
      }
    } finally {
      session.release(bldtos);
    }
    return cacheLocationMap;
  }

  protected ProvidedBlockCacheLoc convert(ProvidedBlockLocationDTO dto) {
    ProvidedBlockCacheLoc lookup = new ProvidedBlockCacheLoc(dto.getBlockId(),
            dto.getStorageId());
    return lookup;
  }

  protected ProvidedBlockLocationDTO convert(HopsSession session,
                                             ProvidedBlockCacheLoc lookup)
          throws StorageException {
    ProvidedBlockLocationDTO dto = session.newInstance(ProvidedBlockCacheLocClusterj.ProvidedBlockLocationDTO.class);
    dto.setBlockId(lookup.getBlockID());
    dto.setStorageId(lookup.getStorageID());
    return dto;
  }
}
