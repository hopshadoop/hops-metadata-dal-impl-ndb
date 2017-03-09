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

import com.google.common.primitives.Ints;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.BlockLookUpDataAccess;
import io.hops.metadata.hdfs.entity.BlockLookUp;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.dalimpl.ClusterjDataAccess;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class BlockLookUpClusterj extends ClusterjDataAccess
    implements TablesDef.BlockLookUpTableDef, BlockLookUpDataAccess<BlockLookUp> {
  public BlockLookUpClusterj(ClusterjConnector connector) {
    super(connector);
  }

  @PersistenceCapable(table = TABLE_NAME)
  public interface BlockLookUpDTO {

    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long bid);

    @Column(name = INODE_ID)
    int getINodeId();

    void setINodeId(int iNodeID);

  }

  private final static int NOT_FOUND_ROW = -1000;

  @Override
  public void prepare(Collection<BlockLookUp> modified,
                      Collection<BlockLookUp> removed) throws StorageException {
    HopsSession session = getConnector().obtainSession();
    for (BlockLookUp block_lookup : removed) {
      BlockLookUpClusterj.BlockLookUpDTO bTable = session
          .newInstance(BlockLookUpClusterj.BlockLookUpDTO.class,
              block_lookup.getBlockId());
      session.deletePersistent(bTable);
      session.release(bTable);
    }

    for (BlockLookUp block_lookup : modified) {
      BlockLookUpClusterj.BlockLookUpDTO bTable =
          session.newInstance(BlockLookUpClusterj.BlockLookUpDTO.class);
      createPersistable(block_lookup, bTable);
      session.savePersistent(bTable);
      session.release(bTable);
    }
  }

  @Override
  public BlockLookUp findByBlockId(long blockId) throws StorageException {
    HopsSession session = getConnector().obtainSession();
    BlockLookUpClusterj.BlockLookUpDTO lookup =
        session.find(BlockLookUpClusterj.BlockLookUpDTO.class, blockId);
    if (lookup == null) {
      return null;
    }
    BlockLookUp blu = createBlockInfo(lookup);
    session.release(lookup);
    return blu;
  }

  @Override
  public int[] findINodeIdsByBlockIds(final long[] blockIds)
      throws StorageException {
    final HopsSession session = getConnector().obtainSession();
    return readINodeIdsByBlockIds(session, blockIds);
  }

  protected static int[] readINodeIdsByBlockIds(final HopsSession session,
                                                final long[] blockIds) throws StorageException {
    final List<BlockLookUpDTO> bldtos = new ArrayList<BlockLookUpDTO>();
    final List<Integer> inodeIds = new ArrayList<Integer>();
    for (int blk = 0; blk < blockIds.length; blk++) {

      BlockLookUpDTO bldto =
          session.newInstance(BlockLookUpDTO.class, blockIds[blk]);
      bldto.setINodeId(NOT_FOUND_ROW);
      bldto = session.load(bldto);
      bldtos.add(bldto);
    }
    session.flush();

    for (int i = 0; i < bldtos.size(); i++) {
      BlockLookUpClusterj.BlockLookUpDTO bld = bldtos.get(i);
      if (bld.getINodeId() != NOT_FOUND_ROW) {
        inodeIds.add(bld.getINodeId());
      } else {
        bld = session.find(BlockLookUpDTO.class, bld.getBlockId());
        if (bld != null) {
          //[M] BUG:
          //ClusterjConnector.LOG.error("xxx: Inode doesn't exists retries for " + bld.getBlockId() + " inodeId " + bld.getINodeId() + " at index " + i);
          inodeIds.add(bld.getINodeId());
        } else {
          inodeIds.add(NOT_FOUND_ROW);
        }
      }
    }
    session.release(bldtos);
    bldtos.clear();
    return Ints.toArray(inodeIds);
  }

  protected static BlockLookUp createBlockInfo(
      BlockLookUpClusterj.BlockLookUpDTO dto) {
    BlockLookUp lookup = new BlockLookUp(dto.getBlockId(), dto.getINodeId());
    return lookup;
  }

  protected static void createPersistable(BlockLookUp lookup,
                                          BlockLookUpClusterj.BlockLookUpDTO persistable) {
    persistable.setBlockId(lookup.getBlockId());
    persistable.setINodeId(lookup.getInodeId());
  }
}
