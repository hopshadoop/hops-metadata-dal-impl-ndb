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
import io.hops.metadata.hdfs.dal.BlockChecksumDataAccess;
import io.hops.metadata.hdfs.entity.BlockChecksum;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.HopsSQLExceptionHelper;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class BlockChecksumClusterj
    implements TablesDef.BlockChecksumTableDef, BlockChecksumDataAccess<BlockChecksum> {

  static final Log LOG = LogFactory.getLog(BlockChecksumClusterj.class);

  private ClusterjConnector clusterjConnector = ClusterjConnector.getInstance();
  private MysqlServerConnector mysqlConnector =
      MysqlServerConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface BlockChecksumDto {

    @PrimaryKey
    @Column(name = INODE_ID)
    int getInodeId();

    void setInodeId(int inodeId);

    @PrimaryKey
    @Column(name = BLOCK_INDEX)
    int getBlockIndex();

    void setBlockIndex(int blockIndex);

    @Column(name = CHECKSUM)
    long getChecksum();

    void setChecksum(long checksum);
  }

  @Override
  public void add(BlockChecksum blockChecksum) throws StorageException {
    LOG.info("ADD " + blockChecksum.toString());
    HopsSession session = clusterjConnector.obtainSession();
    BlockChecksumDto dto = session.newInstance(BlockChecksumDto.class);
    copyState(blockChecksum, dto);
    session.makePersistent(dto);
    session.release(dto);
  }

  @Override
  public void update(BlockChecksum blockChecksum) throws StorageException {
    LOG.info("UPDATE " + blockChecksum.toString());
    HopsSession session = clusterjConnector.obtainSession();
    BlockChecksumDto dto = session.newInstance(BlockChecksumDto.class);
    copyState(blockChecksum, dto);
    session.updatePersistent(dto);
    session.release(dto);
  }

  @Override
  public void delete(BlockChecksum blockChecksum) throws StorageException {
    LOG.info("DELETE " + blockChecksum.toString());
    HopsSession session = clusterjConnector.obtainSession();
    BlockChecksumDto dto = session.newInstance(BlockChecksumDto.class);
    copyState(blockChecksum, dto);
    session.deletePersistent(dto);
    session.release(dto);
  }

  @Override
  public BlockChecksum find(int inodeId, int blockIndex)
      throws StorageException {
    HopsSession session = clusterjConnector.obtainSession();
    BlockChecksumDto dto =
        session.find(BlockChecksumDto.class, new Object[]{inodeId, blockIndex});
    if (dto == null) {
      return null;
    }
    
    BlockChecksum bcs = createBlockChecksum(dto);
    session.release(dto);
    return bcs;
  }

  @Override
  public Collection<BlockChecksum> findAll(int inodeId)
      throws StorageException {
    HopsSession session = clusterjConnector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<BlockChecksumDto> dobj =
        qb.createQueryDefinition(BlockChecksumDto.class);
    HopsPredicate pred1 = dobj.get("inodeId").equal(dobj.param("iNodeParam"));
    dobj.where(pred1);
    HopsQuery<BlockChecksumDto> query = session.createQuery(dobj);
    query.setParameter("iNodeParam", inodeId);
    List<BlockChecksumDto> dtos = query.getResultList();
    Collection<BlockChecksum> csl = createBlockChecksumList(dtos);
    session.release(dtos);
    return csl;
  }

  @Override
  public void deleteAll(int inodeId) throws StorageException {
    final String query =
        String.format("DELETE FROM block_checksum WHERE %s=%d");
    try {
      Connection conn = mysqlConnector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      s.executeQuery();
    } catch (SQLException ex) {
      throw HopsSQLExceptionHelper.wrap(ex);
    } finally {
      mysqlConnector.closeSession();
    }
  }

  private void copyState(BlockChecksum blockChecksum, BlockChecksumDto dto) {
    dto.setInodeId(blockChecksum.getInodeId());
    dto.setBlockIndex(blockChecksum.getBlockIndex());
    dto.setChecksum(blockChecksum.getChecksum());
  }

  private List<BlockChecksum> createBlockChecksumList(
      List<BlockChecksumDto> dtoList) {
    List<BlockChecksum> list = new ArrayList<BlockChecksum>();
    for (BlockChecksumDto dto : dtoList) {
      list.add(createBlockChecksum(dto));
    }
    return list;
  }

  private BlockChecksum createBlockChecksum(BlockChecksumDto dto) {
    if (dto == null) {
      return null;
    }

    return new BlockChecksum(dto.getInodeId(), dto.getBlockIndex(),
        dto.getChecksum());
  }
}
