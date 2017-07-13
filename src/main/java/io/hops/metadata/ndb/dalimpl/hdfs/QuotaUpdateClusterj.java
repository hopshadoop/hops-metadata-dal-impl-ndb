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
import io.hops.metadata.hdfs.dal.QuotaUpdateDataAccess;
import io.hops.metadata.hdfs.entity.QuotaUpdate;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.HopsSQLExceptionHelper;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class QuotaUpdateClusterj
    implements TablesDef.QuotaUpdateTableDef, QuotaUpdateDataAccess<QuotaUpdate> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface QuotaUpdateDTO {

    @PrimaryKey
    @Column(name = ID)
    int getId();

    void setId(int id);

    @PrimaryKey
    @Column(name = INODE_ID)
    int getInodeId();

    void setInodeId(int id);

    @Column(name = NAMESPACE_DELTA)
    long getNamespaceDelta();

    void setNamespaceDelta(long namespaceDelta);

    @Column(name = DISKSPACE_DELTA)
    long getDiskspaceDelta();

    void setDiskspaceDelta(long diskspaceDelta);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private MysqlServerConnector mysqlConnector =
      MysqlServerConnector.getInstance();

  @Override
  public void prepare(Collection<QuotaUpdate> added,
      Collection<QuotaUpdate> removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<QuotaUpdateDTO> changes = new ArrayList<>();
    List<QuotaUpdateDTO> deletions = new ArrayList<>();
    try {
      if (removed != null) {
        for (QuotaUpdate update : removed) {
          QuotaUpdateDTO persistable = createPersistable(update, session);
          deletions.add(persistable);
        }
      }
      if (added != null) {
        for (QuotaUpdate update : added) {
          QuotaUpdateDTO persistable = createPersistable(update, session);
          changes.add(persistable);
        }
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    }finally {
      session.release(deletions);
      session.release(changes);
    }
  }

  private static final String FIND_QUERY =
      "SELECT * FROM " + TABLE_NAME + " ORDER BY " + ID + " LIMIT ";

  @Override
  public List<QuotaUpdate> findLimited(int limit) throws StorageException {
    ArrayList<QuotaUpdate> resultList;
    try {
      Connection conn = mysqlConnector.obtainSession();
      PreparedStatement s = conn.prepareStatement(FIND_QUERY + limit);
      ResultSet result = s.executeQuery();
      resultList = new ArrayList<>();

      while (result.next()) {
        int id = result.getInt(ID);
        int inodeId = result.getInt(INODE_ID);
        int namespaceDelta = result.getInt(NAMESPACE_DELTA);
        long diskspaceDelta = result.getLong(DISKSPACE_DELTA);
        resultList
            .add(new QuotaUpdate(id, inodeId, namespaceDelta, diskspaceDelta));
      }
    } catch (SQLException ex) {
      throw HopsSQLExceptionHelper.wrap(ex);
    } finally {
      mysqlConnector.closeSession();
    }
    return resultList;
  }

  private QuotaUpdateDTO createPersistable(QuotaUpdate update,
      HopsSession session) throws StorageException {
    QuotaUpdateDTO dto = session.newInstance(QuotaUpdateDTO.class);
    dto.setId(update.getId());
    dto.setInodeId(update.getInodeId());
    dto.setNamespaceDelta(update.getNamespaceDelta());
    dto.setDiskspaceDelta(update.getDiskspaceDelta());
    return dto;
  }

  private List<QuotaUpdate> convertAndRelease(HopsSession session,
      List<QuotaUpdateDTO> list) throws StorageException {
    List<QuotaUpdate> result = new ArrayList<>();
    for (QuotaUpdateDTO dto : list) {
      result.add(new QuotaUpdate(dto.getId(), dto.getInodeId(),
          dto.getNamespaceDelta(), dto.getDiskspaceDelta()));
      session.release(dto);
    }
    return result;
  }

  private static final String INODE_ID_PARAM = "inodeId";

  @Override
  public List<QuotaUpdate> findByInodeId(int inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<QuotaUpdateDTO> dobj =
        qb.createQueryDefinition(QuotaUpdateDTO.class);
    HopsPredicate pred1 = dobj.get("inodeId").equal(dobj.param(INODE_ID_PARAM));
    dobj.where(pred1);
    HopsQuery<QuotaUpdateDTO> query = session.createQuery(dobj);
    query.setParameter(INODE_ID_PARAM, inodeId);

    List<QuotaUpdateDTO> results = query.getResultList();
    return convertAndRelease(session, results);
  }

  @Override
  public int getCount() throws StorageException {
    int count = MySQLQueryHelper.countAll(TablesDef.QuotaUpdateTableDef.TABLE_NAME);
    return count;
  }
}