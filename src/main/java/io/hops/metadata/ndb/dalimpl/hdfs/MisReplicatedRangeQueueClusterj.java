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
import io.hops.metadata.hdfs.dal.MisReplicatedRangeQueueDataAccess;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.dalimpl.ClusterjDataAccess;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsSession;

public class MisReplicatedRangeQueueClusterj extends ClusterjDataAccess
    implements TablesDef.MisReplicatedRangeQueueTableDef, MisReplicatedRangeQueueDataAccess {

  public MisReplicatedRangeQueueClusterj(ClusterjConnector connector) {
    super(connector);
  }

  @PersistenceCapable(table = TABLE_NAME)
  public interface MisReplicatedRangeQueueDTO {

    @PrimaryKey
    @Column(name = RANGE)
    String getRange();

    void setRange(String range);
  }

  private final static String SEPERATOR = "-";

  @Override
  public void insert(long start, long end) throws StorageException {
    try {
      HopsSession session = getConnector().obtainSession();
      MisReplicatedRangeQueueDTO dto = session
          .newInstance(MisReplicatedRangeQueueDTO.class, getRange(start, end));
      session.savePersistent(dto);
      session.release(dto);
    } catch (Exception e) {
      throw new StorageException(e);
    }

  }

  @Override
  public void remove(long start, long end) throws StorageException {
    try {
      HopsSession session = getConnector().obtainSession();
      MisReplicatedRangeQueueDTO oldR = session
          .newInstance(MisReplicatedRangeQueueDTO.class, getRange(start, end));
      session.deletePersistent(oldR);
      session.release(oldR);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(getMysqlConnector(), TABLE_NAME);
  }

  private String getRange(long start, long end) {
    return start + SEPERATOR + end;
  }
}
