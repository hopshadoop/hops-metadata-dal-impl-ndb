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


import com.google.common.collect.Lists;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.GroupDataAccess;
import io.hops.metadata.hdfs.entity.Group;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

public class GroupClusterj implements TablesDef.GroupsTableDef, GroupDataAccess<Group>{

  @PersistenceCapable(table = TABLE_NAME)
  public interface GroupDTO {

    @PrimaryKey
    @Column(name = ID)
    int getId();

    void setId(int id);

    @Column(name = NAME)
    String getName();

    void setName(String name);
  }

  @Override
  public Group getGroup(final int groupId) throws StorageException {
    final String query = String.format("SELECT %s FROM %s WHERE %s=%d",
        NAME, TABLE_NAME, ID, groupId);

    return MySQLQueryHelper.execute(query,
        new MySQLQueryHelper.ResultSetHandler<Group>() {

          @Override
          public Group handle(ResultSet result)
              throws SQLException, StorageException {
            if (!result.next()) {
              return null;
            }
            return  new Group(groupId, result.getString(NAME));
          }
        });
  }

  @Override
  public Group getGroup(final String groupName) throws StorageException {
    final String query = String.format("SELECT %s FROM %s WHERE %s='%s'",
        ID, TABLE_NAME, NAME, groupName);

    return MySQLQueryHelper.execute(query,
        new MySQLQueryHelper.ResultSetHandler<Group>() {

          @Override
          public Group handle(ResultSet result)
              throws SQLException, StorageException {
            if (!result.next()) {
              return null;
            }
            return  new Group(result.getInt(ID), groupName);
          }
        });
  }


  @Override
  public Group addGroup(String groupName) throws StorageException {
    final String query = String.format("INSERT IGNORE INTO %s (%s) VALUES" +
        "('%s')", TABLE_NAME, NAME, groupName);
    MySQLQueryHelper.execute(query);
    return getGroup(groupName);
  }

  static List<Group> convertAndRelease(HopsSession session, Collection<GroupDTO>
      dtos)
      throws StorageException {
    List<Group> groups = Lists.newArrayListWithExpectedSize(dtos.size());
    for(GroupDTO dto : dtos){
      groups.add(new Group(dto.getId(), dto.getName()));
      session.release(dto);
    }
    return groups;
  }
}
