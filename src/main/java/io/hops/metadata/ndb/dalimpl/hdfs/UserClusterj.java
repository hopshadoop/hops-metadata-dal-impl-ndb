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

import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.UserDataAccess;
import io.hops.metadata.hdfs.entity.User;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;

import java.sql.ResultSet;
import java.sql.SQLException;


public class UserClusterj implements TablesDef.UsersTableDef, UserDataAccess<User>{

  @Override
  public User getUser(final int userId) throws StorageException {
    final String query = String.format("SELECT %s FROM %s WHERE %s=%d",
        NAME, TABLE_NAME, ID, userId);

    return MySQLQueryHelper.execute(query,
        new MySQLQueryHelper.ResultSetHandler<User>() {

          @Override
          public User handle(ResultSet result)
              throws SQLException, StorageException {
            if (!result.next()){
              return null;
            }
            return  new User(userId, result.getString(NAME));
          }
        });
  }

  @Override
  public User getUser(final String userName) throws StorageException {
    final String query = String.format("SELECT %s FROM %s WHERE %s='%s'",
        ID, TABLE_NAME, NAME, userName);

    return MySQLQueryHelper.execute(query,
        new MySQLQueryHelper.ResultSetHandler<User>() {

          @Override
          public User handle(ResultSet result)
              throws SQLException, StorageException {
            if (!result.next()) {
              return null;
            }
            return  new User(result.getInt(ID), userName);
          }
        });
  }

  @Override
  public User addUser(String userName) throws StorageException{
    final String query = String.format("INSERT IGNORE INTO %s (%s) VALUES" +
        "('%s')", TABLE_NAME, NAME, userName);
    MySQLQueryHelper.execute(query);
    return getUser(userName);
  }

}
