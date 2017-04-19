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
package io.hops.metadata.ndb.mysqlserver;

import io.hops.exception.StorageException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This class is to do count operations using Mysql Server.
 */
public class CountHelper {

  public static final String COUNT_QUERY = "select count(*) from %s";
  public static final String COUNT_QUERY_UNIQUE =
      "select count(distinct %s) from %s";
  public static final String COUNT_WHERE = "select count(*) from %s where %s";
  
  public static int countWhere(MysqlServerConnector connector, String tableName, String condition)
      throws StorageException {
    String query = String.format(COUNT_WHERE, tableName, condition);
    return count(connector, query);
  }

  /**
   * Counts the number of rows in a given table.
   * <p/>
   * This creates and closes connection in every request.
   *
   * @param tableName
   * @return Total number of rows a given table.
   * @throws StorageException
   */
  public static int countAll(MysqlServerConnector connector, String tableName) throws StorageException {
    // TODO[H]: Is it good to create and close connections in every call?
    String query = String.format(COUNT_QUERY, tableName);
    return count(connector, query);
  }
  
  public static int countAllUnique(MysqlServerConnector connector, String tableName, String columnName)
      throws StorageException {
    String query = String.format(COUNT_QUERY_UNIQUE, columnName, tableName);
    return count(connector, query);
  }
  
  private static int count(MysqlServerConnector connector, String query) throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet result = s.executeQuery();
      if (result.next()) {
        return result.getInt(1);
      } else {
        throw new StorageException(
            String.format("Count result set is empty. Query: %s", query));
      }
    } catch (SQLException ex) {
      throw new StorageException(ex);
    } finally {
      connector.closeSession();
    }
  }

  /**
   * Counts the number of rows in a table specified by the table name where
   * satisfies the given criterion. The criterion should be a valid SLQ
   * statement.
   *
   * @param tableName
   * @param criterion
   *     E.g. criterion="id > 100".
   * @return
   */
  public static int countWithCriterion(MysqlServerConnector connector, String tableName, String criterion)
      throws StorageException {
    StringBuilder queryBuilder =
        new StringBuilder(String.format(COUNT_QUERY, tableName)).
            append(" where ").
            append(criterion);
    return count(connector, queryBuilder.toString());
  }
}
