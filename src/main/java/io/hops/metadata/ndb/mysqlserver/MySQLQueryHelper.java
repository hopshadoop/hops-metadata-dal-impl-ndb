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
public class MySQLQueryHelper {

  public static final String COUNT_QUERY = "select count(*) from %s";
  public static final String COUNT_QUERY_UNIQUE =
      "select count(distinct %s) from %s";
  public static final String SELECT_EXISTS = "select exists(%s)";
  public static final String SELECT_EXISTS_QUERY = "select * from %s";
  public static final String MIN = "select min(%s) from %s";
  public static final String MAX = "select max(%s) from %s";
  
  private static MysqlServerConnector connector =
      MysqlServerConnector.getInstance();

  /**
   * Counts the number of rows in a given table.
   * <p/>
   * This creates and closes connection in every request.
   *
   * @param tableName
   * @return Total number of rows a given table.
   * @throws io.hops.exception.StorageException
   */
  public static int countAll(String tableName) throws StorageException {
    // TODO[H]: Is it good to create and close connections in every call?
    String query = String.format(COUNT_QUERY, tableName);
    return executeIntAggrQuery(query);
  }
  
  public static int countAllUnique(String tableName, String columnName)
      throws StorageException {
    String query = String.format(COUNT_QUERY_UNIQUE, columnName, tableName);
    return executeIntAggrQuery(query);
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
  public static int countWithCriterion(String tableName, String criterion)
      throws StorageException {
    StringBuilder queryBuilder =
        new StringBuilder(String.format(COUNT_QUERY, tableName)).
            append(" where ").
            append(criterion);
    return executeIntAggrQuery(queryBuilder.toString());
  }
  
  public static boolean exists(String tableName, String criterion)
      throws StorageException {
    StringBuilder query =
        new StringBuilder(String.format(SELECT_EXISTS_QUERY, tableName));
    query.append(" where ").append(criterion);
    return executeBooleanQuery(String.format(SELECT_EXISTS, query.toString()));
  }

  public static int minInt(String tableName, String column, String criterion)
      throws StorageException {
    StringBuilder query =
        new StringBuilder(String.format(MIN, column, tableName));
    query.append(" where ").append(criterion);
    return executeIntAggrQuery(query.toString());
  }
  
  public static int maxInt(String tableName, String column, String criterion)
      throws StorageException {
    StringBuilder query =
        new StringBuilder(String.format(MAX, column, tableName));
    query.append(" where ").append(criterion);
    return executeIntAggrQuery(query.toString());
  }

  private static int executeIntAggrQuery(final String query)
      throws StorageException {
    return execute(query, new ResultSetHandler<Integer>() {
      @Override
      public Integer handle(ResultSet result) throws SQLException, StorageException {
        if (!result.next()) {
          throw new StorageException(
              String.format("result set is empty. Query: %s", query));
        }
        return result.getInt(1);
      }
    });
  }
  
  private static boolean executeBooleanQuery(final String query)
      throws StorageException {
    return execute(query, new ResultSetHandler<Boolean>() {
      @Override
      public Boolean handle(ResultSet result) throws SQLException, StorageException {
        if (!result.next()) {
          throw new StorageException(
              String.format("result set is empty. Query: %s", query));
        }
        return result.getBoolean(1);
      }
    });
  }
  
  public static interface ResultSetHandler<R> {
    R handle(ResultSet result) throws SQLException, StorageException;
  }
  
  public static <R> R execute(String query, ResultSetHandler<R> handler)
      throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet result = s.executeQuery();
      return handler.handle(result);
    } catch (SQLException ex) {
      throw new StorageException(ex);
    } finally {
      connector.closeSession();
    }
  }
}
