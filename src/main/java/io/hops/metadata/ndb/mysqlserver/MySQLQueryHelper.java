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
  
  /**
   * Counts the number of rows in a given table.
   * <p/>
   * This creates and closes connection in every request.
   *
   * @param tableName
   * @return Total number of rows a given table.
   * @throws io.hops.exception.StorageException
   */
  public static int countAll(MysqlServerConnector connector, String tableName) throws StorageException {
    // TODO[H]: Is it good to create and close connections in every call?
    String query = String.format(COUNT_QUERY, tableName);
    return executeIntAggrQuery(connector, query);
  }
  
  public static int countAllUnique(MysqlServerConnector connector, String tableName, String columnName)
      throws StorageException {
    String query = String.format(COUNT_QUERY_UNIQUE, columnName, tableName);
    return executeIntAggrQuery(connector, query);
  }

  /**
   * Counts the number of rows in a table specified by the table name where
   * satisfies the given criterion. The criterion should be a valid SLQ
   * statement.
   *
   *
   * @param connector
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
    return executeIntAggrQuery(connector, queryBuilder.toString());
  }
  
  public static boolean exists(MysqlServerConnector connector, String tableName, String criterion)
      throws StorageException {
    StringBuilder query =
        new StringBuilder(String.format(SELECT_EXISTS_QUERY, tableName));
    query.append(" where ").append(criterion);
    return executeBooleanQuery(connector, String.format(SELECT_EXISTS, query.toString()));
  }

  public static int minInt(MysqlServerConnector connector, String tableName, String column, String criterion)
      throws StorageException {
    StringBuilder query =
        new StringBuilder(String.format(MIN, column, tableName));
    query.append(" where ").append(criterion);
    return executeIntAggrQuery(connector, query.toString());
  }
  
  public static int maxInt(MysqlServerConnector connector, String tableName, String column, String criterion)
      throws StorageException {
    StringBuilder query =
        new StringBuilder(String.format(MAX, column, tableName));
    query.append(" where ").append(criterion);
    return executeIntAggrQuery(connector, query.toString());
  }

  private static int executeIntAggrQuery(MysqlServerConnector connector, final String query)
      throws StorageException {
    return execute(connector, query, new ResultSetHandler<Integer>() {
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
  
  private static boolean executeBooleanQuery(MysqlServerConnector connector, final String query)
      throws StorageException {
    return execute(connector, query, new ResultSetHandler<Boolean>() {
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

  public static int execute(MysqlServerConnector connector, String query) throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      return s.executeUpdate();
    } catch (SQLException ex) {
      throw HopsSQLExceptionHelper.wrap(ex);
    } finally {
      connector.closeSession();
    }
  }

  public interface ResultSetHandler<R> {
    R handle(ResultSet result) throws SQLException, StorageException;
  }

  public static <R> R execute(MysqlServerConnector connector, String query, ResultSetHandler<R> handler)
      throws StorageException {
    try {
      Connection conn = connector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet result = s.executeQuery();
      return handler.handle(result);
    } catch (SQLException ex) {
      throw HopsSQLExceptionHelper.wrap(ex);
    } finally {
      connector.closeSession();
    }
  }
}
