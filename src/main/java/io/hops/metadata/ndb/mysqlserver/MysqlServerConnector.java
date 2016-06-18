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

import com.mysql.clusterj.Constants;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.common.EntityDataAccess;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * This class presents a singleton connector to Mysql Server. It creates
 * connections to Mysql Server and loads the driver.
 */
public class MysqlServerConnector implements StorageConnector<Connection> {

  static final Log LOG = LogFactory.getLog(MysqlServerConnector.class);
  private final static MysqlServerConnector instance =
          new MysqlServerConnector();
  private Properties conf;
  /**
   * Never access this variable directly. Use getConnectionPool
   *
   * @see MysqlServerConnector#getConnectionPool()
   */
  private static volatile HikariDataSource connectionPool;
  private ThreadLocal<Connection> connection = new ThreadLocal<Connection>();

  public static MysqlServerConnector getInstance() {
    return instance;
  }

  @Override
  public void setConfiguration(Properties conf) throws StorageException {
    this.conf = conf;
  }

  private void initializeConnectionPool(Properties conf) {
    HikariConfig config = new HikariConfig();
    config.setMaximumPoolSize(Integer.valueOf(conf.getProperty(
            io.hops.metadata.ndb.mysqlserver.Constants.PROPERTY_MYSQL_CONNECTION_POOL_SIZE)));
    config.setDataSourceClassName(conf.getProperty(
            io.hops.metadata.ndb.mysqlserver.Constants.PROPERTY_MYSQL_DATA_SOURCE_CLASS_NAME));
    config.addDataSourceProperty("serverName", conf.getProperty(
            io.hops.metadata.ndb.mysqlserver.Constants.PROPERTY_MYSQL_HOST));
    config.addDataSourceProperty("port", conf.getProperty(
            io.hops.metadata.ndb.mysqlserver.Constants.PROPERTY_MYSQL_PORT));
    config.addDataSourceProperty("databaseName",
            conf.getProperty(Constants.PROPERTY_CLUSTER_DATABASE));
    config.addDataSourceProperty("user", conf.getProperty(
            io.hops.metadata.ndb.mysqlserver.Constants.PROPERTY_MYSQL_USERNAME));
    config.addDataSourceProperty("password", conf.getProperty(
            io.hops.metadata.ndb.mysqlserver.Constants.PROPERTY_MYSQL_PASSWORD));

    connectionPool = new HikariDataSource(config);
  }

  @Override
  public Connection obtainSession() throws StorageException {
    Connection conn = connection.get();
    if (conn == null) {
      HikariDataSource connectionPool = getConnectionPool();
      try {
        conn = connectionPool.getConnection();
        connection.set(conn);
      } catch (SQLException ex) {
        throw HopsSQLExceptionHelper.wrap(ex);
      }
    }
    return conn;
  }

  /**
   * This method uses the dupple-checked locking method to safely implement a
   * multithreaded lazy init.
   * http://www.cs.umd.edu/~pugh/java/memoryModel/DoubleCheckedLocking.html
   *
   * @return
   */
  private HikariDataSource getConnectionPool() {
    if (connectionPool == null) {
      synchronized (this) {
        if (connectionPool == null) {
          initializeConnectionPool(conf);
        }
      }
    }
    return connectionPool;
  }

  public void closeSession() throws StorageException {
    Connection conn = connection.get();
    if (conn != null) {
      try {
        conn.close();
        connection.remove();
      } catch (SQLException ex) {
        throw HopsSQLExceptionHelper.wrap(ex);
      }
    }
  }

  public static void truncateTable(boolean transactional, String tableName)
          throws StorageException, SQLException {
    truncateTable(transactional, tableName, -1);
  }

  public static void truncateTable(String tableName, int limit)
          throws StorageException, SQLException {
    truncateTable(true, tableName, -1);
  }

  public static void truncateTable(boolean transactional, String tableName,
          int limit) throws StorageException, SQLException {
    MysqlServerConnector connector = MysqlServerConnector.getInstance();
    try {
      Connection conn = connector.obtainSession();
      if (transactional) {
        if (limit > 0) {
          PreparedStatement s = conn.prepareStatement(
                  "delete from " + tableName + " limit " + limit);
          s.executeUpdate();
        } else {
          int nbrows = 0;
          do {
            PreparedStatement s = conn.prepareStatement(
                    "delete from " + tableName + " limit 1000");
            nbrows = s.executeUpdate();
          } while (nbrows > 0);
        }
      } else {
        PreparedStatement s =
                conn.prepareStatement("truncate table " + tableName);
        s.executeUpdate();
      }
    } finally {
      connector.closeSession();
    }
  }

  @Override
  public void beginTransaction() throws StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void commit() throws StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void rollback() throws StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean formatStorage() throws StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public boolean formatStorage(Class<? extends EntityDataAccess>... das)
          throws StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isTransactionActive() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void stopStorage() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void readLock() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void writeLock() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void readCommitted() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setPartitionKey(Class className, Object key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean formatStorageNonTransactional() throws StorageException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void dropAndRecreateDB() throws StorageException {
    MysqlServerConnector connector = MysqlServerConnector.getInstance();

    try {
      Connection conn = null;
      Statement stmt = null;
      String sql = "";
      String database = conf.getProperty("com.mysql.clusterj.database");
      try {
        conn = connector.obtainSession();
        stmt = conn.createStatement();

        sql = "DROP DATABASE IF EXISTS " + database;
        LOG.warn("Dropping database " + database);
        stmt.executeUpdate(sql);
        LOG.warn("Database dropped");
      } catch (Exception e) {
        LOG.warn(e);
      }

      try {
        sql = "CREATE DATABASE  " + database;
        LOG.warn("Creating database " + database);
        stmt.executeUpdate(sql);
        LOG.warn("Database created");
      } catch (Exception e) {
        LOG.warn(e);
      }

      try {
        sql = "use  " + database;
        LOG.warn("Selectign database " + database);
        stmt.executeUpdate(sql);
        LOG.warn("Database selected");
      } catch (Exception e) {
        LOG.warn(e);
      }

      try {
        ScriptRunner runner = new ScriptRunner(conn, false, false);
        LOG.warn("Importing Database");
        runner.runScript(new BufferedReader(new InputStreamReader(getSchema())));
        LOG.warn("Schema imported");
      } catch (Exception e) {
        LOG.warn(e);
      }
    } finally {
      connector.closeSession();
      //System.exit(0); //The namenode can not continue after that. Format the database afterwards
    }

  }

  public InputStream getSchema()
          throws IOException {
    String configFile = "schema.sql";
    InputStream inStream =
            StorageConnector.class.getClassLoader().getResourceAsStream(configFile);
    return inStream;
  }
}
