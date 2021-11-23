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
package io.hops.metadata.ndb;

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.ClusterJUserException;
import com.mysql.clusterj.Constants;
import com.mysql.clusterj.LockMode;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.wrapper.ClusterJCaching;
import io.hops.metadata.ndb.wrapper.HopsExceptionHelper;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.ndb.wrapper.HopsSessionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class DBSessionProvider implements Runnable {

  static final Log LOG = LogFactory.getLog(DBSessionProvider.class);
  static HopsSessionFactory sessionFactory;
  private ConcurrentLinkedQueue<DBSession> sessionPool =
      new ConcurrentLinkedQueue<>();
  private ConcurrentLinkedQueue<DBSession> toGC =
      new ConcurrentLinkedQueue<>();
  private final int MAX_REUSE_COUNT;
  private Properties conf;
  private final Random rand;
  private AtomicInteger sessionsCreated = new AtomicInteger(0);
  private long rollingAvg[];
  private AtomicInteger rollingAvgIndex = new AtomicInteger(-1);
  private boolean automaticRefresh = false;
  private Thread thread;
  private final ClusterJCaching clusterJCaching;
  private UUID currentConnectionID;
  private final int initialPoolSize;

  public DBSessionProvider(Properties conf)
      throws StorageException {
    this.conf = conf;

    boolean useClusterjDtoCache = Boolean.parseBoolean(
            (String) conf.get("io.hops.enable.clusterj.dto.cache"));
    boolean useClusterjSessionCache = Boolean.parseBoolean(
            (String) conf.get("io.hops.enable.clusterj.session.cache"));
    clusterJCaching = new ClusterJCaching(useClusterjDtoCache, useClusterjSessionCache);

    initialPoolSize = Integer.parseInt(
            (String) conf.get("io.hops.session.pool.size"));
    int reuseCount = Integer.parseInt(
            (String) conf.get("io.hops.session.reuse.count"));

    if (reuseCount <= 0) {
      System.err.println("Invalid value for session reuse count");
      System.exit(-1);
    }

    this.MAX_REUSE_COUNT = reuseCount;
    rand = new Random(System.currentTimeMillis());
    rollingAvg = new long[initialPoolSize];
    start(initialPoolSize);
  }

  private void start(int initialPoolSize) throws StorageException {
    LOG.info("Database connect string: " +
        conf.get(Constants.PROPERTY_CLUSTER_CONNECTSTRING));
    LOG.info(
        "Database name: " + conf.get(Constants.PROPERTY_CLUSTER_DATABASE));
    LOG.info("Max Transactions: " +
        conf.get(Constants.PROPERTY_CLUSTER_MAX_TRANSACTIONS));
    LOG.info("Reconnect Timeout: " +
      conf.get(Constants.PROPERTY_CONNECTION_RECONNECT_TIMEOUT));
    LOG.info("Using ClusterJ Session Cache: "+clusterJCaching.useClusterjSessionCache());
    LOG.info("Using ClusterJ DTO Cache: "+clusterJCaching.useClusterjDtoCache());
    try {
      sessionFactory =
          new HopsSessionFactory(ClusterJHelper.getSessionFactory(conf), clusterJCaching);
    } catch (ClusterJException ex) {
      throw HopsExceptionHelper.wrap(ex);
    }

    createNewSessions();

    thread = new Thread(this, "Session Pool Refresh Daemon");
    thread.setDaemon(true);
    automaticRefresh = true;
    thread.start();
  }

  private DBSession initSession() throws StorageException {
    Long startTime = System.currentTimeMillis();
    HopsSession session = sessionFactory.getSession();
    Long sessionCreationTime = (System.currentTimeMillis() - startTime);
    rollingAvg[rollingAvgIndex.incrementAndGet() % rollingAvg.length] =
        sessionCreationTime;

    int reuseCount = rand.nextInt(MAX_REUSE_COUNT) + 1;
    DBSession dbSession = new DBSession(session, reuseCount, currentConnectionID);
    sessionsCreated.incrementAndGet();
    return dbSession;
  }

  private void closeSession(DBSession dbSession) throws StorageException {
    Long startTime = System.currentTimeMillis();
    dbSession.getSession().close();
    Long sessionCreationTime = (System.currentTimeMillis() - startTime);
    rollingAvg[rollingAvgIndex.incrementAndGet() % rollingAvg.length] =
        sessionCreationTime;
  }

  public void stop() throws StorageException {
    automaticRefresh = false;
    while (!sessionPool.isEmpty()) {
      DBSession dbsession = sessionPool.remove();
      closeSession(dbsession);
    }
  }

  public DBSession getSession() throws StorageException {
    try {
      DBSession session = sessionPool.remove();
      return session;
    } catch (NoSuchElementException e) {
      LOG.warn("DB session provider cant keep up with the demand for new sessions");
      return initSession();
    }
  }

  public void returnSession(DBSession returnedSession, Exception... exceptions) throws StorageException {
    boolean forceClose = false;
    if(sessionFactory.isOpen()){
      for(Exception e : exceptions){
        if (e == null) continue;
        Throwable cause = e.getCause();
        if (cause instanceof ClusterJDatastoreException) {
          forceClose = true;
        } else if (cause instanceof ClusterJUserException){
          if(cause.getMessage().contains("No more operations can be performed while this Db is " +
            "closing")){
            forceClose = true;
          }
        } else if (returnedSession.getConnectionID() != currentConnectionID) {
          forceClose = true;
        }
      }
    }

    //session has been used, increment the use counter
    returnedSession
        .setSessionUseCount(returnedSession.getSessionUseCount() + 1);

    if (sessionFactory.isOpen() && ((returnedSession.getSessionUseCount() >=
        returnedSession.getMaxReuseCount()) ||
        forceClose)) { // session can be closed even before the reuse count has expired. Close
      // the session incase of database errors.
      toGC.add(returnedSession);
    } else { // increment the count and return it to the pool
      returnedSession.getSession().setLockMode(LockMode.READ_COMMITTED);
      sessionPool.add(returnedSession);
    }
  }

  public double getSessionCreationRollingAvg() {
    double avg = 0;
    for (long aRollingAvg : rollingAvg) {
      avg += aRollingAvg;
    }
    avg = avg / rollingAvg.length;
    return avg;
  }

  public int getTotalSessionsCreated() {
    return sessionsCreated.get();
  }

  public int getAvailableSessions() {
    return sessionPool.size();
  }

  boolean reconnecting = false;
  @Override
  public void run() {
    while (automaticRefresh) {
      try {
        if (!sessionFactory.isOpen()) {
          reconnecting = true;
          sessionFactory.reconnect();
        }

        if ( sessionFactory.isOpen() && reconnecting ){
          // reconnected after network failure
          // close all old sessions and start new sessions
          reconnecting = false;
          currentConnectionID = UUID.randomUUID();
          renewAllSessions();
          gcSessions(false);
        } else {
          // do normal gc
          gcSessions(true);
        }

        Thread.sleep(50);
      } catch (NoSuchElementException e) {
        for (int i = 0; i < 100; i++) {
          try {
            sessionPool.add(initSession());
          } catch (StorageException e1) {
            LOG.error(e1);
          }
        }
      } catch (InterruptedException ex) {
        LOG.warn(ex);
        Thread.currentThread().interrupt();
      } catch (StorageException e) {
        LOG.error(e);
      }
    }
  }

  public void renewAllSessions() throws StorageException {
    while (!sessionPool.isEmpty()) {
      DBSession session = sessionPool.poll();
      if (session != null) {
        closeSession(session);
      }
    }
    createNewSessions();
  }

  public void gcSessions(boolean createNewSessions) throws StorageException {
    int toGCSize = toGC.size();

    if (toGCSize > 0) {
      LOG.debug("Renewing a session(s) " + toGCSize);
      for (int i = 0; i < toGCSize; i++) {
        DBSession session = toGC.remove();
        closeSession(session);
      }

      if(createNewSessions){
        for (int i = 0; i < toGCSize; i++) {
          sessionPool.add(initSession());
        }
      }
    }
  }

  public void createNewSessions() throws StorageException {
    for (int i = 0; i < initialPoolSize; i++) {
      sessionPool.add(initSession());
    }
  }

  public void clearCache() throws StorageException {
    Iterator<DBSession> itr = sessionPool.iterator();
    while(itr.hasNext()){
      DBSession session = itr.next();
      session.getSession().dropInstanceCache();
    }
  }
}
