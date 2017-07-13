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

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.ClusterJHelper;
import com.mysql.clusterj.Constants;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.wrapper.HopsExceptionHelper;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.ndb.wrapper.HopsSessionFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Random;
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

  public DBSessionProvider(Properties conf, int reuseCount, int initialPoolSize)
      throws StorageException {
    this.conf = conf;
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
    System.out.println("Database connect string: " +
        conf.get(Constants.PROPERTY_CLUSTER_CONNECTSTRING));
    System.out.println(
        "Database name: " + conf.get(Constants.PROPERTY_CLUSTER_DATABASE));
    System.out.println("Max Transactions: " +
        conf.get(Constants.PROPERTY_CLUSTER_MAX_TRANSACTIONS));
    try {
      sessionFactory =
          new HopsSessionFactory(ClusterJHelper.getSessionFactory(conf));
    } catch (ClusterJException ex) {
      throw HopsExceptionHelper.wrap(ex);
    }

    for (int i = 0; i < initialPoolSize; i++) {
      sessionPool.add(initSession());
    }

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
    DBSession dbSession = new DBSession(session, reuseCount);
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
      LOG.warn(
          "DB Sessino provider cant keep up with the demand for new sessions");
      return initSession();
    }
  }

  public void returnSession(DBSession returnedSession, boolean forceClose) {
    //session has been used, increment the use counter
    returnedSession
        .setSessionUseCount(returnedSession.getSessionUseCount() + 1);

    if ((returnedSession.getSessionUseCount() >=
        returnedSession.getMaxReuseCount()) ||
        forceClose) { // session can be closed even before the reuse count has expired. Close the session incase of database errors.
      toGC.add(returnedSession);
    } else { // increment the count and return it to the pool
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

  @Override
  public void run() {
    while (automaticRefresh) {
      try {
        int toGCSize = toGC.size();

        if (toGCSize > 0) {
          LOG.debug("Renewing a session(s) " + toGCSize);
          for (int i = 0; i < toGCSize; i++) {
            DBSession session = toGC.remove();
            session.getSession().close();
          }
          //System.out.println("CGed " + toGCSize);

          for (int i = 0; i < toGCSize; i++) {
            sessionPool.add(initSession());
          }
          //System.out.println("Created " + toGCSize);
        }
        //                for (int i = 0; i < 100; i++) {
        //                    DBSession session = sessionPool.remove();
        //                    double percent = (((double) session.getSessionUseCount() / (double) session.getMaxReuseCount()) * (double) 100);
        //                    // System.out.print(session.getSessionUseCount()+","+session.getMaxReuseCount()+","+percent+" ");
        //                    if (percent > 80) { // more than 80% used then recyle it
        //                        session.getSession().close();
        //                        System.out.println("Recycled a session");
        //                        //add a new session
        //                        sessionPool.add(initSession());
        //                    } else {
        //                        sessionPool.add(session);
        //                    }
        //                }
        Thread.sleep(5);
      } catch (NoSuchElementException e) {
        //System.out.print(".");
        for (int i = 0; i < 100; i++) {
          try {
            sessionPool.add(initSession());
          } catch (StorageException e1) {
            LOG.error(e1);
          }
        }
      } catch (InterruptedException ex) {
        LOG.warn(ex);
      } catch (StorageException e) {
        LOG.error(e);
      }
    }
  }
}
