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
package io.hops.metadata.ndb.wrapper;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.SessionFactory;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.entity.Storage;

import java.util.Map;

public class HopsSessionFactory {
  private final SessionFactory factory;
  private final ClusterJCaching clusterJCaching;

  public HopsSessionFactory(SessionFactory factory, ClusterJCaching clusterJCaching) {
    this.factory = factory;
    this.clusterJCaching = clusterJCaching;
  }

  public HopsSession getSession() throws StorageException {
    try {
      return new HopsSession(factory.getSession(), clusterJCaching);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public HopsSession getSession(Map map) throws StorageException {
    try {
      return new HopsSession(factory.getSession(map), clusterJCaching);
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void close() throws StorageException {
    try {
      factory.close();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public boolean isOpen() throws StorageException {
    try {
      return factory.currentState() == SessionFactory.State.Open;
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }

  public void reconnect() throws StorageException {
    try {
      factory.reconnect();
    } catch (ClusterJException e) {
      throw HopsExceptionHelper.wrap(e);
    }
  }
}
