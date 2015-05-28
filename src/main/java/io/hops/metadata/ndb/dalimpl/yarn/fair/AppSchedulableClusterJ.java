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
package io.hops.metadata.ndb.dalimpl.yarn.fair;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.fair.AppSchedulableDataAccess;
import io.hops.metadata.yarn.entity.fair.AppSchedulable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AppSchedulableClusterJ implements TablesDef.AppSchedulableTableDef,
        AppSchedulableDataAccess<AppSchedulable> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface AppSchedulableDTO {

    @PrimaryKey
    @Column(name = SCHEDULERAPP_ID)
    String getschedulerapp_id();

    void setschedulerapp_id(String schedulerapp_id);

    @Column(name = STARTTIME)
    long getstarttime();

    void setstarttime(long starttime);

    @Column(name = FSQUEUENAME)
    String getfsqueuename();

    void setfsqueuename(String fsqueuename);

  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public AppSchedulable findById(String id) throws StorageException {
    HopsSession session = connector.obtainSession();

    AppSchedulableClusterJ.AppSchedulableDTO appSchedulableDTO = null;
    if (session != null) {
      appSchedulableDTO = session.find(
              AppSchedulableClusterJ.AppSchedulableDTO.class, id);
    }
    if (appSchedulableDTO == null) {
      throw new StorageException("HOP :: Error while retrieving row");
    }

    return createAppSchedulable(appSchedulableDTO);
  }

  @Override
  public void addAll(Collection<AppSchedulable> modified) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    try {
      if (modified != null) {
        List<AppSchedulableClusterJ.AppSchedulableDTO> toModify
                = new ArrayList<AppSchedulableClusterJ.AppSchedulableDTO>();
        for (AppSchedulable hop : modified) {
          AppSchedulableClusterJ.AppSchedulableDTO persistable
                  = createPersistable(hop, session);
          toModify.add(persistable);
        }
        session.savePersistentAll(toModify);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void removeAll(Collection<AppSchedulable> removed) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    try {
      if (removed != null) {
        List<AppSchedulableClusterJ.AppSchedulableDTO> toRemove
                = new ArrayList<AppSchedulableClusterJ.AppSchedulableDTO>();
        for (AppSchedulable hop : removed) {
          AppSchedulableClusterJ.AppSchedulableDTO persistable = session.
                  newInstance(AppSchedulableClusterJ.AppSchedulableDTO.class,
                          hop.getSchedulerAppId());
          toRemove.add(persistable);
        }
        session.deletePersistentAll(toRemove);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private AppSchedulable createAppSchedulable(
          AppSchedulableDTO appSchedulableDTO) {
    return new AppSchedulable(appSchedulableDTO.getschedulerapp_id(),
            appSchedulableDTO.getstarttime(),
            appSchedulableDTO.getfsqueuename());
  }

  private AppSchedulableDTO createPersistable(AppSchedulable hop,
          HopsSession session) throws StorageException {
    AppSchedulableClusterJ.AppSchedulableDTO appSchedulableDTO = session.
            newInstance(AppSchedulableClusterJ.AppSchedulableDTO.class);

    appSchedulableDTO.setschedulerapp_id(hop.getSchedulerAppId());
    appSchedulableDTO.setstarttime(hop.getStarttime());
    appSchedulableDTO.setfsqueuename(hop.getQueuename());

    return appSchedulableDTO;
  }

}
