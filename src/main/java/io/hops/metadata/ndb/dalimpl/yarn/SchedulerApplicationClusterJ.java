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
package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.SchedulerApplicationDataAccess;
import io.hops.metadata.yarn.entity.SchedulerApplication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchedulerApplicationClusterJ
    implements TablesDef.SchedulerApplicationTableDef,
    SchedulerApplicationDataAccess<SchedulerApplication> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface SchedulerApplicationDTO {

    @PrimaryKey
    @Column(name = APPID)
    String getappid();

    void setappid(String appid);

    @Column(name = USER)
    String getuser();

    void setuser(String user);

    @Column(name = QUEUENAME)
    String getqueuename();

    void setqueuename(String queuename);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Map<String, SchedulerApplication> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<SchedulerApplicationDTO> dobj =
        qb.createQueryDefinition(
            SchedulerApplicationClusterJ.SchedulerApplicationDTO.class);
    HopsQuery<SchedulerApplicationDTO> query = session.
        createQuery(dobj);
    List<SchedulerApplicationClusterJ.SchedulerApplicationDTO> results = query.
        getResultList();
    return createApplicationIdMap(results);
  }

  @Override
  public void addAll(Collection<SchedulerApplication> toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<SchedulerApplicationDTO> toPersist =
        new ArrayList<SchedulerApplicationDTO>();
    for (SchedulerApplication req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }
    session.savePersistentAll(toPersist);
    session.flush();
  }

  @Override
  public void removeAll(Collection<SchedulerApplication> toRemove)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<SchedulerApplicationDTO> toPersist =
        new ArrayList<SchedulerApplicationDTO>();
    for (SchedulerApplication entry : toRemove) {
      toPersist.add(session.newInstance(SchedulerApplicationDTO.class, entry.
          getAppid()));
    }
    session.deletePersistentAll(toPersist);
  }

  private Map<String, SchedulerApplication> createApplicationIdMap(
      List<SchedulerApplicationClusterJ.SchedulerApplicationDTO> list) {
    Map<String, SchedulerApplication> schedulerApplications =
        new HashMap<String, SchedulerApplication>();
    for (SchedulerApplicationClusterJ.SchedulerApplicationDTO persistable : list) {
      SchedulerApplication app = createHopSchedulerApplication(persistable);
      schedulerApplications.put(app.getAppid(), app);
    }
    return schedulerApplications;
  }

  private SchedulerApplication createHopSchedulerApplication(
      SchedulerApplicationDTO schedulerApplicationDTO) {
    return new SchedulerApplication(schedulerApplicationDTO.getappid(),
        schedulerApplicationDTO.getuser(), schedulerApplicationDTO.
        getqueuename());
  }

  private SchedulerApplicationDTO createPersistable(SchedulerApplication hop,
      HopsSession session) throws StorageException {
    SchedulerApplicationClusterJ.SchedulerApplicationDTO
        schedulerApplicationDTO = session.newInstance(
        SchedulerApplicationClusterJ.SchedulerApplicationDTO.class);

    schedulerApplicationDTO.setappid(hop.getAppid());
    schedulerApplicationDTO.setuser(hop.getUser());
    schedulerApplicationDTO.setqueuename(hop.getQueuename());
    return schedulerApplicationDTO;
  }

}
