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
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.fair.RunnableAppsDataAccess;
import io.hops.metadata.yarn.entity.fair.RunnableApps;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RunnableAppsClusterJ implements TablesDef.RunnableAppsTableDef,
        RunnableAppsDataAccess<RunnableApps> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface RunnableAppsDTO {

    @PrimaryKey
    @Column(name = QUEUENAME)
    String getqueuename();

    void setqueuename(String queuename);

    @Column(name = SCHEDULERAPP_ID)
    String getschedulerapp_id();

    void setschedulerapp_id(String schedulerapp_id);

    @Column(name = ISRUNNABLE)
    boolean getisrunnable();

    void setisrunnable(boolean isrunnable);

  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<RunnableApps> findById(String queuename) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<RunnableAppsClusterJ.RunnableAppsDTO> dobj = qb.
            createQueryDefinition(RunnableAppsClusterJ.RunnableAppsDTO.class);
    HopsPredicate pred1 = dobj.get("queuename").equal(dobj.param("queuename"));
    dobj.where(pred1);
    HopsQuery<RunnableAppsClusterJ.RunnableAppsDTO> query = session.
            createQuery(dobj);
    query.setParameter("queuename", queuename);

    List<RunnableAppsClusterJ.RunnableAppsDTO> results = query.getResultList();
    return createRunnableAppsList(results);
  }

  @Override
  public void add(RunnableApps modified) throws StorageException {
    HopsSession session = connector.obtainSession();

    if (modified != null) {

      RunnableAppsClusterJ.RunnableAppsDTO persistable = createPersistable(
              modified, session);

      session.savePersistent(persistable);
    }
  }

  @Override
  public void removeAll(Collection<RunnableApps> removed) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    if (removed != null) {
      List<RunnableAppsClusterJ.RunnableAppsDTO> toRemove
              = new ArrayList<RunnableAppsClusterJ.RunnableAppsDTO>();
      for (RunnableApps hop : removed) {
        Object[] objarr = new Object[2];
        objarr[0] = hop.getQueuename();
        objarr[1] = hop.getSchedulerapp_id();
        toRemove.add(session.newInstance(
                RunnableAppsClusterJ.RunnableAppsDTO.class, objarr));
      }
      session.deletePersistentAll(toRemove);
    }
  }

  private RunnableApps createRunnableApps(RunnableAppsDTO runnableAppsDTO) {
    return new RunnableApps(runnableAppsDTO.getqueuename(),
            runnableAppsDTO.getschedulerapp_id(),
            runnableAppsDTO.getisrunnable());
  }

  private RunnableAppsDTO createPersistable(RunnableApps hop,
          HopsSession session) throws StorageException {
    RunnableAppsClusterJ.RunnableAppsDTO runnableAppsDTO = session.newInstance(
            RunnableAppsClusterJ.RunnableAppsDTO.class);

    runnableAppsDTO.setqueuename(hop.getQueuename());
    runnableAppsDTO.setschedulerapp_id(hop.getSchedulerapp_id());
    runnableAppsDTO.setisrunnable(hop.isIsrunnable());

    return runnableAppsDTO;
  }

  private List<RunnableApps> createRunnableAppsList(
          List<RunnableAppsDTO> results) {
    List<RunnableApps> hopRunnableApps = new ArrayList<RunnableApps>();
    for (RunnableAppsDTO persistable : results) {
      hopRunnableApps.add(createRunnableApps(persistable));
    }
    return hopRunnableApps;
  }
}
