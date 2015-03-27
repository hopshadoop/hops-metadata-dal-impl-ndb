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
package io.hops.metadata.ndb.dalimpl.yarn.capacity;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.capacity.FiCaSchedulerAppReservedContainersDataAccess;
import io.hops.metadata.yarn.entity.capacity.FiCaSchedulerAppReservedContainers;

import java.util.Collection;

public class FiCaSchedulerAppReservedContainersClusterJ
    implements TablesDef.FiCaSchedulerAppReservedContainersTableDef,
    FiCaSchedulerAppReservedContainersDataAccess<FiCaSchedulerAppReservedContainers> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface FiCaSchedulerAppReservedContainersDTO {

    @PrimaryKey
    @Column(name = SCHEDULERAPP_ID)
    String getschedulerappid();

    void setschedulerappid(String schedulerappid);

    @Column(name = PRIORITY_ID)
    int getpriorityid();

    void setpriorityid(int priorityid);

    @Column(name = NODEID)
    int getnodeid();

    void setnodeid(int nodeid);

    @Column(name = RMCONTAINER_ID)
    String getrmcontainerid();

    void setrmcontainerid(String rmcontainerid);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public FiCaSchedulerAppReservedContainers findById(int id)
      throws StorageException {
    HopsSession session = connector.obtainSession();

    FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO
        fiCaSchedulerAppReservedContainersDTO = null;
    if (session != null) {
      fiCaSchedulerAppReservedContainersDTO = session.find(
          FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO.class,
          id);
    }
    if (fiCaSchedulerAppReservedContainersDTO == null) {
      throw new StorageException("HOP :: Error while retrieving row");
    }

    return createHopFiCaSchedulerAppReservedContainers(
        fiCaSchedulerAppReservedContainersDTO);
  }

  @Override
  public void prepare(Collection<FiCaSchedulerAppReservedContainers> modified,
      Collection<FiCaSchedulerAppReservedContainers> removed)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    try {
      if (removed != null) {
        for (FiCaSchedulerAppReservedContainers hop : removed) {
          FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO
              persistable = session.newInstance(
              FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO.class,
              hop.getSchedulerapp_id());
          session.deletePersistent(persistable);
        }
      }
      if (modified != null) {
        for (FiCaSchedulerAppReservedContainers hop : modified) {
          FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO
              persistable = createPersistable(hop, session);
          session.savePersistent(persistable);
        }
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private FiCaSchedulerAppReservedContainers createHopFiCaSchedulerAppReservedContainers(
      FiCaSchedulerAppReservedContainersDTO fiCaSchedulerAppReservedContainersDTO) {
    return new FiCaSchedulerAppReservedContainers(
        fiCaSchedulerAppReservedContainersDTO.getschedulerappid(),
        fiCaSchedulerAppReservedContainersDTO.getpriorityid(),
        fiCaSchedulerAppReservedContainersDTO.getnodeid(),
        fiCaSchedulerAppReservedContainersDTO.getrmcontainerid());
  }

  private FiCaSchedulerAppReservedContainersDTO createPersistable(
      FiCaSchedulerAppReservedContainers hop, HopsSession session)
      throws StorageException {
    FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO
        fiCaSchedulerAppReservedContainersDTO = session.newInstance(
        FiCaSchedulerAppReservedContainersClusterJ.FiCaSchedulerAppReservedContainersDTO.class);

    fiCaSchedulerAppReservedContainersDTO
        .setschedulerappid(hop.getSchedulerapp_id());
    fiCaSchedulerAppReservedContainersDTO.setpriorityid(hop.getPriority_id());
    fiCaSchedulerAppReservedContainersDTO.setnodeid(hop.getNodeid());
    fiCaSchedulerAppReservedContainersDTO
        .setrmcontainerid(hop.getRmcontainer_id());

    return fiCaSchedulerAppReservedContainersDTO;
  }

}

