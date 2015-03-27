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
import io.hops.metadata.yarn.dal.capacity.FiCaSchedulerAppReservationsDataAccess;
import io.hops.metadata.yarn.entity.capacity.FiCaSchedulerAppReservations;

import java.util.Collection;

public class FiCaSchedulerAppReservationsClusterJ
    implements TablesDef.FiCaSchedulerAppReservationsTableDef,
    FiCaSchedulerAppReservationsDataAccess<FiCaSchedulerAppReservations> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface FiCaSchedulerAppReservationsDTO {

    @PrimaryKey
    @Column(name = SCHEDULERAPP_ID)
    String getschedulerappid();

    void setschedulerappid(String schedulerappid);

    @Column(name = PRIORITY_ID)
    int getpriorityid();

    void setpriorityid(int priorityid);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public FiCaSchedulerAppReservations findById(int id) throws StorageException {
    HopsSession session = connector.obtainSession();

    FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO
        fiCaSchedulerAppReservationsDTO = null;
    if (session != null) {
      fiCaSchedulerAppReservationsDTO = session.find(
          FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO.class,
          id);
    }
    if (fiCaSchedulerAppReservationsDTO == null) {
      throw new StorageException("HOP :: Error while retrieving row");
    }

    return createHopFiCaSchedulerAppReservations(
        fiCaSchedulerAppReservationsDTO);
  }

  @Override
  public void prepare(Collection<FiCaSchedulerAppReservations> modified,
      Collection<FiCaSchedulerAppReservations> removed)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    try {
      if (removed != null) {
        for (FiCaSchedulerAppReservations hop : removed) {
          FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO
              persistable = session.newInstance(
              FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO.class,
              hop.getSchedulerapp_id());
          session.deletePersistent(persistable);
        }
      }
      if (modified != null) {
        for (FiCaSchedulerAppReservations hop : modified) {
          FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO
              persistable = createPersistable(hop, session);
          session.savePersistent(persistable);
        }
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }


  private FiCaSchedulerAppReservations createHopFiCaSchedulerAppReservations(
      FiCaSchedulerAppReservationsDTO fiCaSchedulerAppReservationsDTO) {
    return new FiCaSchedulerAppReservations(
        fiCaSchedulerAppReservationsDTO.getschedulerappid(),
        fiCaSchedulerAppReservationsDTO.getpriorityid());
  }

  private FiCaSchedulerAppReservationsDTO createPersistable(
      FiCaSchedulerAppReservations hop, HopsSession session)
      throws StorageException {
    FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO
        fiCaSchedulerAppReservationsDTO = session.newInstance(
        FiCaSchedulerAppReservationsClusterJ.FiCaSchedulerAppReservationsDTO.class);

    fiCaSchedulerAppReservationsDTO.setschedulerappid(hop.getSchedulerapp_id());
    fiCaSchedulerAppReservationsDTO.setpriorityid(hop.getPriority_id());

    return fiCaSchedulerAppReservationsDTO;
  }
}
