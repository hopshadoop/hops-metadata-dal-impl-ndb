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
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.ContainerToDecreaseDataAccess;
import io.hops.metadata.yarn.entity.Container;
import io.hops.metadata.yarn.entity.ContainerToSignal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ContainerToDecreaseClusterJ implements TablesDef.ContainerToDecreaseTableDef,
    ContainerToDecreaseDataAccess<Container> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ContainerToDecreaseDTO {

    @PrimaryKey
    @Column(name = RMNODEID)
    String getRmnodeid();

    void setRmnodeid(String rmnodeid);

    @PrimaryKey
    @Column(name = CONTAINERID)
    String getContainerid();

    void setContainerid(String containerid);

    @PrimaryKey
    @Column(name = HTTPADDRESS)
    String getHttpAddress();

    void setHttpAddress(String httpAddress);

    @PrimaryKey
    @Column(name = PRIORITY)
    int getPriority();

    void setPriority(int priority);

    @PrimaryKey
    @Column(name = MEMSIZE)
    long getMemSize();

    void setMemSize(long memSize);

    @PrimaryKey
    @Column(name = VIRTUALCORES)
    int getVirtualCores();

    void setVirtualCores(int virtualCores);

    @PrimaryKey
    @Column(name = GPUS)
    int getGpus();

    void setGpus(int gpus);

    @PrimaryKey
    @Column(name = VERSION)
    int getVersion();

    void setVersion(int version);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void addAll(Collection<Container> containers) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerToDecreaseDTO> toModify = new ArrayList<>();
    for (Container hop : containers) {
      toModify.add(createPersistable(hop, session));
    }
    session.savePersistentAll(toModify);
    session.release(toModify);
  }

  @Override
  public void removeAll(Collection<Container> containers) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerToDecreaseDTO> toRemove = new ArrayList<>();
    for (Container hop : containers) {
      toRemove.add(createPersistable(hop, session));
    }
    session.deletePersistentAll(toRemove);
    session.release(toRemove);
  }

  private ContainerToDecreaseDTO createPersistable(Container hop, HopsSession session) throws StorageException {
    ContainerToDecreaseDTO dto = session.newInstance(ContainerToDecreaseDTO.class);
    //Set values to persist new ContainerStatus
    dto.setRmnodeid(hop.getNodeId());
    dto.setContainerid(hop.getContainerId());
    dto.setGpus(hop.getGpus());
    dto.setHttpAddress(hop.getHttpAddress());
    dto.setMemSize(hop.getMemSize());
    dto.setPriority(hop.getPriority());
    dto.setVersion(hop.getVersion());
    dto.setVirtualCores(hop.getVirtualCores());

    return dto;
  }

}
