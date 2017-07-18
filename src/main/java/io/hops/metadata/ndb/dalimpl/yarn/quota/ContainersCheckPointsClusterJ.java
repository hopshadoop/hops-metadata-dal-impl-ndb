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
package io.hops.metadata.ndb.dalimpl.yarn.quota;

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
import io.hops.metadata.yarn.dal.quota.ContainersCheckPointsDataAccess;
import io.hops.metadata.yarn.entity.quota.ContainerCheckPoint;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ContainersCheckPointsClusterJ implements
        TablesDef.ContainersCheckPointsTableDef,
        ContainersCheckPointsDataAccess<ContainerCheckPoint> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ContainerCheckPointDTO {

    @PrimaryKey
    @Column(name = CONTAINERID)
    String getContainerID();

    void setContainerID(String containerID);

    @Column(name = CHECKPOINT)
    long getCheckPoint();

    void setCheckPoint(long checkPoint);
    
    @Column(name = MULTIPLICATOR)
    float getPrice();

    void setPrice(float price);
    
    
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Map<String, ContainerCheckPoint> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<ContainerCheckPointDTO> dobj = qb.
            createQueryDefinition(ContainerCheckPointDTO.class);
    HopsQuery<ContainerCheckPointDTO> query = session.createQuery(dobj);

    List<ContainerCheckPointDTO> queryResults = query.getResultList();
    Map<String, ContainerCheckPoint> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public void addAll(List<ContainerCheckPoint> containersCheckPoints) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerCheckPointDTO> toAdd
            = new ArrayList<>();
    for (ContainerCheckPoint checkPoint : containersCheckPoints) {
      toAdd.add(createPersistable(checkPoint, session));
    }
    session.savePersistentAll(toAdd);
    session.release(toAdd);
  }

  @Override
  public void removeAll(List<ContainerCheckPoint> containersCheckPoints) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerCheckPointDTO> toRemove
            = new ArrayList<>();
    for (ContainerCheckPoint checkPoint : containersCheckPoints) {
      toRemove.add(createPersistable(checkPoint, session));
    }
    session.deletePersistentAll(toRemove);
    session.release(toRemove);
  }
  
  private ContainerCheckPointDTO createPersistable(
          ContainerCheckPoint checkPoint, HopsSession session) throws
          StorageException {
    ContainerCheckPointDTO ccpDTO = session.newInstance(ContainerCheckPointDTO.class);
    //Set values to persist new ContainerStatus
    ccpDTO.setContainerID(checkPoint.getContainerId());
    ccpDTO.setCheckPoint(checkPoint.getCheckPoint());
    ccpDTO.setPrice(checkPoint.getMultiplicator());

    return ccpDTO;
  }

  public static Map<String, ContainerCheckPoint> createMap(
          List<ContainerCheckPointDTO> dtos) {
    Map<String, ContainerCheckPoint> map = new HashMap<>();
    for (ContainerCheckPointDTO dto : dtos) {
      map.put(dto.getContainerID(), new ContainerCheckPoint(dto.getContainerID()
                                                           ,dto.getCheckPoint(), 
                                                            dto.getPrice()));
    }
    return map;
  }

}
