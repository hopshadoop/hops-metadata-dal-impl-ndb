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
import static io.hops.metadata.yarn.TablesDef.ContainersLogsTableDef.EXITSTATUS;
import static io.hops.metadata.yarn.TablesDef.ContainersLogsTableDef.MB;
import io.hops.metadata.yarn.dal.quota.ContainersLogsDataAccess;
import io.hops.metadata.yarn.entity.quota.ContainerLog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ContainersLogsClusterJ implements
        TablesDef.ContainersLogsTableDef,
        ContainersLogsDataAccess<ContainerLog> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ContainerLogDTO {

    @PrimaryKey
    @Column(name = CONTAINERID)
    String getcontainerid();

    void setcontainerid(String containerid);

    @Column(name = START)
    long getstart();

    void setstart(long start);

    @Column(name = STOP)
    long getstop();

    void setstop(long stop);

    @Column(name = EXITSTATUS)
    int getexitstatus();

    void setexitstatus(int exitstate);
    
    @Column(name = PRICE)
    float getPrice();

    void setPrice(float price);

    @Column(name = VCORES)
    int getVCores();

    void setVCores(int vcores);
    
    @Column(name = MB)
    long getMB();

    void setMB(long mb);

    @Column(name = GPUS)
    int getGpus();

    void setGpus(int mb);
    
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void addAll(Collection<ContainerLog> containersLogs) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerLogDTO> toAdd = new ArrayList<>();

    for (ContainerLog entry : containersLogs) {
      toAdd.add(createPersistable(entry, session));
    }

    session.savePersistentAll(toAdd);
    session.release(toAdd);
  }

  @Override
  public void removeAll(Collection<ContainerLog> containersLogs) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerLogDTO> toRemove = new ArrayList<>();

    for (ContainerLog entry : containersLogs) {
      toRemove.add(createPersistable(entry, session));
    }

    session.deletePersistentAll(toRemove);
    session.flush();
    session.release(toRemove);
  }
  
  @Override
  public Map<String, ContainerLog> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<ContainerLogDTO> dobj = qb.createQueryDefinition(
            ContainerLogDTO.class);
    HopsQuery<ContainerLogDTO> query = session.createQuery(dobj);

    List<ContainerLogDTO> queryResults = query.getResultList();
    Map<String, ContainerLog> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  private ContainerLogDTO createPersistable(ContainerLog hopCL,
          HopsSession session) throws StorageException {
    ContainerLogDTO clDTO = session.newInstance(ContainerLogDTO.class);

    //Set values to persist new ContainerLog
    clDTO.setcontainerid(hopCL.getContainerid());
    clDTO.setstart(hopCL.getStart());
    clDTO.setstop(hopCL.getStop());
    clDTO.setexitstatus(hopCL.getExitstatus());
    clDTO.setPrice(hopCL.getPrice());
    clDTO.setVCores(hopCL.getNbVcores());
    clDTO.setMB(hopCL.getMemoryUsed());
    clDTO.setGpus(hopCL.getGpuUsed());
    return clDTO;
  }

  private static Map<String, ContainerLog> createMap(
          List<ContainerLogDTO> results) {
    Map<String, ContainerLog> map = new HashMap<>();
    for (ContainerLogDTO persistable : results) {
      ContainerLog hop = createHopContainerLog(persistable);
      map.put(hop.getContainerid(), hop);
    }
    return map;
  }

  private static ContainerLog createHopContainerLog(
          ContainerLogDTO clDTO) {
    ContainerLog hop = new ContainerLog(
            clDTO.getcontainerid(),
            clDTO.getstart(),
            clDTO.getstop(),
            clDTO.getexitstatus(), 
            clDTO.getPrice(),
            clDTO.getVCores(),
            clDTO.getMB(),
            clDTO.getGpus()
    );
    return hop;
  }
}
