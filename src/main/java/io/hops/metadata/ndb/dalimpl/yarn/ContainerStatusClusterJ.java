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
import static io.hops.metadata.yarn.TablesDef.ContainerStatusTableDef.RMNODEID;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.entity.ContainerStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ContainerStatusClusterJ implements
    TablesDef.ContainerStatusTableDef,
    ContainerStatusDataAccess<ContainerStatus> {

  private static final Log LOG = LogFactory.
      getLog(ContainerStatusClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface ContainerStatusDTO {

    @PrimaryKey
    @Column(name = CONTAINERID)
    String getcontainerid();

    void setcontainerid(String containerid);

    @PrimaryKey
    @Column(name = RMNODEID)
    String getrmnodeid();

    void setrmnodeid(String rmnodeid);

    @PrimaryKey
    @Column(name = TYPE)
    String getType();

    void setType(String type);

    @Column(name = STATE)
    String getstate();

    void setstate(String state);

    @Column(name = DIAGNOSTICS)
    String getdiagnostics();

    void setdiagnostics(String diagnostics);

    @Column(name = EXIT_STATUS)
    int getexitstatus();

    void setexitstatus(int exitstatus);
    
    @Column(name = PENDING_EVENT_ID)
    int getpendingeventid();

    void setpendingeventid(int pedingeventid);
    

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public ContainerStatus findEntry(String containerId, String rmNodeId, 
          String type)
      throws StorageException {
    LOG.debug("HOP :: ClusterJ ContainerStatus.findById - START");
    HopsSession session = connector.obtainSession();

    ContainerStatusDTO uciDTO;
    if (session != null) {
      uciDTO = session
          .find(ContainerStatusDTO.class, new Object[]{containerId, rmNodeId, 
              type});
      LOG.debug("HOP :: ClusterJ ContainerStatus.findById - FINISH");
      if (uciDTO != null) {
          ContainerStatus result = createHopContainerStatus(uciDTO);
          session.release(uciDTO);
          return result;
      }
    }
    return null;
  }

  @Override
  public Map<String, ContainerStatus> getAll() throws StorageException {
    LOG.debug("HOP :: ClusterJ ContainerStatus.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<ContainerStatusDTO> dobj =
        qb.createQueryDefinition(ContainerStatusDTO.class);
    HopsQuery<ContainerStatusDTO> query = session.createQuery(dobj);

    List<ContainerStatusDTO> queryResults = query.getResultList();
    LOG.debug("HOP :: ClusterJ ContainerStatus.getAll - START");
    Map<String, ContainerStatus> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public void addAll(Collection<ContainerStatus> containersStatus)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerStatusDTO> toAdd = new ArrayList<ContainerStatusDTO>();
    for (ContainerStatus containerStatus : containersStatus) {
      toAdd.add(createPersistable(containerStatus, session));
    }
    session.savePersistentAll(toAdd);
//    session.flush();
    session.release(toAdd);
  }

  @Override
  public void removeAll(Collection<ContainerStatus> containersStatus)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerStatusDTO> toRemove = new ArrayList<ContainerStatusDTO>();
    for (ContainerStatus containerStatus : containersStatus) {
      toRemove.add(createPersistable(containerStatus, session));
    }
    session.deletePersistentAll(toRemove);
    session.release(toRemove);
  }
  
  private ContainerStatusDTO createPersistable(ContainerStatus hopCS,
      HopsSession session) throws StorageException {
    ContainerStatusDTO csDTO = session.newInstance(ContainerStatusDTO.class);
    //Set values to persist new ContainerStatus
    csDTO.setcontainerid(hopCS.getContainerid());
    csDTO.setstate(hopCS.getState());
    csDTO.setdiagnostics(hopCS.getDiagnostics());
    csDTO.setexitstatus(hopCS.getExitstatus());
    csDTO.setrmnodeid(hopCS.getRMNodeId());
    csDTO.setpendingeventid(hopCS.getPendingEventId());
    csDTO.setType(hopCS.getType().name());
    return csDTO;
  }

  private static ContainerStatus createHopContainerStatus(
          ContainerStatusDTO csDTO) {
    ContainerStatus hop = new ContainerStatus(csDTO.getcontainerid(), csDTO.
            getstate(),
            csDTO.getdiagnostics(), csDTO.getexitstatus(), csDTO.getrmnodeid(),
            csDTO.getpendingeventid(), 
            ContainerStatus.Type.valueOf(csDTO.getType()));
    return hop;
  }

  public static Map<String, ContainerStatus> createMap(
      List<ContainerStatusDTO> results) {
    Map<String, ContainerStatus> map = new HashMap<String, ContainerStatus>();
    for (ContainerStatusDTO persistable : results) {
      ContainerStatus hop = createHopContainerStatus(persistable);
      map.put(hop.getContainerid(), hop);
    }
    return map;
  }

  public static List<ContainerStatus> createList(
          List<ContainerStatusDTO> results) {
    List<ContainerStatus> list
            = new ArrayList<ContainerStatus>();
    for (ContainerStatusDTO persistable : results) {
      ContainerStatus hop = createHopContainerStatus(persistable);
      list.add(hop);
    }
    return list;
  }
}
