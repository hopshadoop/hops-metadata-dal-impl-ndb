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
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.entity.RMContainer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RMContainerClusterJ
    implements TablesDef.RMContainerTableDef, RMContainerDataAccess<RMContainer> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface RMContainerDTO {

    @PrimaryKey
    @Column(name = CONTAINERID_ID)
    String getcontaineridid();

    void setcontaineridid(String containeridid);

    @Column(name = APPLICATIONATTEMPTID_ID)
    String getappattemptidid();

    void setappattemptidid(String appattemptidid);

    @Column(name = NODEID_ID)
    String getnodeidid();

    void setnodeidid(String nodeidid);

    @Column(name = USER)
    String getuser();

    void setuser(String user);

    @Column(name = STARTTIME)
    long getstarttime();

    void setstarttime(long starttime);

    @Column(name = FINISHTIME)
    long getfinishtime();

    void setfinishtime(long finishtime);

    @Column(name = STATE)
    String getstate();

    void setstate(String state);

    @Column(name = FINISHEDSTATUSSTATE)
    String getfinishedstatusstate();

    void setfinishedstatusstate(String finishedstatusstate);

    @Column(name = EXITSTATUS)
    int getexitstatus();

    void setexitstatus(int exitstatus);

      @Column(name = RESERVED_NODE_ID)
    String getreservednodeid();

    void setreservednodeid(String reservednodeid);

        @Column(name = RESERVED_PRIORITY)
    int getreservedpriority();

    void setreservedpriority(int reservedpriority);
    
        @Column(name = RESERVED_MEMORY)
    int getreservedmemory();
    
    void setreservedmemory(int reservedmemory);
    
        @Column(name = RESERVED_VCORES)
    int getreservedvcores();
    
    void setreservedvcores(int reservedvcores);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Map<String, RMContainer> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<RMContainerDTO> dobj =
        qb.createQueryDefinition(RMContainerDTO.class);
    HopsQuery<RMContainerDTO> query = session.
        createQuery(dobj);
    List<RMContainerDTO> results = query.
        getResultList();
    return createMap(results);
  }

  @Override

  public void addAll(Collection<RMContainer> toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();

    List<RMContainerDTO> toPersist =
        new ArrayList<RMContainerDTO>();
    for (RMContainer hop : toAdd) {
      toPersist.add(createPersistable(hop, session));
    }

    session.savePersistentAll(toPersist);
  }

  @Override
  public void removeAll(Collection<RMContainer> toRemove)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<RMContainerDTO> toPersist = new ArrayList<RMContainerDTO>(toRemove.
        size());
    for (RMContainer hop : toRemove) {
      toPersist.add(session.
          newInstance(RMContainerClusterJ.RMContainerDTO.class, hop.
              getContainerIdID()));
    }
    session.deletePersistentAll(toPersist);
  }

  @Override
  public void add(RMContainer rmcontainer) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(rmcontainer, session));
  }

  private RMContainer createHopRMContainer(RMContainerDTO rMContainerDTO) {

    return new RMContainer(rMContainerDTO.getcontaineridid(),
        rMContainerDTO.getappattemptidid(),
            rMContainerDTO.getnodeidid(),
            rMContainerDTO.getuser(),
            rMContainerDTO.getreservednodeid(),
            rMContainerDTO.getreservedpriority(),
            rMContainerDTO.getreservedmemory(),
            rMContainerDTO.getreservedvcores(),
            rMContainerDTO.getstarttime(),
            rMContainerDTO.getfinishtime(),
            rMContainerDTO.getstate(),
        rMContainerDTO.getfinishedstatusstate(),
        rMContainerDTO.getexitstatus());

  }

  private RMContainerDTO createPersistable(RMContainer hop, HopsSession session)
      throws StorageException {
    RMContainerClusterJ.RMContainerDTO rMContainerDTO =
        session.newInstance(RMContainerClusterJ.RMContainerDTO.class);

    rMContainerDTO.setcontaineridid(hop.getContainerIdID());
    rMContainerDTO.setappattemptidid(hop.getApplicationAttemptIdID());
    rMContainerDTO.setnodeidid(hop.getNodeIdID());
    rMContainerDTO.setuser(hop.getUser());
    rMContainerDTO.setstarttime(hop.getStarttime());
    rMContainerDTO.setfinishtime(hop.getFinishtime());
    rMContainerDTO.setstate(hop.getState());
    rMContainerDTO.setfinishedstatusstate(hop.getFinishedStatusState());
    rMContainerDTO.setexitstatus(hop.getExitStatus());
    rMContainerDTO.setreservednodeid(hop.getReservedNodeIdID());
    rMContainerDTO.setreservedpriority(hop.getReservedPriorityID());
    rMContainerDTO.setreservedmemory(hop.getReservedMemory());
    rMContainerDTO.setreservedvcores(hop.getReservedVCores());

    return rMContainerDTO;
  }

  private Map<String, RMContainer> createMap(List<RMContainerDTO> results) {
    Map<String, RMContainer> map = new HashMap<String, RMContainer>();
    for (RMContainerDTO dto : results) {
      RMContainer hop = createHopRMContainer(dto);
      map.put(hop.getContainerIdID(), hop);
    }
    return map;
  }
}
