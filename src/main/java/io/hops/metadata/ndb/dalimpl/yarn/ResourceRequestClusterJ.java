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
import io.hops.metadata.yarn.dal.ResourceRequestDataAccess;
import io.hops.metadata.yarn.entity.ResourceRequest;
import io.hops.util.CompressionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

public class ResourceRequestClusterJ implements
    TablesDef.ResourceRequestTableDef,
    ResourceRequestDataAccess<ResourceRequest> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ResourceRequestDTO {

    @PrimaryKey
    @Column(name = APPSCHEDULINGINFO_ID)
    String getappschedulinginfo_id();

    void setappschedulinginfo_id(String appschedulinginfo_id);

    @Column(name = PRIORITY)
    int getpriority();

    void setpriority(int priority);

    @Column(name = NAME)
    String getname();
    
    void setname(String name);

    @Column(name = RESOURCEREQUESTSTATE)
    byte[] getresourcerequeststate();

    void setresourcerequeststate(byte[] resourcerequeststate);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();


  @Override
  public Map<String, List<ResourceRequest>> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ResourceRequestDTO> dobj =
        qb.createQueryDefinition(ResourceRequestDTO.class);
    HopsQuery<ResourceRequestDTO> query = session.
        createQuery(dobj);
    List<ResourceRequestDTO> queryResults = query.
        getResultList();
    Map<String,List<ResourceRequest>> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public void addAll(Collection<ResourceRequest> toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ResourceRequestDTO> toPersist = new ArrayList<ResourceRequestDTO>();
    for (ResourceRequest req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }
    session.savePersistentAll(toPersist);
    session.release(toPersist);
  }

  @Override
  public void removeAll(Collection<ResourceRequest> toRemove)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ResourceRequestDTO> toPersist = new ArrayList<ResourceRequestDTO>();
    for (ResourceRequest hop : toRemove) {
      Object[] pk = new Object[3];
      pk[0] = hop.getId();
      pk[1] = hop.getPriority();
      pk[2] = hop.getName();
      toPersist.add(session.newInstance(ResourceRequestDTO.class, pk));
    }
    session.deletePersistentAll(toPersist);
    session.release(toPersist);
  }

  private ResourceRequest createHopResourceRequest(
      ResourceRequestDTO resourceRequestDTO) throws StorageException {
    try {
      return new ResourceRequest(resourceRequestDTO.getappschedulinginfo_id(),
          resourceRequestDTO.getpriority(), resourceRequestDTO.getname(),
          CompressionUtils.decompress(resourceRequestDTO.
              getresourcerequeststate()));
    } catch (IOException e) {
      throw new StorageException(e);
    } catch (DataFormatException e) {
      throw new StorageException(e);
    }
  }

  private ResourceRequestDTO createPersistable(ResourceRequest hop,
      HopsSession session) throws StorageException {
    ResourceRequestClusterJ.ResourceRequestDTO resourceRequestDTO = session.
        newInstance(ResourceRequestClusterJ.ResourceRequestDTO.class);

    resourceRequestDTO.setappschedulinginfo_id(hop.getId());
    resourceRequestDTO.setpriority(hop.getPriority());
    resourceRequestDTO.setname(hop.getName());
    try {
      resourceRequestDTO.setresourcerequeststate(CompressionUtils.compress(hop.
          getResourcerequeststate()));
    } catch (IOException e) {
      throw new StorageException(e);
    }
    return resourceRequestDTO;
  }

  private List<ResourceRequest> createResourceRequestList(
      List<ResourceRequestClusterJ.ResourceRequestDTO> results)
      throws StorageException {
    List<ResourceRequest> resourceRequests = new ArrayList<ResourceRequest>();
    for (ResourceRequestClusterJ.ResourceRequestDTO persistable : results) {
      resourceRequests.add(createHopResourceRequest(persistable));
    }
    return resourceRequests;
  }

  private Map<String, List<ResourceRequest>> createMap(
      List<ResourceRequestDTO> results) throws StorageException {
    Map<String, List<ResourceRequest>> map =
        new HashMap<String, List<ResourceRequest>>();
    for (ResourceRequestDTO dto : results) {
      ResourceRequest hop = createHopResourceRequest(dto);
      if (map.get(hop.getId()) == null) {
        map.put(hop.getId(), new ArrayList<ResourceRequest>());
      }
      map.get(hop.getId()).add(hop);
    }
    return map;
  }
}
