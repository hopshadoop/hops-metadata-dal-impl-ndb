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
import io.hops.metadata.ndb.dalimpl.ClusterjDataAccess;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.entity.Resource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

public class ResourceClusterJ extends ClusterjDataAccess
    implements TablesDef.ResourceTableDef, ResourceDataAccess<Resource> {

  public ResourceClusterJ(ClusterjConnector connector) {
    super(connector);
  }

  private static final Log LOG = LogFactory.getLog(ResourceClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface ResourceDTO {

    @PrimaryKey
    @Column(name = ID)
    String getId();

    void setId(String id);

    @Column(name = MEMORY)
    int getMemory();

    void setMemory(int memory);

    @Column(name = VIRTUAL_CORES)
    int getVirtualcores();

    void setVirtualcores(int virtualcores);

    @Column(name = PENDING_EVENT_ID)
    int getpendingeventid();

    void setpendingeventid(int pendingid);

  }

  @Override
  public Resource findEntry(String id)
      throws StorageException {
    LOG.debug("HOP :: ClusterJ Resource.findEntry - START:" + id);
    HopsSession session = getConnector().obtainSession();
    ResourceDTO resourceDTO;
    resourceDTO = session.find(ResourceDTO.class, id);
    LOG.debug("HOP :: ClusterJ Resource.findEntry - FINISH:" + id);
    Resource result = null;
    if (resourceDTO != null) {
      result = createHopResource(resourceDTO);
    }
    session.release(resourceDTO);
    return result;
  }

  @Override
  public Map<String, Resource> getAll()
      throws StorageException {
    LOG.debug("HOP :: ClusterJ Resource.getAll - START");
    HopsSession session = getConnector().obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ResourceDTO> dobj =
        qb.createQueryDefinition(ResourceDTO.class);
    HopsQuery<ResourceDTO> query = session.
        createQuery(dobj);
    List<ResourceDTO> queryResults = query.
        getResultList();
    LOG.debug("HOP :: ClusterJ Resource.getAll - FINISH");
    Map<String, Resource> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public void addAll(Collection<Resource> toAdd) throws StorageException {
    HopsSession session = getConnector().obtainSession();
    List<ResourceDTO> toPersist = new ArrayList<ResourceDTO>();
    for (Resource req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }

    session.savePersistentAll(toPersist);
    session.release(toPersist);
  }

  @Override
  public void removeAll(Collection<Resource> toRemove) throws StorageException {
    HopsSession session = getConnector().obtainSession();
    List<ResourceDTO> toPersist = new ArrayList<ResourceDTO>();
    for (Resource req : toRemove) {
      Object[] pk = new Object[3];
      pk[0] = req.getId();
      toPersist.add(session.newInstance(ResourceDTO.class, pk));
    }
    session.deletePersistentAll(toPersist);
    session.release(toPersist);
  }

  @Override
  public void add(Resource resourceNode) throws StorageException {
    HopsSession session = getConnector().obtainSession();
    ResourceDTO dto = createPersistable(resourceNode, session);
    session.savePersistent(dto);
    session.release(dto);
  }

  private Resource createHopResource(ResourceDTO resourceDTO) {
    if (resourceDTO == null) {
      return null;

    }
    return new Resource(resourceDTO.getId(), resourceDTO.getMemory(), resourceDTO.
        getVirtualcores(), resourceDTO.getpendingeventid());
  }

  private ResourceDTO createPersistable(Resource resource, HopsSession session)
      throws StorageException {
    ResourceDTO resourceDTO = session.newInstance(ResourceDTO.class);
    resourceDTO.setId(resource.getId());
    resourceDTO.setMemory(resource.getMemory());
    resourceDTO.setVirtualcores(resource.getVirtualCores());
    resourceDTO.setpendingeventid(resource.getPendingEventId());
    return resourceDTO;
  }

  private Map<String, Resource> createMap(
      List<ResourceDTO> results) {
    Map<String, Resource> map;
    map = new HashMap<>();
    for (ResourceDTO dto : results) {
      Resource hop = createHopResource(dto);
      map.put(hop.getId(), hop);
    }
    return map;
  }
}
