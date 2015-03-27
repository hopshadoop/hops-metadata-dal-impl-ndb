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
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.entity.Resource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResourceClusterJ
    implements TablesDef.ResourceTableDef, ResourceDataAccess<Resource> {

  private static final Log LOG = LogFactory.getLog(ResourceClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface ResourceDTO {

    @PrimaryKey
    @Column(name = ID)
    String getId();

    void setId(String id);

    @Column(name = TYPE)
    int getType();

    void setType(int type);

    @Column(name = PARENT)
    int getParent();

    void setParent(int parent);

    @Column(name = MEMORY)
    int getMemory();

    void setMemory(int memory);

    @Column(name = VIRTUAL_CORES)
    int getVirtualcores();

    void setVirtualcores(int virtualcores);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Resource findEntry(String id, int type, int parent)
      throws StorageException {
    LOG.debug("HOP :: ClusterJ Resource.findEntry - START:" + id);
    HopsSession session = connector.obtainSession();
    if (session != null) {
      ResourceDTO resourceDTO;
      Object[] pk = new Object[3];
      pk[0] = id;
      pk[1] = type;
      pk[2] = parent;
      resourceDTO = session.find(ResourceDTO.class, pk);
      LOG.debug("HOP :: ClusterJ Resource.findEntry - FINISH:" + id);
      if (resourceDTO != null) {
        return createHopResource(resourceDTO);
      }
    }
    return null;
  }

  @Override
  public Map<String, Map<Integer, Map<Integer, Resource>>> getAll()
      throws StorageException {
    LOG.debug("HOP :: ClusterJ Resource.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ResourceDTO> dobj =
        qb.createQueryDefinition(ResourceDTO.class);
    HopsQuery<ResourceDTO> query = session.
        createQuery(dobj);
    List<ResourceDTO> results = query.
        getResultList();
    LOG.debug("HOP :: ClusterJ Resource.getAll - FINISH");
    return createMap(results);
  }

  @Override
  public void addAll(Collection<Resource> toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ResourceDTO> toPersist = new ArrayList<ResourceDTO>();
    for (Resource req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }

    session.savePersistentAll(toPersist);
    session.flush();
  }

  @Override
  public void removeAll(Collection<Resource> toRemove) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ResourceDTO> toPersist = new ArrayList<ResourceDTO>();
    for (Resource req : toRemove) {
      Object[] pk = new Object[3];
      pk[0] = req.getId();
      pk[1] = req.getType();
      pk[2] = req.getParent();
      toPersist.add(session.newInstance(ResourceDTO.class, pk));
    }
    session.deletePersistentAll(toPersist);
    session.flush();
  }

  @Override
  public void add(Resource resourceNode) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(resourceNode, session));
  }

  private Resource createHopResource(ResourceDTO resourceDTO) {
    if (resourceDTO == null) {
      return null;

    }
    return new Resource(resourceDTO.getId(), resourceDTO.getType(),
        resourceDTO.getParent(), resourceDTO.getMemory(), resourceDTO.
        getVirtualcores());
  }

  private ResourceDTO createPersistable(Resource resource, HopsSession session)
      throws StorageException {
    ResourceDTO resourceDTO = session.newInstance(ResourceDTO.class);
    resourceDTO.setId(resource.getId());
    resourceDTO.setType(resource.getType());
    resourceDTO.setParent(resource.getParent());
    resourceDTO.setMemory(resource.getMemory());
    resourceDTO.setVirtualcores(resource.getVirtualCores());
    return resourceDTO;
  }

  private Map<String, Map<Integer, Map<Integer, Resource>>> createMap(
      List<ResourceDTO> results) {
    Map<String, Map<Integer, Map<Integer, Resource>>> map =
        new HashMap<String, Map<Integer, Map<Integer, Resource>>>();
    for (ResourceDTO dto : results) {
      Resource hop = createHopResource(dto);
      if (map.get(hop.getId()) == null) {
        map.put(hop.getId(), new HashMap<Integer, Map<Integer, Resource>>());
      }
      Map<Integer, Map<Integer, Resource>> inerMap = map.get(hop.getId());
      if (inerMap.get(hop.getType()) == null) {
        inerMap.put(hop.getType(), new HashMap<Integer, Resource>());
      }
      inerMap.get(hop.getType()).put(hop.getParent(), hop);
    }
    return map;
  }
}
