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
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.capacity.CSQueueDataAccess;
import io.hops.metadata.yarn.entity.capacity.CSQueue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CSQueueClusterJ implements TablesDef.CSQueueTableDef,
        CSQueueDataAccess<CSQueue> {

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface CSQueueDTO {

    @PrimaryKey
    @Column(name = PATH)
    String getpath();

    void setpath(String path);

    @Column(name = NAME)
    String getqueuename();

    void setqueuename(String queuename);

    @Column(name = USED_CAPACITY)
    float getusedcapacity();

    void setusedcapacity(float usedcapacity);

    @Column(name = USED_RESOURCE_MEMORY)
    int getusedresourcememory();

    void setusedresourcememory(int usedresourcememory);

    @Column(name = USED_RESOURCE_VCORES)
    int getusedresourcevcores();

    void setusedresourcevcores(int usedresourcevcores);

    @Column(name = ABSOLUTE_USED_CAPACITY)
    float getabsoluteusedcapacity();

    void setabsoluteusedcapacity(float absoluteusedcapacity);

    @Column(name = IS_PARENT)
    int getisparent();

    void setisparent(int isparent);

    @Column(name = NUM_CONTAINERS)
    int getnumcontainers();

    void setnumcontainers(int numcontainers);

  }

  @Override
  public CSQueue findById(String id) throws StorageException {
    HopsSession session = connector.obtainSession();

    CSQueueDTO csQueueDTO = null;
    if (session != null) {
      csQueueDTO = session.find(CSQueueDTO.class, id);
    }
    CSQueue result = createCSQueue(csQueueDTO);
    session.release(csQueueDTO);
    return result;
  }

  @Override
  public Map<String, CSQueue> getAll() throws StorageException, IOException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<CSQueueClusterJ.CSQueueDTO> dobj = qb.
            createQueryDefinition(CSQueueClusterJ.CSQueueDTO.class);
    HopsQuery<CSQueueClusterJ.CSQueueDTO> query = session.createQuery(dobj);
    List<CSQueueClusterJ.CSQueueDTO> queryResults = query.getResultList();

    Map<String, CSQueue> result = createCSQueueMap(queryResults);
    session.release(queryResults);
    return result;
  }

  private Map<String,CSQueue> createCSQueueMap(List<CSQueueClusterJ.CSQueueDTO> list)
          throws IOException {
    Map<String, CSQueue> csQueueMap = new HashMap<String,CSQueue>();
    for (CSQueueClusterJ.CSQueueDTO persistable : list) {
      if (persistable != null) {
        CSQueue queue = createCSQueue(persistable);
        csQueueMap.put(queue.getPath(),queue);
      }
    }
    return csQueueMap;
  }

  @Override
  public void addAll(Collection<CSQueue> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    if (modified != null) {
      List<CSQueueDTO> toModify = new ArrayList<CSQueueDTO>(modified.size());
      for (CSQueue hop : modified) {
        toModify.add(createPersistable(hop, session));
      }
      session.savePersistentAll(toModify);
      session.release(toModify);
    }
  }

  @Override
  public void removeAll(Collection<CSQueue> removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    if (removed != null) {
      List<CSQueueDTO> toRemove = new ArrayList<CSQueueDTO>(removed.size());
      for (CSQueue hop : removed) {
        toRemove.add(createPersistable(hop, session));
      }
      session.deletePersistentAll(toRemove);
      session.release(toRemove);
    }
  }

  @Override
  public void createCSQueue(CSQueue csqueue) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(csqueue, session));
  }

  private CSQueue createCSQueue(CSQueueDTO csQueueDTO) {
    if (csQueueDTO == null) {
      return null;
    } else {
      return new CSQueue(
              csQueueDTO.getqueuename(),
              csQueueDTO.getpath(),
              csQueueDTO.getusedcapacity(),
              csQueueDTO.getusedresourcememory(),
              csQueueDTO.getusedresourcevcores(),
              csQueueDTO.getabsoluteusedcapacity(),
              (csQueueDTO.getisparent() == 1),
              csQueueDTO.getnumcontainers()
      );
    }
  }

  private CSQueueDTO createPersistable(CSQueue hop, HopsSession session) throws
          StorageException {
    CSQueueClusterJ.CSQueueDTO csQueueDTO = session.newInstance(
            CSQueueClusterJ.CSQueueDTO.class);

    csQueueDTO.setpath(hop.getPath());
    csQueueDTO.setqueuename(hop.getName());
    csQueueDTO.setusedcapacity(hop.getUsedCapacity());
    csQueueDTO.setusedresourcememory(hop.getUsedResourceMemory());
    csQueueDTO.setusedresourcevcores(hop.getUsedResourceVCores());
    csQueueDTO.setabsoluteusedcapacity(hop.getAbsoluteUsedCapacity());
    csQueueDTO.setisparent(hop.isParent() ? 1 : 0);
    csQueueDTO.setnumcontainers(hop.getNumContainers());

    return csQueueDTO;
  }
}
