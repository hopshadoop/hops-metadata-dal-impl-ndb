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
import io.hops.metadata.yarn.dal.ContainerDataAccess;
import io.hops.metadata.yarn.entity.Container;
import io.hops.util.CompressionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

public class ContainerClusterJ
    implements TablesDef.ContainerTableDef, ContainerDataAccess<Container> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ContainerDTO {

    @PrimaryKey
    @Column(name = CONTAINERID_ID)
    String getcontainerid();

    void setcontainerid(String containerid);

    @Column(name = CONTAINERSTATE)
    byte[] getcontainerstate();

    void setcontainerstate(byte[] containerstate);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Map<String, Container> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ContainerDTO> dobj =
        qb.createQueryDefinition(ContainerDTO.class);
    HopsQuery<ContainerDTO> query = session.
        createQuery(dobj);
    List<ContainerDTO> queryResults = query.
        getResultList();
    Map<String, Container> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public void addAll(Collection<Container> toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerDTO> toPersist = new ArrayList<ContainerDTO>();
    for (Container container : toAdd) {
      ContainerDTO persistable = createPersistable(container, session);
      toPersist.add(persistable);
    }
    session.savePersistentAll(toPersist);
    session.release(toPersist);
  }

  @Override
  public void removeAll(Collection<Container> toRemove) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainerDTO> toPersist = new ArrayList<ContainerDTO>();
    for (Container container : toRemove) {
      ContainerDTO persistable = createPersistable(container, session);
      toPersist.add(persistable);
    }
    session.deletePersistentAll(toPersist);
    session.release(toPersist);
  }
  
  @Override
  public void createContainer(Container container) throws StorageException {
    HopsSession session = connector.obtainSession();
    ContainerDTO persistable = createPersistable(container, session);
    session.savePersistent(persistable);
    session.release(persistable);
  }

  private Container createHopContainer(ContainerDTO containerDTO)
      throws StorageException {
    Container hop = null;
    try {
      hop = new Container(containerDTO.getcontainerid(),
          CompressionUtils.decompress(containerDTO.getcontainerstate()));
    } catch (IOException e) {
      throw new StorageException(e);
    } catch (DataFormatException e) {
      throw new StorageException(e);
    }
    return hop;
  }

  private ContainerDTO createPersistable(Container hopContainer,
      HopsSession session) throws StorageException {
    ContainerDTO containerDTO = session.newInstance(ContainerDTO.class);
    containerDTO.setcontainerid(hopContainer.getContainerId());
    if (hopContainer.getContainerState() != null) {
        try {
            containerDTO.setcontainerstate(CompressionUtils.compress(hopContainer.
                    getContainerState()));
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }
    return containerDTO;
  }

  private Map<String, Container> createMap(List<ContainerDTO> results)
      throws StorageException {

    Map<String, Container> map = new HashMap<String, Container>();
    for (ContainerDTO dto : results) {
      Container hop = createHopContainer(dto);
      map.put(hop.getContainerId(), hop);
    }
    return map;
  }
}
