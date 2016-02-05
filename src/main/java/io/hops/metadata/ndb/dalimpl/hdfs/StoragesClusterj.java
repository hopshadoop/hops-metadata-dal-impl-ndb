/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.StorageDataAccess;
import io.hops.metadata.hdfs.entity.AccessTimeLogEntry;
import io.hops.metadata.hdfs.entity.Storage;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StoragesClusterj implements TablesDef.StoragesTableDef,
    StorageDataAccess<Storage> {

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface StorageDto {
    @PrimaryKey
    @Column(name = STORAGE_ID)
    int getStorageId();

    void setStorageId(int storageId);

    @Column(name = HOST_ID)
    String getHostId();

    void setHostId(String hostId);

    @Column(name = STORAGE_TYPE)
    int getStorageType();

    void setStorageType(int storageType);
  }

  @Override
  public void prepare(Collection<Storage> collection,
      Collection<Storage> collection1) throws StorageException {

  }

  @Override
  public Storage find(int storageId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    Object[] key = new Object[1];
    key[0] = storageId;
    StorageDto dto = session.find(StorageDto.class, key);
    Storage storage = create(dto);
    return storage;
  }

  @Override
  public List<Storage> findByHostUuid(String uuid) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<StorageDto> dobj =
        qb.createQueryDefinition(StorageDto.class);

    HopsPredicate pred1 = dobj.get("host_id").equal(dobj.param("hostId"));

    dobj.where(pred1);

    HopsQuery<StorageDto> query = session.createQuery(dobj);
    query.setParameter("hostId", uuid);

    return convertAndRelease(session, query.getResultList());
  }

  /**
   * Convert a list of storageDTO's into storages
   */
  private List<Storage> convertAndRelease(HopsSession session,
      List<StorageDto> dtos) throws StorageException {
    ArrayList<Storage> list = new ArrayList<Storage>(dtos.size());

    for (StorageDto dto : dtos) {
      list.add(create(dto));
      session.release(dto);
    }

    return list;
  }

  private StorageDto createPersistable(Storage storage,
      HopsSession session) throws StorageException {
    StorageDto dto = session.newInstance(StorageDto.class);
    dto.setStorageId(storage.getStorageID());
    dto.setHostId(storage.getHostID());
    dto.setStorageType(storage.getStorageType());
    return dto;
  }

  private Storage create(StorageDto dto) {
    Storage storage = new Storage(
        dto.getStorageId(),
        dto.getHostId(),
        dto.getStorageType());
    return storage;
  }
}
