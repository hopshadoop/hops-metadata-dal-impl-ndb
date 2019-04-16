/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.metadata.ndb.dalimpl.hdfs;

import com.google.common.collect.Lists;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.XAttrDataAccess;
import io.hops.metadata.hdfs.entity.StoredXAttr;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class XAttrClusterJ implements TablesDef.XAttrTableDef,
    XAttrDataAccess<StoredXAttr, StoredXAttr.PrimaryKey> {
  
  static final Logger LOG = Logger.getLogger(VariableClusterj.class);
  
  @PersistenceCapable(table = TABLE_NAME)
  public interface XAttrDTO {
    
    @PrimaryKey
    @Column(name = INODE_ID)
    long getINodeId();
    
    void setINodeId(long inodeId);
  
    @PrimaryKey
    @Column(name = NAMESPACE)
    byte getNamespace();
  
    void setNamespace(byte id);
  
    @PrimaryKey
    @Column(name = NAME)
    String getName();
    
    void setName(String name);
  
    @Column(name = VALUE)
    String getValue();
  
    void setValue(String value);
  }
  
  private ClusterjConnector connector = ClusterjConnector.getInstance();
  
  @Override
  public List<StoredXAttr> getXAttrsByPrimaryKeyBatch(
      List<StoredXAttr.PrimaryKey> pks) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<XAttrDTO> dtos = Lists.newArrayListWithExpectedSize(pks.size());
    try {
      for (StoredXAttr.PrimaryKey pk : pks) {
        XAttrDTO dto = session.newInstance(XAttrDTO.class,
            new Object[]{pk.getInodeId(), pk.getNamespace(), pk.getName()});
        session.load(dto);
        dtos.add(dto);
      }
      session.flush();
      return convertAndCheck(session, dtos);
    }finally {
      session.release(dtos);
    }
  }
  
  @Override
  public Collection<StoredXAttr> getXAttrsByInodeId(long inodeId) throws StorageException{
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<XAttrDTO> dobj =
        qb.createQueryDefinition(XAttrDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").equal(dobj.param("idParam"));
    dobj.where(pred1);
  
    HopsQuery<XAttrDTO> query = session.createQuery(dobj);
    query.setParameter("idParam", inodeId);
  
    List<XAttrDTO> results = null;
    try {
      results = query.getResultList();
      if (results.isEmpty()) {
        return null;
      }
      return convert(results);
    }finally {
      session.release(results);
    }
  
  }
  
  @Override
  public int removeXAttrsByInodeId(long inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<XAttrDTO> dobj =
        qb.createQueryDefinition(XAttrDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").equal(dobj.param("idParam"));
    dobj.where(pred1);
  
    HopsQuery<XAttrDTO> query = session.createQuery(dobj);
    query.setParameter("idParam", inodeId);
    return query.deletePersistentAll();
  }
  
  @Override
  public void prepare(Collection<StoredXAttr> removed, Collection<StoredXAttr> newed,
      Collection<StoredXAttr> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<XAttrDTO> changes = new ArrayList<>();
    List<XAttrDTO> deletions = new ArrayList<>();
    try {
      for (StoredXAttr xattr : removed) {
        XAttrDTO persistable = createPersistable(session, xattr);
        deletions.add(persistable);
      }
    
      for (StoredXAttr xattr : newed) {
        XAttrDTO persistable = createPersistable(session, xattr);
        changes.add(persistable);
      }
    
      for (StoredXAttr xattr : modified) {
        XAttrDTO persistable = createPersistable(session, xattr);
        changes.add(persistable);
      }
      
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    }finally {
      session.release(deletions);
      session.release(changes);
    }
  }
  
  private XAttrDTO createPersistable(HopsSession session, StoredXAttr xattr)
      throws StorageException {
    XAttrDTO dto = session.newInstance(XAttrDTO.class);
    dto.setINodeId(xattr.getInodeId());
    dto.setNamespace(xattr.getNamespace());
    dto.setName(xattr.getName());
    dto.setValue(xattr.getValue() == null ? "" : xattr.getValue());
    return dto;
  }
  
  private List<StoredXAttr> convert(List<XAttrDTO> dtos){
    List<StoredXAttr> results = Lists.newArrayListWithExpectedSize(dtos.size());
    for(XAttrDTO dto : dtos){
      results.add(convert(dto));
    }
    return results;
  }
  
  private List<StoredXAttr> convertAndCheck(HopsSession session,
      List<XAttrDTO> dtos) throws StorageException {
    List<StoredXAttr> results = Lists.newArrayListWithExpectedSize(dtos.size());
    for(XAttrDTO dto : dtos){
      //check if the row exists, default value is empty string
      if(dto.getValue() != null) {
        results.add(convert(dto));
      }
    }
    return results;
  }
  
  private StoredXAttr convert(XAttrDTO dto){
    return new StoredXAttr(dto.getINodeId(), dto.getNamespace(), dto.getName(),
        dto.getValue().isEmpty() ? null : dto.getValue());
  }
  
}
