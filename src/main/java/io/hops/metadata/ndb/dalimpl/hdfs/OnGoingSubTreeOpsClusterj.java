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
package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.OngoingSubTreeOpsDataAccess;
import io.hops.metadata.hdfs.entity.SubTreeOperation;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsPredicateOperand;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class OnGoingSubTreeOpsClusterj
    implements TablesDef.OnGoingSubTreeOpsDef, OngoingSubTreeOpsDataAccess<SubTreeOperation> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column =  PARTITION_ID)
  public interface OnGoingSubTreeOpsDTO {
    @PrimaryKey
    @Column(name = PATH)
    String getPath();
    void setPath(String path);

    @PrimaryKey
    @Column(name = PARTITION_ID)
    int getPartitionId();
    void setPartitionId(int partitionId);

    @Column(name = NAME_NODE_ID)
    long getNamenodeId();
    void setNamenodeId(long namenodeId);
    
    @Column(name = OP_NAME)
    int getOpName();
    void setOpName(int namenodeId);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void prepare(Collection<SubTreeOperation> removed, 
      Collection<SubTreeOperation> newed, Collection<SubTreeOperation> modified)
          throws StorageException {
    
    List<OnGoingSubTreeOpsDTO> changes = new ArrayList<OnGoingSubTreeOpsDTO>();
    List<OnGoingSubTreeOpsDTO> deletions = new ArrayList<OnGoingSubTreeOpsDTO>();
    HopsSession dbSession = connector.obtainSession();
    for (SubTreeOperation ops : newed) {
      OnGoingSubTreeOpsDTO lTable = dbSession.newInstance(OnGoingSubTreeOpsDTO.class);
      createPersistableSubTreeOp(ops, lTable);
      changes.add(lTable);
    }

    for (SubTreeOperation ops : modified) {
      OnGoingSubTreeOpsDTO lTable = dbSession.newInstance(OnGoingSubTreeOpsDTO.class);
      createPersistableSubTreeOp(ops, lTable);
      changes.add(lTable);
    }

    for (SubTreeOperation ops : removed) {
      Object[] key = new Object[2];
      key[0] = getHash(ops.getPath());
      key[1] = ops.getPath();
      OnGoingSubTreeOpsDTO opsTable = dbSession.newInstance(OnGoingSubTreeOpsDTO.class, key);
      deletions.add(opsTable);
    }
    if(!deletions.isEmpty()){
      dbSession.deletePersistentAll(deletions);
      dbSession.release(deletions);
    }
    if(!changes.isEmpty()){
      dbSession.savePersistentAll(changes);
      dbSession.release(changes);
    }
  }
  
  @Override
  public Collection<SubTreeOperation> allOps() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQuery<OnGoingSubTreeOpsDTO> query =
        session.createQuery(qb.createQueryDefinition(OnGoingSubTreeOpsDTO.class));
    return convertAndRelease(session, query.getResultList());
  }

//  @Override
//  public SubTeeOperation findByPKey(String path) throws StorageException {
//    Object[] key = new Object[2];
//    key[0] = path;
//    key[1] = PART_KEY_VAL;
//    HopsSession dbSession = connector.obtainSession();
//    OnGoingSubTreeOpsDTO lPTable = dbSession.find(OnGoingSubTreeOpsDTO.class, key);
//    SubTeeOperation lPath = null;
//    if (lPTable != null) {
//      lPath = convertAndRelease(lPTable);
//    }
//    return lPath;
//  }

  @Override
  public Collection<SubTreeOperation> findByPathsByPrefix(String prefix)
      throws StorageException {

    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType dobj = qb.createQueryDefinition(OnGoingSubTreeOpsDTO.class);
    HopsPredicate pred = dobj.get("partitionId").equal(dobj.param("partitionIDParam"));
    HopsPredicateOperand propertyPredicate = dobj.get("path");
    HopsPredicateOperand propertyLimit = dobj.param("prefix");
    HopsPredicate like = propertyPredicate.like(propertyLimit);
    dobj.where(pred.and(like));
    HopsQuery query = dbSession.createQuery(dobj);
    query.setParameter("partitionIDParam", getHash(prefix));
    query.setParameter("prefix", prefix + "%");
    query.setLimits(0, LIMIT);
    return convertAndRelease(dbSession, query.getResultList());
  }

  private List<SubTreeOperation> convertAndRelease(HopsSession session,
      Collection<OnGoingSubTreeOpsDTO> dtos) throws StorageException {
    List<SubTreeOperation> list = new ArrayList<SubTreeOperation>();
    for (OnGoingSubTreeOpsDTO dto : dtos) {
      list.add(convertAndRelease(session, dto));
    }
    return list;
  }

  private SubTreeOperation convertAndRelease(HopsSession session,
      OnGoingSubTreeOpsDTO opsDto) throws StorageException {
    SubTreeOperation subTreeOperation = new SubTreeOperation(opsDto.getPath(),
        opsDto.getNamenodeId(),SubTreeOperation.StoOperationType.values()
        [opsDto.getOpName()] );
    session.release(opsDto);
    return subTreeOperation;
  }

  private void createPersistableSubTreeOp(SubTreeOperation op,
      OnGoingSubTreeOpsDTO opDto) {
    opDto.setPath(op.getPath());
    opDto.setPartitionId(getHash(op.getPath()));
    opDto.setNamenodeId(op.getNameNodeId());
    opDto.setOpName(op.getOpType().ordinal());
  }

  private static int getHash(String path){
    String[] pathComponents =  PathUtils.getPathNames(path);
    if(pathComponents.length == 1){
      throw new UnsupportedOperationException("Taking sub tree lock on the root is not yet supported ");
    }
    return  pathComponents[1].hashCode();
  }
}
