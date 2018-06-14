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

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.ReplicaDataAccess;
import io.hops.metadata.hdfs.entity.Replica;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReplicaClusterj
    implements TablesDef.ReplicaTableDef, ReplicaDataAccess<Replica> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = INODE_ID)
  @Index(name = "storage_idx")
  public interface ReplicaDTO {

    @PrimaryKey
    @Column(name = INODE_ID)
    int getINodeId();

    void setINodeId(int inodeID);
    
    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long bid);

    @PrimaryKey
    @Column(name = STORAGE_ID)
    int getStorageId();

    void setStorageId(int id);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<Replica> findReplicasById(long blockId, int inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaDTO> dobj =
        qb.createQueryDefinition(ReplicaDTO.class);
    HopsPredicate pred1 = dobj.get("blockId").equal(dobj.param("blockIdParam"));
    HopsPredicate pred2 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
    dobj.where(pred1.and(pred2));
    HopsQuery<ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("blockIdParam", blockId);
    query.setParameter("iNodeIdParam", inodeId);
    return convertAndRelease(session, query.getResultList());
  }
  
  
  @Override
  public List<Replica> findReplicasByINodeId(int inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaDTO> dobj =
        qb.createQueryDefinition(ReplicaDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
    dobj.where(pred1);
    HopsQuery<ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("iNodeIdParam", inodeId);
    return convertAndRelease(session, query.getResultList());
  }
  

  @Override
  public List<Replica> findReplicasByINodeIds(int[] inodeIds)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaDTO> dobj =
        qb.createQueryDefinition(ReplicaDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").in(dobj.param("iNodeIdParam"));
    dobj.where(pred1);
    HopsQuery<ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("iNodeIdParam", Ints.asList(inodeIds));
    return convertAndRelease(session, query.getResultList());
  }
  
  @Override
  public Map<Long,Integer> findBlockAndInodeIdsByStorageId(int storageId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ReplicaDTO> res = getReplicas(session, storageId);
    Map<Long,Integer> map = new HashMap<>();
    for(ReplicaDTO dto : res){
      map.put(dto.getBlockId(), dto.getINodeId() );
    }
    session.release(res);
    return map;
  }
  
  @Override
  public Map<Long, Integer> findBlockAndInodeIdsByStorageIdAndBucketId(
      int storageId, int bucketId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaDTO> dobj =
        qb.createQueryDefinition(ReplicaDTO.class);
    HopsPredicate pred1 = dobj.get("storageId").equal(dobj.param
        ("storageIdParam"));
    HopsPredicate pred2 = dobj.get("hashBucket").equal(dobj.param
        ("bucketIdParam"));
    dobj.where(pred1.and(pred2));
    HopsQuery<ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("storageIdParam", storageId);
    query.setParameter("bucketIdParam", bucketId);
  
    List<Replica> replicas = convertAndRelease(session, query.getResultList());
    Map<Long, Integer> result = new HashMap<>();
    for (Replica replica: replicas){
      result.put(replica.getBlockId(), replica.getInodeId());
    }
    return result;
  }
  
  @Override
  public void prepare(Collection<Replica> removed,
      Collection<Replica> newed, Collection<Replica> modified)
      throws StorageException {
    List<ReplicaDTO> changes = new ArrayList<>();
    List<ReplicaDTO> deletions = new ArrayList<>();
    HopsSession session = connector.obtainSession();
    try {
      for (Replica replica : removed) {
        ReplicaDTO newInstance = session.newInstance(ReplicaDTO.class);
        createPersistable(replica, newInstance);
        deletions.add(newInstance);
      }

      for (Replica replica : newed) {
        ReplicaDTO newInstance = session.newInstance(ReplicaDTO.class);
        createPersistable(replica, newInstance);
        changes.add(newInstance);
      }

      for (Replica replica : modified) {
        ReplicaDTO newInstance = session.newInstance(ReplicaDTO.class);
        createPersistable(replica, newInstance);
        changes.add(newInstance);
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    }finally {
      session.release(deletions);
      session.release(changes);
    }
  }
  
  @Override
  public int countAllReplicasForStorageId(int sid) throws StorageException {
    return MySQLQueryHelper.countWithCriterion(TABLE_NAME,
        String.format("%s=%d", STORAGE_ID, sid));
  }

  protected static Set<Long> getReplicas(int storageId) throws
      StorageException {
    return MySQLQueryHelper.execute(String.format("SELECT %s " +
        "FROM %s WHERE %s='%d'", BLOCK_ID, TABLE_NAME, STORAGE_ID, storageId)
        , new MySQLQueryHelper.ResultSetHandler<Set<Long>>() {
      @Override
      public Set<Long> handle(ResultSet result) throws SQLException {
        Set<Long> blocks = Sets.newHashSet();
        while (result.next()){
          blocks.add(result.getLong(BLOCK_ID));
        }
        return blocks;
      }
    });
  }

  protected static List<ReplicaClusterj.ReplicaDTO> getReplicas(
      HopsSession session, int storageId) throws StorageException {
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaDTO> dobj =
        qb.createQueryDefinition(ReplicaClusterj.ReplicaDTO.class);
    dobj.where(dobj.get("storageId").equal(dobj.param("param")));
    HopsQuery<ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("param", storageId);
    return query.getResultList();
  }


  private List<Replica> convertAndRelease(HopsSession session,
      List<ReplicaDTO> triplets) throws StorageException {
    List<Replica> replicas =
        new ArrayList<>(triplets.size());
    for (ReplicaDTO t : triplets) {
        replicas.add(
            new Replica(t.getStorageId(), t.getBlockId(), t.getINodeId()));
      session.release(t);
    }
    return replicas;
  }

  private void createPersistable(Replica replica,
      ReplicaDTO newInstance) {
    newInstance.setBlockId(replica.getBlockId());
    newInstance.setStorageId(replica.getStorageId());
    newInstance.setINodeId(replica.getInodeId());
  }
}
