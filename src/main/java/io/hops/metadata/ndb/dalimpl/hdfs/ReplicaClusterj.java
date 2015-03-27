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

import com.google.common.primitives.Ints;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.ReplicaDataAccess;
import io.hops.metadata.hdfs.entity.IndexedReplica;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ReplicaClusterj
    implements TablesDef.ReplicaTableDef, ReplicaDataAccess<IndexedReplica> {
  //  static final Log LOG = LogFactory.getLog(ReplicaClusterj.class);
  static final Logger LOG = Logger.getLogger(ReplicaClusterj.class);

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

    @Column(name = REPLICA_INDEX)
    int getIndex();

    void setIndex(int index);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private final static int NOT_FOUND_ROW = -1000;
  
  @Override
  public List<IndexedReplica> findReplicasById(long blockId, int inodeId)
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
    return createReplicaList(query.getResultList());
  }
  
  
  @Override
  public List<IndexedReplica> findReplicasByINodeId(int inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaDTO> dobj =
        qb.createQueryDefinition(ReplicaDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").equal(dobj.param("iNodeIdParam"));
    dobj.where(pred1);
    HopsQuery<ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("iNodeIdParam", inodeId);
    return createReplicaList(query.getResultList());
  }
  

  @Override
  public List<IndexedReplica> findReplicasByINodeIds(int[] inodeIds)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ReplicaDTO> dobj =
        qb.createQueryDefinition(ReplicaDTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").in(dobj.param("iNodeIdParam"));
    dobj.where(pred1);
    HopsQuery<ReplicaDTO> query = session.createQuery(dobj);
    query.setParameter("iNodeIdParam", Ints.asList(inodeIds));
    return createReplicaList(query.getResultList());
  }
  
  @Override
  public List<IndexedReplica> findReplicasByStorageId(int storageId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ReplicaDTO> res = getReplicas(session, storageId);
    //ClusterjConnector.LOG.error("xxxa: got replicas " + res.size() + " in " + (System.currentTimeMillis() - t));
    return createReplicaList(res);
  }

  @Override
  public List<IndexedReplica> findReplicasByPKS(final long[] blockIds,
      final int[] inodeIds, final int[] sids) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ReplicaDTO> dtos = new ArrayList<ReplicaDTO>();
    for (int i = 0; i < blockIds.length; i++) {
      ReplicaDTO newInstance = session.newInstance(ReplicaDTO.class,
          new Object[]{inodeIds[i], blockIds[i], sids[i]});
      newInstance.setIndex(NOT_FOUND_ROW);
      newInstance = session.load(newInstance);
      dtos.add(newInstance);
    }
    session.flush();
    return createReplicaList(dtos);
  }

  
  @Override
  public void prepare(Collection<IndexedReplica> removed,
      Collection<IndexedReplica> newed, Collection<IndexedReplica> modified)
      throws StorageException {
    List<ReplicaDTO> changes = new ArrayList<ReplicaDTO>();
    List<ReplicaDTO> deletions = new ArrayList<ReplicaDTO>();
    HopsSession session = connector.obtainSession();
    for (IndexedReplica replica : removed) {
      ReplicaDTO newInstance = session.newInstance(ReplicaDTO.class);
      createPersistable(replica, newInstance);
      deletions.add(newInstance);
    }

    for (IndexedReplica replica : newed) {
      ReplicaDTO newInstance = session.newInstance(ReplicaDTO.class);
      createPersistable(replica, newInstance);
      changes.add(newInstance);
    }

    for (IndexedReplica replica : modified) {
      ReplicaDTO newInstance = session.newInstance(ReplicaDTO.class);
      createPersistable(replica, newInstance);
      changes.add(newInstance);
    }
    session.deletePersistentAll(deletions);
    session.savePersistentAll(changes);
  }

  @Override
  public int countAllReplicasForStorageId(int sid) throws StorageException {
    return MySQLQueryHelper.countWithCriterion(TABLE_NAME,
        String.format("%s=%d", STORAGE_ID, sid));
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


  private List<IndexedReplica> createReplicaList(List<ReplicaDTO> triplets) {
    List<IndexedReplica> replicas =
        new ArrayList<IndexedReplica>(triplets.size());
    for (ReplicaDTO t : triplets) {
      if (t.getIndex() != NOT_FOUND_ROW) {
        replicas.add(
            new IndexedReplica(t.getBlockId(), t.getStorageId(), t.getINodeId(),
                t.getIndex()));
      }
    }
    return replicas;
  }

  private void createPersistable(IndexedReplica replica,
      ReplicaDTO newInstance) {
    newInstance.setBlockId(replica.getBlockId());
    newInstance.setIndex(replica.getIndex());
    newInstance.setStorageId(replica.getStorageId());
    newInstance.setINodeId(replica.getInodeId());
  }
}
