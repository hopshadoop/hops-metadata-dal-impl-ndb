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
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.PendingBlockDataAccess;
import io.hops.metadata.hdfs.entity.PendingBlockInfo;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsPredicateOperand;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PendingBlockClusterj
    implements TablesDef.PendingBlockTableDef, PendingBlockDataAccess<PendingBlockInfo> {

  @Override
  public int countValidPendingBlocks(long timeLimit) throws StorageException {
    return MySQLQueryHelper.countWithCriterion(TABLE_NAME,
        String.format("%s>%d", TIME_STAMP, timeLimit));
  }

  @Override
  public List<PendingBlockInfo> findByINodeId(int inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<PendingBlockDTO> qdt =
        qb.createQueryDefinition(PendingBlockDTO.class);

    HopsPredicate pred1 = qdt.get("iNodeId").equal(qdt.param("idParam"));
    qdt.where(pred1);

    HopsQuery<PendingBlockDTO> query = session.createQuery(qdt);
    query.setParameter("idParam", inodeId);

    return createList(query.getResultList());
  }

  @Override
  public List<PendingBlockInfo> findByINodeIds(int[] inodeIds)
      throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<PendingBlockDTO> qdt =
        qb.createQueryDefinition(PendingBlockDTO.class);

    HopsPredicate pred1 = qdt.get("iNodeId").in(qdt.param("idParam"));
    qdt.where(pred1);

    HopsQuery<PendingBlockDTO> query = session.createQuery(qdt);
    query.setParameter("idParam", Ints.asList(inodeIds));

    return createList(query.getResultList());
  }

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = INODE_ID)
  public interface PendingBlockDTO {

    @PrimaryKey
    @Column(name = INODE_ID)
    int getINodeId();

    void setINodeId(int inodeId);
    
    @PrimaryKey
    @Column(name = BLOCK_ID)
    long getBlockId();

    void setBlockId(long blockId);

    @Column(name = TIME_STAMP)
    long getTimestamp();

    void setTimestamp(long timestamp);

    @Column(name = NUM_REPLICAS_IN_PROGRESS)
    int getNumReplicasInProgress();

    void setNumReplicasInProgress(int numReplicasInProgress);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void prepare(Collection<PendingBlockInfo> removed,
      Collection<PendingBlockInfo> newed, Collection<PendingBlockInfo> modified)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<PendingBlockDTO> changes = new ArrayList<PendingBlockDTO>();
    List<PendingBlockDTO> deletions = new ArrayList<PendingBlockDTO>();
    for (PendingBlockInfo p : newed) {
      PendingBlockDTO pTable = session.newInstance(PendingBlockDTO.class);
      createPersistableHopPendingBlockInfo(p, pTable);
      changes.add(pTable);
    }
    for (PendingBlockInfo p : modified) {
      PendingBlockDTO pTable = session.newInstance(PendingBlockDTO.class);
      createPersistableHopPendingBlockInfo(p, pTable);
      changes.add(pTable);
    }

    for (PendingBlockInfo p : removed) {
      PendingBlockDTO pTable = session.newInstance(PendingBlockDTO.class);
      createPersistableHopPendingBlockInfo(p, pTable);
      deletions.add(pTable);
    }
    session.deletePersistentAll(deletions);
    session.savePersistentAll(changes);
  }

  @Override
  public PendingBlockInfo findByPKey(long blockId, int inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    Object[] pk = new Object[2];
    pk[0] = inodeId;
    pk[1] = blockId;

    PendingBlockDTO pendingTable = session.find(PendingBlockDTO.class, pk);
    PendingBlockInfo pendingBlock = null;
    if (pendingTable != null) {
      pendingBlock = createHopPendingBlockInfo(pendingTable);
    }

    return pendingBlock;
  }

  @Override
  public List<PendingBlockInfo> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQuery<PendingBlockDTO> query =
        session.createQuery(qb.createQueryDefinition(PendingBlockDTO.class));
    return createList(query.getResultList());
  }

  @Override
  public List<PendingBlockInfo> findByTimeLimitLessThan(long timeLimit)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<PendingBlockDTO> qdt =
        qb.createQueryDefinition(PendingBlockDTO.class);
    HopsPredicateOperand predicateOp = qdt.get("timestamp");
    String paramName = "timelimit";
    HopsPredicateOperand param = qdt.param(paramName);
    HopsPredicate lessThan = predicateOp.lessThan(param);
    qdt.where(lessThan);
    HopsQuery query = session.createQuery(qdt);
    query.setParameter(paramName, timeLimit);
    return createList(query.getResultList());
  }

  @Override
  public void removeAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistentAll(PendingBlockDTO.class);
  }

  private List<PendingBlockInfo> createList(Collection<PendingBlockDTO> dtos) {
    List<PendingBlockInfo> list = new ArrayList<PendingBlockInfo>();
    for (PendingBlockDTO dto : dtos) {
      list.add(createHopPendingBlockInfo(dto));
    }
    return list;
  }

  private PendingBlockInfo createHopPendingBlockInfo(
      PendingBlockDTO pendingTable) {
    return new PendingBlockInfo(pendingTable.getBlockId(),
        pendingTable.getINodeId(), pendingTable.getTimestamp(),
        pendingTable.getNumReplicasInProgress());
  }

  private void createPersistableHopPendingBlockInfo(
      PendingBlockInfo pendingBlock, PendingBlockDTO pendingTable) {
    pendingTable.setBlockId(pendingBlock.getBlockId());
    pendingTable.setNumReplicasInProgress(pendingBlock.getNumReplicas());
    pendingTable.setTimestamp(pendingBlock.getTimeStamp());
    pendingTable.setINodeId(pendingBlock.getInodeId());
  }
}
