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
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.entity.INode;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.ProjectedINode;
import io.hops.metadata.hdfs.snapshots.SnapShotConstants;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.NdbBoolean;
import io.hops.metadata.ndb.mysqlserver.HopsSQLExceptionHelper;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class INodeClusterj implements TablesDef.INodeTableDef, INodeDataAccess<INode> {

  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = PARENT_ID)
  public interface InodeDTO {
    @Column(name = ID)
    @Index(name = "inode_idx")
    int getId();     // id of the inode
    void setId(int id);

    @PrimaryKey
    @Column(name = NAME)
    String getName();     //name of the inode
    void setName(String name);

    //id of the parent inode 
    @PrimaryKey
    @Column(name = PARENT_ID)
    @Index(name = "pidex")
    int getParentId();     // id of the inode
    void setParentId(int parentid);

    // Inode
    @Column(name = MODIFICATION_TIME)
    long getModificationTime();

    void setModificationTime(long modificationTime);

    // Inode
    @Column(name = ACCESS_TIME)
    long getATime();

    void setATime(long modificationTime);

    // Inode
    @Column(name = PERMISSION)
    byte[] getPermission();

    void setPermission(byte[] permission);

    // InodeFileUnderConstruction
    @Column(name = CLIENT_NAME)
    String getClientName();

    void setClientName(String isUnderConstruction);

    // InodeFileUnderConstruction
    @Column(name = CLIENT_MACHINE)
    String getClientMachine();

    void setClientMachine(String clientMachine);

    @Column(name = CLIENT_NODE)
    String getClientNode();

    void setClientNode(String clientNode);

    //  marker for InodeFile
    @Column(name = GENERATION_STAMP)
    int getGenerationStamp();

    void setGenerationStamp(int generation_stamp);

    // InodeFile
    @Column(name = HEADER)
    long getHeader();

    void setHeader(long header);

    //INodeSymlink
    @Column(name = SYMLINK)
    String getSymlink();

    void setSymlink(String symlink);

    @Column(name = QUOTA_ENABLED)
    byte getQuotaEnabled();

    void setQuotaEnabled(byte quotaEnabled);

    @Column(name = UNDER_CONSTRUCTION)
    boolean getUnderConstruction();

    void setUnderConstruction(boolean underConstruction);

    @Column(name = SUBTREE_LOCKED)
    byte getSubtreeLocked();

    void setSubtreeLocked(byte locked);

    @Column(name = SUBTREE_LOCK_OWNER)
    long getSubtreeLockOwner();

    void setSubtreeLockOwner(long leaderId);

    @Column(name = META_ENABLED)
    byte getMetaEnabled();

    void setMetaEnabled(byte metaEnabled);

    @Column(name = SIZE)
    long getSize();

    void setSize(long size);

    @Column(name = ISDELETED)
    int getIsDeleted();

    void setIsDeleted(int isdeleted);

    @Column(name = STATUS)
    int getStatus();

    void setStatus(int Status);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private MysqlServerConnector mysqlConnector =
      MysqlServerConnector.getInstance();
  private final static int NOT_FOUND_ROW = -1000;
  
  @Override
  public void prepare(Collection<INode> removed, Collection<INode> newEntries,
      Collection<INode> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<InodeDTO> changes = new ArrayList<InodeDTO>();
    List<InodeDTO> deletions = new ArrayList<InodeDTO>();
    for (INode inode : removed) {
      Object[] pk = new Object[2];
      pk[0] = inode.getParentId();
      pk[1] = inode.getName();
      InodeDTO persistable = session.newInstance(InodeDTO.class, pk);
      deletions.add(persistable);
    }

    for (INode inode : newEntries) {
      InodeDTO persistable = session.newInstance(InodeDTO.class);
      createPersistable(inode, persistable);
      changes.add(persistable);
    }

    for (INode inode : modified) {
      InodeDTO persistable = session.newInstance(InodeDTO.class);
      createPersistable(inode, persistable);
      changes.add(persistable);
    }
    session.deletePersistentAll(deletions);
    session.savePersistentAll(changes);
    
    session.release(deletions);
    session.release(changes);
  }

  @Override
  public INode indexScanfindInodeById(int inodeId) throws StorageException {
    //System.out.println("*** pruneScanfindInodeById, Id "+inodeId);
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InodeDTO> dobj =
        qb.createQueryDefinition(InodeDTO.class);
    HopsPredicate pred1 = dobj.get("id").equal(dobj.param("idParam"));
    dobj.where(pred1);

    HopsQuery<InodeDTO> query = session.createQuery(dobj);
    query.setParameter("idParam", inodeId);

    List<InodeDTO> results = query.getResultList();

    if (results.size() > 1) {
      throw new StorageException("Only one record was expected");
    }
    
    if (results.size() == 1) {
      INode inode = createInode(results.get(0));
      session.release(results);
      return inode;
    } else {
      return null;
    }
  }

  @Override
  public List<INode> indexScanFindInodesByParentId(int parentId)
      throws StorageException {
    //System.out.println("*** indexScanFindInodesByParentId ");
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InodeDTO> dobj =
        qb.createQueryDefinition(InodeDTO.class);
    HopsPredicate pred1 =
        dobj.get("parentId").equal(dobj.param("parentIDParam"));
      dobj.where(pred1);
    HopsQuery<InodeDTO> query = session.createQuery(dobj);
    query.setParameter("parentIDParam", parentId);

    List<InodeDTO> results = query.getResultList();
    List<INode> inodeList = createInodeList(results);
    session.release(results);
    return inodeList;
  }
    
    @Override
    public List<INode> indexScanFindInodesByParentIdIncludeDeletes(int parentId) throws StorageException {
        try {

            HopsSession session = connector.obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();
            HopsQueryDomainType<InodeDTO> dobj = qb.createQueryDefinition(InodeDTO.class);
            HopsPredicate pred1 = dobj.get("parentId").equal(dobj.param("parentIDParam1"));
            HopsPredicate pred2 = dobj.get("parentId").equal(dobj.param("parentIDParam2"));
            HopsPredicate pred3 = dobj.get("isdeleted").equal(dobj.param("isdeletedParam"));
            dobj.where(pred1.or(pred2.and(pred3)));

            HopsQuery<InodeDTO> query = session.createQuery(dobj);
            query.setParameter("parentIDParam1", parentId);
            query.setParameter("parentIDParam2", -parentId);
            query.setParameter("parentIDParam3", 1);

            List<InodeDTO> results = query.getResultList();
            explain(query);
            return createInodeList(results);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }
    @Override
    public List<ProjectedINode> findInodesByParentIdForSubTreeOpsWithReadLockIncludeDeletes(
            int parentId) throws StorageException {
        final String query = String.format(
                "SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s FROM %s WHERE  %s=%d or (%s=%d and %s=%d) LOCK IN SHARE MODE",
                ID, NAME, PARENT_ID, PERMISSION, HEADER, SYMLINK, QUOTA_ENABLED,
                UNDER_CONSTRUCTION, SUBTREE_LOCKED, SUBTREE_LOCK_OWNER,STATUS,ISDELETED,TABLE_NAME,
                PARENT_ID,PARENT_ID,ISDELETED, parentId,-parentId,SnapShotConstants.isDeleted);
        ArrayList<ProjectedINode> resultList;
        try {
            Connection conn = mysqlConnector.obtainSession();
            PreparedStatement s = conn.prepareStatement(query);
            ResultSet result = s.executeQuery();
            resultList = new ArrayList<ProjectedINode>();

            while (result.next()) {
                resultList.add(
                        new ProjectedINode(result.getInt(ID), result.getInt(PARENT_ID),
                                result.getString(NAME), result.getBytes(PERMISSION),
                                result.getLong(HEADER),
                                result.getString(SYMLINK) == null ? false : true,
                                result.getBoolean(QUOTA_ENABLED),
                                result.getBoolean(UNDER_CONSTRUCTION),
                                result.getBoolean(SUBTREE_LOCKED),
                                result.getLong(SUBTREE_LOCK_OWNER),
                                result.getLong(SIZE),
                                result.getInt(ISDELETED),
                                result.getInt(STATUS)));
            }
        } catch (SQLException ex) {
            throw HopsSQLExceptionHelper.wrap(ex);
        } finally {
            mysqlConnector.closeSession();
        }
        return resultList;
    }
  @Override
  public List<ProjectedINode> findInodesForSubtreeOperationsWithWriteLock(
      int parentId) throws StorageException {
    final String query = String.format(
        "SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s FROM %s WHERE %s=%d FOR UPDATE ",
        ID, NAME, PARENT_ID, PERMISSION, HEADER, SYMLINK, QUOTA_ENABLED,
        UNDER_CONSTRUCTION, SUBTREE_LOCKED, SUBTREE_LOCK_OWNER, SIZE,ISDELETED,STATUS, TABLE_NAME,
        PARENT_ID, parentId);
    ArrayList<ProjectedINode> resultList;
    try {
      Connection conn = mysqlConnector.obtainSession();
      PreparedStatement s = conn.prepareStatement(query);
      ResultSet result = s.executeQuery();
      resultList = new ArrayList<ProjectedINode>();

      while (result.next()) {
        resultList.add(
            new ProjectedINode(result.getInt(ID), result.getInt(PARENT_ID),
                result.getString(NAME), result.getBytes(PERMISSION),
                result.getLong(HEADER),
                result.getString(SYMLINK) == null ? false : true,
                result.getBoolean(QUOTA_ENABLED),
                result.getBoolean(UNDER_CONSTRUCTION),
                result.getBoolean(SUBTREE_LOCKED),
                result.getLong(SUBTREE_LOCK_OWNER),
                result.getLong(SIZE),
                result.getInt(ISDELETED),
                result.getInt(STATUS)));
      }
    } catch (SQLException ex) {
      throw HopsSQLExceptionHelper.wrap(ex);
    } finally {
      mysqlConnector.closeSession();
    }
    return resultList;
  }

  @Override
  public INode pkLookUpFindInodeByNameAndParentId(String name, int parentId)
      throws StorageException {
    HopsSession session = connector.obtainSession();

    Object[] pk = new Object[2];
    pk[0] = parentId;
    pk[1] = name;

    InodeDTO result = session.find(InodeDTO.class, pk);
    if (result != null) {
      INode inode = createInode(result);
      session.release(result);
      return inode;
    } else {
      return null;
    }
  }

  @Override
  public List<INode> getINodesPkBatched(String[] names, int[] parentIds)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<InodeDTO> dtos = new ArrayList<InodeDTO>();
    for (int i = 0; i < names.length; i++) {
      InodeDTO dto = session
          .newInstance(InodeDTO.class, new Object[]{parentIds[i], names[i]});
      dto.setId(NOT_FOUND_ROW);
      dto = session.load(dto);
      dtos.add(dto);
    }
    session.flush();
    List<INode> inodeList = createInodeList(dtos);
    session.release(dtos);
    return inodeList;
  }
  
  @Override
  public List<INodeIdentifier> getAllINodeFiles(long startId, long endId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InodeDTO> dobj =
        qb.createQueryDefinition(InodeDTO.class);
    HopsPredicate pred = dobj.get("header").equal(dobj.param("isDirParam"));
    HopsPredicate pred2 =
        dobj.get("id").between(dobj.param("startId"), dobj.param("endId"));
    dobj.where(pred.not().and(pred2));
    HopsQuery<InodeDTO> query = session.createQuery(dobj);
    query.setParameter("isDirParam", 0L);
    //FIXME: InodeId is integer
    //startId is inclusive and endId exclusive
    query.setParameter("startId", (int) startId);
    query.setParameter("endId", (int) (endId - 1));
    List<InodeDTO> dtos = query.getResultList();
    List<INodeIdentifier> res = new ArrayList<INodeIdentifier>();
    for (InodeDTO dto : dtos) {
      res.add(
          new INodeIdentifier(dto.getId(), dto.getParentId(), dto.getName()));
    }
    session.release(dtos);
    return res;
  }
  
  
  @Override
  public boolean haveFilesWithIdsBetween(long startId, long endId)
      throws StorageException {
    return MySQLQueryHelper.exists(TABLE_NAME, String
        .format("%s<>0 and %s " + "between %d and %d", HEADER, ID, startId,
            (endId - 1)));
  }
  
  @Override
  public boolean haveFilesWithIdsGreaterThan(long id) throws StorageException {
    return MySQLQueryHelper.exists(TABLE_NAME,
        String.format("%s<>0 and " + "%s>%d", HEADER, ID, id));
  }
  
  @Override
  public long getMinFileId() throws StorageException {
    return MySQLQueryHelper
        .minInt(TABLE_NAME, ID, String.format("%s<>0", HEADER));
  }

  @Override
  public long getMaxFileId() throws StorageException {
    return MySQLQueryHelper
        .maxInt(TABLE_NAME, ID, String.format("%s<>0", HEADER));
  }

  @Override
  public int countAllFiles() throws StorageException {
    return MySQLQueryHelper
        .countWithCriterion(TABLE_NAME, String.format("%s<>0", HEADER));
  }
  
  @Override
  public List<INode> allINodes() throws StorageException { // only for testing
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQuery<InodeDTO> query =
        session.createQuery(qb.createQueryDefinition(InodeDTO.class));
    return createInodeList(query.getResultList());
  }
  
  @Override
  public boolean hasChildren(int parentId) throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InodeDTO> dobj =
        qb.createQueryDefinition(InodeDTO.class);
    HopsPredicate pred1 =
        dobj.get("parentId").equal(dobj.param("parentIDParam"));
    dobj.where(pred1);
    HopsQuery<InodeDTO> query = session.createQuery(dobj);
    query.setParameter("parentIDParam", parentId);
    query.setLimits(0, 1);

    List<InodeDTO> results = query.getResultList();
    if(results.isEmpty()){
      return false;
    }else{
      session.release(results);
      return true;
    }
  }

  
  private List<INode> createInodeList(List<InodeDTO> list) {
    List<INode> inodes = new ArrayList<INode>();
    for (InodeDTO persistable : list) {
      if (persistable.getId() != NOT_FOUND_ROW) {
        inodes.add(createInode(persistable));
      }
    }
    return inodes;
  }

  private INode createInode(InodeDTO persistable) {
    INode node = new INode(persistable.getId(), persistable.getName(),
        persistable.getParentId(),
        NdbBoolean.convert(persistable.getQuotaEnabled()),
        persistable.getModificationTime(), persistable.getATime(),
        persistable.getPermission(), persistable.getUnderConstruction(),
        persistable.getClientName(), persistable.getClientMachine(),
        persistable.getClientNode(), persistable.getGenerationStamp(),
        persistable.getHeader(), persistable.getSymlink(),
        NdbBoolean.convert(persistable.getSubtreeLocked()),
        persistable.getSubtreeLockOwner(),
        NdbBoolean.convert(persistable.getMetaEnabled()),
        persistable.getSize(), persistable.getIsDeleted(),persistable.getStatus());
    return node;
  }

  private void createPersistable(INode inode, InodeDTO persistable) {
    persistable.setId(inode.getId());
    persistable.setName(inode.getName());
    persistable.setParentId(inode.getParentId());
    persistable.setQuotaEnabled(NdbBoolean.convert(inode.isDirWithQuota()));
    persistable.setModificationTime(inode.getModificationTime());
    persistable.setATime(inode.getAccessTime());
    persistable.setPermission(inode.getPermission());
    persistable.setUnderConstruction(inode.isUnderConstruction());
    persistable.setClientName(inode.getClientName());
    persistable.setClientMachine(inode.getClientMachine());
    persistable.setClientNode(inode.getClientNode());
    persistable.setGenerationStamp(inode.getGenerationStamp());
    persistable.setHeader(inode.getHeader());
    persistable.setSymlink(inode.getSymlink());
    persistable.setSubtreeLocked(NdbBoolean.convert(inode.isSubtreeLocked()));
    persistable.setSubtreeLockOwner(inode.getSubtreeLockOwner());
    persistable.setMetaEnabled(NdbBoolean.convert(inode.isMetaEnabled()));
    persistable.setSize(inode.getSize());
    persistable.setIsDeleted(inode.getIsDeleted());
    persistable.setStatus(inode.getStatus());
  }

  private void explain(HopsQuery<InodeDTO> query) {
    //      Map<String,Object> map = query.explain();
    //      System.out.println("Explain");
    //      System.out.println("keys " +Arrays.toString(map.keySet().toArray()));
    //      System.out.println("values "+ Arrays.toString(map.values().toArray()));
  }

}
