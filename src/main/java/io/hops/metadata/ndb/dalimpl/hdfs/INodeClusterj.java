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

import com.google.common.collect.Lists;
import com.mysql.clusterj.LockMode;
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
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class INodeClusterj implements TablesDef.INodeTableDef, INodeDataAccess<INode> {
//  static final Logger LOG = Logger.getLogger(INodeClusterj.class);
  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = PARTITION_ID)
  public interface InodeDTO {
    @PrimaryKey
    @Column(name = PARTITION_ID)
    int getPartitionId();
    void setPartitionId(int partitionId);

    //id of the parent inode
    @PrimaryKey
    @Column(name = PARENT_ID)
    @Index(name = "pidex")
    int getParentId();     // id of the inode
    void setParentId(int parentid);

    @PrimaryKey
    @Column(name = NAME)
    String getName();     //name of the inode
    void setName(String name);

    @Column(name = ID)
    @Index(name = "inode_idx")
    int getId();     // id of the inode
    void setId(int id);

    @Column(name = IS_DIR)
    byte getIsDir();
    void setIsDir(byte isDir);

    // Inode
    @Column(name = MODIFICATION_TIME)
    long getModificationTime();
    void setModificationTime(long modificationTime);

    // Inode
    @Column(name = ACCESS_TIME)
    long getATime();
    void setATime(long modificationTime);

    // Inode
    @Column(name = USER_ID)
    int getUserID();
    void setUserID(int userID);

    // Inode
    @Column(name = GROUP_ID)
    int getGroupID();
    void setGroupID(int groupID);

    // Inode
    @Column(name = PERMISSION)
    short getPermission();
    void setPermission(short permission);

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
    byte getUnderConstruction();
    void setUnderConstruction(byte underConstruction);

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
      Object[] pk = new Object[3];
      pk[0] = inode.getPartitionId();
      pk[1] = inode.getParentId();
      pk[2] = inode.getName();
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
  public INode findInodeByIdFTIS(int inodeId) throws StorageException {
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
      throw new StorageException("Fetching inode by id:"+inodeId+". Only one record was expected. Found: "+results.size());
    }
    
    if (results.size() == 1) {
      INode inode = convert(results.get(0));
      session.release(results);
      return inode;
    } else {
      return null;
    }
  }

  @Override
  public List<INode> findInodesByParentIdFTIS(int parentId)
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
    List<INode> inodeList = convert(results);
    session.release(results);
    return inodeList;
  }

  @Override
  public List<INode> findInodesByParentIdAndPartitionIdPPIS(int parentId, int partitionId)
      throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InodeDTO> dobj =
        qb.createQueryDefinition(InodeDTO.class);
    HopsPredicate pred1 = dobj.get("partitionId").equal(dobj.param("partitionIDParam"));
    HopsPredicate pred2 = dobj.get("parentId").equal(dobj.param("parentIDParam"));
    dobj.where(pred1.and(pred2));
    HopsQuery<InodeDTO> query = session.createQuery(dobj);
    query.setParameter("partitionIDParam", partitionId);
    query.setParameter("parentIDParam", parentId);

    explain(query);

    List<InodeDTO> results = query.getResultList();
    List<INode> inodeList = convert(results);
    session.release(results);
    return inodeList;
  }

  @Override
  public List<ProjectedINode> findInodesForSubtreeOperationsWithWriteLockFTIS(
      int parentId) throws StorageException {
    HopsSession session = connector.obtainSession();
    try {
      session.currentTransaction().begin();
      session.setLockMode(LockMode.EXCLUSIVE);
      HopsQueryBuilder qb = session.getQueryBuilder();
      HopsQueryDomainType<InodeDTO> dobj =
              qb.createQueryDefinition(InodeDTO.class);
      HopsPredicate pred2 = dobj.get("parentId").equal(dobj.param("parentIDParam"));
      dobj.where(pred2);
      HopsQuery<InodeDTO> query = session.createQuery(dobj);
      query.setParameter("parentIDParam", parentId);

      ArrayList<ProjectedINode> resultList = new ArrayList<ProjectedINode>();
      List<InodeDTO> results = query.getResultList();
      for (InodeDTO inode : results) {
        resultList.add(createProjectedINode(inode));
      }
      session.release(results);
      session.currentTransaction().commit();
      return resultList;
    }catch(StorageException e){
      session.currentTransaction().rollback();
      throw e;
    }
  }

  private ProjectedINode createProjectedINode(InodeDTO inode){
    return new ProjectedINode(inode.getId(),
              inode.getParentId(),
              inode.getName(),
              inode.getPartitionId(),
              NdbBoolean.convert(inode.getIsDir()),
              inode.getPermission(),
              inode.getUserID(),
              inode.getGroupID(),
              inode.getHeader(),
              inode.getSymlink()== null ? false : true,
              NdbBoolean.convert(inode.getQuotaEnabled()),
              NdbBoolean.convert(inode.getUnderConstruction()),
              NdbBoolean.convert(inode.getSubtreeLocked()),
              inode.getSubtreeLockOwner(),
              inode.getSize());
  }
//  public List<ProjectedINode> findInodesForSubtreeOperationsWithWriteLockFTIS(
//      int parentId) throws StorageException {
//    final String query = String.format(
//        "SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s " +
//            "WHERE %s=%d FOR UPDATE",
//        ID, NAME, PARENT_ID, PARTITION_ID, IS_DIR, PERMISSION, USER_ID, GROUP_ID, HEADER, SYMLINK,
//        QUOTA_ENABLED,
//        UNDER_CONSTRUCTION, SUBTREE_LOCKED, SUBTREE_LOCK_OWNER, SIZE, TABLE_NAME,
//        PARENT_ID, parentId);
//    ArrayList<ProjectedINode> resultList;
//    try {
//      Connection conn = mysqlConnector.obtainSession();
//      PreparedStatement s = conn.prepareStatement(query);
//      ResultSet result = s.executeQuery();
//      resultList = new ArrayList<ProjectedINode>();
//
//      while (result.next()) {
//        resultList.add(
//            new ProjectedINode(result.getInt(ID), result.getInt(PARENT_ID),
//                result.getString(NAME), result.getInt(PARTITION_ID),
//                result.getBoolean(IS_DIR), result.getShort(PERMISSION),
//                result.getInt(USER_ID), result.getInt(GROUP_ID),
//                result.getLong(HEADER),
//                result.getString(SYMLINK) == null ? false : true,
//                result.getBoolean(QUOTA_ENABLED),
//                result.getBoolean(UNDER_CONSTRUCTION),
//                result.getBoolean(SUBTREE_LOCKED),
//                result.getLong(SUBTREE_LOCK_OWNER),
//                result.getLong(SIZE)));
//      }
//    } catch (SQLException ex) {
//      throw HopsSQLExceptionHelper.wrap(ex);
//    } finally {
//      mysqlConnector.closeSession();
//    }
//    return resultList;
//  }

  @Override

  public List<ProjectedINode> findInodesForSubtreeOperationsWithWriteLockPPIS(
          int parentId, int partitionId) throws StorageException {
    HopsSession session = connector.obtainSession();
    try {
      session.currentTransaction().begin();
      session.setLockMode(LockMode.EXCLUSIVE);
      HopsQueryBuilder qb = session.getQueryBuilder();
      HopsQueryDomainType<InodeDTO> dobj =
              qb.createQueryDefinition(InodeDTO.class);
      HopsPredicate pred1 = dobj.get("partitionId").equal(dobj.param("partitionIDParam"));
      HopsPredicate pred2 = dobj.get("parentId").equal(dobj.param("parentIDParam"));
      dobj.where(pred1.and(pred2));
      HopsQuery<InodeDTO> query = session.createQuery(dobj);
      query.setParameter("partitionIDParam", partitionId);
      query.setParameter("parentIDParam", parentId);

      ArrayList<ProjectedINode> resultList = new ArrayList<ProjectedINode>();
      List<InodeDTO> results = query.getResultList();
      for (InodeDTO inode : results) {
        resultList.add(createProjectedINode(inode));
      }
      session.release(results);
      session.currentTransaction().commit();
      return resultList;
    }catch(StorageException e){
      session.currentTransaction().rollback();
      throw e;
    }
  }
//  public List<ProjectedINode> findInodesForSubtreeOperationsWithWriteLockPPIS(
//      int parentId, int partitionId) throws StorageException {
//    final String query = String.format(
//        "SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s " +
//            "WHERE %s=%d and %s=%d FOR UPDATE",
//        ID, NAME, PARENT_ID, PARTITION_ID, IS_DIR, PERMISSION, USER_ID, GROUP_ID, HEADER, SYMLINK,
//        QUOTA_ENABLED,
//        UNDER_CONSTRUCTION, SUBTREE_LOCKED, SUBTREE_LOCK_OWNER, SIZE, TABLE_NAME,
//        PARENT_ID, parentId, PARTITION_ID, partitionId);
//    ArrayList<ProjectedINode> resultList;
//    try {
//      Connection conn = mysqlConnector.obtainSession();
//      PreparedStatement s = conn.prepareStatement(query);
//      ResultSet result = s.executeQuery();
//      resultList = new ArrayList<ProjectedINode>();
//
//      while (result.next()) {
//        resultList.add(
//            new ProjectedINode(result.getInt(ID), result.getInt(PARENT_ID),
//                result.getString(NAME), result.getInt(PARTITION_ID),
//                result.getBoolean(IS_DIR), result.getShort(PERMISSION),
//                result.getInt(USER_ID), result.getInt(GROUP_ID),
//                result.getLong(HEADER),
//                result.getString(SYMLINK) == null ? false : true,
//                result.getBoolean(QUOTA_ENABLED),
//                result.getBoolean(UNDER_CONSTRUCTION),
//                result.getBoolean(SUBTREE_LOCKED),
//                result.getLong(SUBTREE_LOCK_OWNER),
//                result.getLong(SIZE)));
//      }
//    } catch (SQLException ex) {
//      throw HopsSQLExceptionHelper.wrap(ex);
//    } finally {
//      mysqlConnector.closeSession();
//    }
//    return resultList;
//  }

  @Override
  public INode findInodeByNameParentIdAndPartitionIdPK(String name, int parentId, int partitionId)
      throws StorageException {
    HopsSession session = connector.obtainSession();

    Object[] pk = new Object[3];
    pk[0] = partitionId;
    pk[1] = parentId;
    pk[2] = name;

    InodeDTO result = session.find(InodeDTO.class, pk);
    if (result != null) {
      INode inode = convert(result);
      session.release(result);
      return inode;
    } else {
      return null;
    }
  }

  @Override
  public List<INode> getINodesPkBatched(String[] names, int[] parentIds, int[] partitionIds)
      throws StorageException {
    HopsSession session = connector.obtainSession();

    List<InodeDTO> dtos = new ArrayList<InodeDTO>();
    for (int i =  0; i < names.length; i++) {
      InodeDTO dto = session
          .newInstance(InodeDTO.class, new Object[]{partitionIds[i], parentIds[i],names[i]});
      dto.setId(NOT_FOUND_ROW);
      dto = session.load(dto);
      dtos.add(dto);
    }
    session.flush();
    List<INode> inodeList = convert(dtos);
    session.release(dtos);
    return inodeList;
  }

  private boolean isRoot(INode inode){
    return inode.getName().equals("") && inode.getParentId() == 0 && inode
        .getId() == 1;
  }

  @Override
  public List<INodeIdentifier> getAllINodeFiles(long startId, long endId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InodeDTO> dobj =
        qb.createQueryDefinition(InodeDTO.class);
    HopsPredicate pred = dobj.get("isDir").equal(dobj.param("isDirParam"));
    HopsPredicate pred2 =
        dobj.get("id").between(dobj.param("startId"), dobj.param("endId"));
    dobj.where(pred.not().and(pred2));
    HopsQuery<InodeDTO> query = session.createQuery(dobj);
    query.setParameter("isDirParam", NdbBoolean.convert(true));
    //FIXME: InodeId is integer
    //startId is inclusive and endId exclusive
    query.setParameter("startId", (int) startId);
    query.setParameter("endId", (int) (endId - 1));
    List<InodeDTO> dtos = query.getResultList();
    List<INodeIdentifier> res = new ArrayList<INodeIdentifier>();
    for (InodeDTO dto : dtos) {
      res.add(
          new INodeIdentifier(dto.getId(), dto.getParentId(), dto.getName(), dto.getPartitionId()));
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
    return convert(query.getResultList());
  }
  
  @Override
  public boolean hasChildren(int parentId, boolean areChildRandomlyPartitioned) throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InodeDTO> dobj =
        qb.createQueryDefinition(InodeDTO.class);
    HopsPredicate pred1 = dobj.get("parentId").equal(dobj.param("parentIDParam"));
    HopsPredicate pred2 = dobj.get("partitionId").equal(dobj.param("partitionIDParam"));
    if(areChildRandomlyPartitioned){
      dobj.where(pred1);
    }else{
      dobj.where(pred2.and(pred1));
    }
    HopsQuery<InodeDTO> query = session.createQuery(dobj);

    query.setParameter("parentIDParam", parentId);
    if(!areChildRandomlyPartitioned){
      query.setParameter("partitionIDParam", parentId);
    }
    query.setLimits(0, 1);

    List<InodeDTO> results = query.getResultList();
    if(results.isEmpty()){
      return false;
    }else{
      session.release(results);
      return true;
    }
  }

  
  private List<INode> convert(List<InodeDTO> list) throws StorageException {
    List<INode> inodes = new ArrayList<INode>();
    for (InodeDTO persistable : list) {
      if (persistable.getId() != NOT_FOUND_ROW) {
        inodes.add(convert(persistable));
      }
    }
    return inodes;
  }


  protected static INode convert(InodeDTO persistable) {
    INode node = new INode(persistable.getId(), persistable.getName(),
        persistable.getParentId(), persistable.getPartitionId(),
        NdbBoolean.convert(persistable.getIsDir()),
        NdbBoolean.convert(persistable.getQuotaEnabled()),
        persistable.getModificationTime(), persistable.getATime(),
        persistable.getUserID(), persistable.getGroupID(),
        persistable.getPermission(), NdbBoolean.convert(persistable.getUnderConstruction()),
        persistable.getClientName(), persistable.getClientMachine(),
        persistable.getClientNode(), persistable.getGenerationStamp(),
        persistable.getHeader(), persistable.getSymlink(),
        NdbBoolean.convert(persistable.getSubtreeLocked()),
        persistable.getSubtreeLockOwner(),
        NdbBoolean.convert(persistable.getMetaEnabled()),
        persistable.getSize());
    return node;
  }

  protected static void createPersistable(INode inode, InodeDTO persistable) {
    persistable.setId(inode.getId());
    persistable.setName(inode.getName());
    persistable.setParentId(inode.getParentId());
    persistable.setQuotaEnabled(NdbBoolean.convert(inode.isDirWithQuota()));
    persistable.setModificationTime(inode.getModificationTime());
    persistable.setATime(inode.getAccessTime());
    persistable.setUserID(inode.getUserID());
    persistable.setGroupID(inode.getGroupID());
    persistable.setPermission(inode.getPermission());
    persistable.setUnderConstruction(NdbBoolean.convert(inode.isUnderConstruction()));
    persistable.setClientName(inode.getClientName());
    persistable.setClientMachine(inode.getClientMachine());
    persistable.setClientNode(inode.getClientNode());
    persistable.setGenerationStamp(inode.getGenerationStamp());
    persistable.setHeader(inode.getHeader());
    persistable.setSymlink(inode.getSymlink());
    persistable.setSubtreeLocked(NdbBoolean.convert(inode.isSubtreeLocked()));
    persistable.setSubtreeLockOwner(inode.getSubtreeLockOwner());
    persistable.setMetaEnabled(NdbBoolean.convert(inode.isMetaEnabled()));
    persistable.setSize(inode.getFileSize());
    persistable.setIsDir(NdbBoolean.convert(inode.isDirectory()));
    persistable.setPartitionId(inode.getPartitionId());
  }

  private void explain(HopsQuery<InodeDTO> query) {
//    Map<String, Object> map = null;
//    try {
//      map = query.explain();
//      System.out.println("Explain");
//      System.out.println("keys " + Arrays.toString(map.keySet().toArray()));
//      System.out.println("values " + Arrays.toString(map.values().toArray()));
//
//    } catch (StorageException e) {
//      e.printStackTrace();
//    }
  }

}
