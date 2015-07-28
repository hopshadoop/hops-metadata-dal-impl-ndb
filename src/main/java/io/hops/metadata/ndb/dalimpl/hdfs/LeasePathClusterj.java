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
import io.hops.metadata.hdfs.dal.LeasePathDataAccess;
import io.hops.metadata.hdfs.entity.LeasePath;
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

public class LeasePathClusterj
    implements TablesDef.LeasePathTableDef, LeasePathDataAccess<LeasePath> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = PART_KEY)
  @Index(name = "holder_idx")
  public interface LeasePathsDTO {

    @Column(name = HOLDER_ID)
    int getHolderId();

    void setHolderId(int holder_id);

    @PrimaryKey
    @Column(name = PATH)
    String getPath();

    void setPath(String path);

    @PrimaryKey
    @Column(name = PART_KEY)
    int getPartKey();

    void setPartKey(int partKey);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void prepare(Collection<LeasePath> removed,
      Collection<LeasePath> newed, Collection<LeasePath> modified)
      throws StorageException {
    List<LeasePathsDTO> changes = new ArrayList<LeasePathsDTO>();
    List<LeasePathsDTO> deletions = new ArrayList<LeasePathsDTO>();
    HopsSession dbSession = connector.obtainSession();
    for (LeasePath lp : newed) {
      LeasePathsDTO lTable = dbSession.newInstance(LeasePathsDTO.class);
      createPersistableLeasePathInstance(lp, lTable);
      changes.add(lTable);
    }

    for (LeasePath lp : modified) {
      LeasePathsDTO lTable = dbSession.newInstance(LeasePathsDTO.class);
      createPersistableLeasePathInstance(lp, lTable);
      changes.add(lTable);
    }

    for (LeasePath lp : removed) {
      Object[] key = new Object[2];
      key[0] = lp.getPath();
      key[1] = PART_KEY_VAL;
      LeasePathsDTO lTable = dbSession.newInstance(LeasePathsDTO.class, key);
      deletions.add(lTable);
    }
    dbSession.deletePersistentAll(deletions);
    dbSession.savePersistentAll(changes);
    dbSession.release(deletions);
    dbSession.release(changes);
  }

  @Override
  public Collection<LeasePath> findByHolderId(int holderId)
      throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType<LeasePathsDTO> dobj =
        qb.createQueryDefinition(LeasePathsDTO.class);
    HopsPredicate pred1 = dobj.get("holderId").equal(dobj.param("param1"));
    HopsPredicate pred2 = dobj.get("partKey").equal(dobj.param("param2"));
    dobj.where(pred1);
    HopsQuery<LeasePathsDTO> query = dbSession.createQuery(dobj);
    query.setParameter("param1", holderId);
    query.setParameter("param2", PART_KEY_VAL);
    
    Collection<LeasePathsDTO> dtos = query.getResultList();
    Collection<LeasePath> lpl = createList(dtos);
    dbSession.release(dtos);
    return lpl;
  }

  @Override
  public LeasePath findByPKey(String path) throws StorageException {
    Object[] key = new Object[2];
    key[0] = path;
    key[1] = PART_KEY_VAL;
    HopsSession dbSession = connector.obtainSession();
    LeasePathsDTO lPTable = dbSession.find(LeasePathsDTO.class, key);
    LeasePath lPath = null;
    if (lPTable != null) {
      lPath = createLeasePath(lPTable);
      dbSession.release(lPTable);
    }
    return lPath;
  }

  @Override
  public Collection<LeasePath> findByPrefix(String prefix)
      throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType dobj = qb.createQueryDefinition(LeasePathsDTO.class);
    HopsPredicateOperand propertyPredicate = dobj.get("path");
    String param = "prefix";
    HopsPredicateOperand propertyLimit = dobj.param(param);
    HopsPredicate like = propertyPredicate.like(propertyLimit)
        .and(dobj.get("partKey").equal(dobj.param("partKeyParam")));
    dobj.where(like);
    HopsQuery query = dbSession.createQuery(dobj);
    query.setParameter(param, prefix + "%");
    query.setParameter("partKeyParam", PART_KEY_VAL);
    
    Collection<LeasePathsDTO> dtos = query.getResultList();
    Collection<LeasePath> lpl = createList(dtos);
    dbSession.release(dtos);
    return lpl;
  }

  @Override
  public Collection<LeasePath> findAll() throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType dobj = qb.createQueryDefinition(LeasePathsDTO.class);
    HopsPredicate pred = dobj.get("partKey").equal(dobj.param("param"));
    dobj.where(pred);
    HopsQuery query = dbSession.createQuery(dobj);
    query.setParameter("param", PART_KEY_VAL);
    
    Collection<LeasePathsDTO> dtos = query.getResultList();
    Collection<LeasePath> lpl = createList(dtos);
    dbSession.release(dtos);
    return lpl;
  }

  @Override
  public void removeAll() throws StorageException {
    HopsSession dbSession = connector.obtainSession();
    dbSession.deletePersistentAll(LeasePathsDTO.class);
  }

  private List<LeasePath> createList(Collection<LeasePathsDTO> dtos) {
    List<LeasePath> list = new ArrayList<LeasePath>();
    for (LeasePathsDTO leasePathsDTO : dtos) {
      list.add(createLeasePath(leasePathsDTO));
    }
    return list;
  }

  private LeasePath createLeasePath(LeasePathsDTO leasePathTable) {
    return new LeasePath(leasePathTable.getPath(),
        leasePathTable.getHolderId());
  }

  private void createPersistableLeasePathInstance(LeasePath lp,
      LeasePathsDTO lTable) {
    lTable.setHolderId(lp.getHolderId());
    lTable.setPath(lp.getPath());
    lTable.setPartKey(PART_KEY_VAL);
  }
}
