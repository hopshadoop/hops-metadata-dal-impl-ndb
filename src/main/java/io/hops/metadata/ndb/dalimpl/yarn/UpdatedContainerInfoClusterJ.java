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
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.entity.UpdatedContainerInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpdatedContainerInfoClusterJ
        implements TablesDef.UpdatedContainerInfoTableDef,
        UpdatedContainerInfoDataAccess<UpdatedContainerInfo> {

  private static final Log LOG = LogFactory.getLog(UpdatedContainerInfoClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface UpdatedContainerInfoDTO {

    @PrimaryKey
    @Column(name = RMNODEID)
    String getrmnodeid();

    void setrmnodeid(String rmnodeid);

    @PrimaryKey
    @Column(name = CONTAINERID)
    String getcontainerid();

    void setcontainerid(String containerid);

    @PrimaryKey
    @Column(name = UPDATEDCONTAINERINFOID)
    int getupdatedcontainerinfoid();

    void setupdatedcontainerinfoid(int updatedcontainerinfoid);

    @Column(name = PENDING_EVENT_ID)
    int getpendingeventid();

    void setpendingeventid(int updatedcontainerinfoid);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Map<Integer, List<UpdatedContainerInfo>> findByRMNode(String rmnodeid)
          throws StorageException {
    LOG.debug("HOP :: ClusterJ UpdatedContainerInfo.findByRMNode - START:" +
        rmnodeid);
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<UpdatedContainerInfoDTO> dobj = qb.
            createQueryDefinition(UpdatedContainerInfoDTO.class);
    HopsPredicate pred1 = dobj.get(RMNODEID).equal(dobj.param(RMNODEID));
    dobj.where(pred1);

    HopsQuery<UpdatedContainerInfoDTO> query = session.createQuery(dobj);
    query.setParameter(RMNODEID, rmnodeid);
    List<UpdatedContainerInfoDTO> queryResults = query.getResultList();
    LOG.debug("HOP :: ClusterJ UpdatedContainerInfo.findByRMNode - FINISH:" +
        rmnodeid);
    Map<Integer, List<UpdatedContainerInfo>> result = null;
    if (queryResults != null && !queryResults.isEmpty()) {
      result = createUpdatedContainerInfoMap(queryResults);
    }
    session.release(queryResults);
    return result;
  }

  @Override
  public List<UpdatedContainerInfo> findByRMNodeList(
          String rmnodeid) throws StorageException {
    List<UpdatedContainerInfoDTO> results = findByRMNodeInt(rmnodeid);
    if (results != null && !results.isEmpty()) {
      return createUpdatedContainerInfoList(results);
    }
    return null;
  }

  private List<UpdatedContainerInfoDTO> findByRMNodeInt(
          String rmnodeid) throws StorageException {
    LOG.debug("HOP :: ClusterJ UpdatedContainerInfo.findByRMNode - START:"
            + rmnodeid);
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<UpdatedContainerInfoDTO> dobj = qb.
            createQueryDefinition(UpdatedContainerInfoDTO.class);
    HopsPredicate pred1 = dobj.get(RMNODEID).equal(dobj.param(RMNODEID));
    dobj.where(pred1);

    HopsQuery<UpdatedContainerInfoDTO> query = session.createQuery(dobj);
    query.setParameter(RMNODEID, rmnodeid);
    List<UpdatedContainerInfoDTO> results = query.getResultList();
    LOG.debug("HOP :: ClusterJ UpdatedContainerInfo.findByRMNode - FINISH:"
            + rmnodeid);
    return results;
  }

  @Override
  public Map<String, Map<Integer, List<UpdatedContainerInfo>>> getAll()
          throws StorageException {
    LOG.debug("HOP :: ClusterJ UpdatedContainerInfo.findByRMNode - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<UpdatedContainerInfoDTO> dobj = qb.
            createQueryDefinition(UpdatedContainerInfoDTO.class);
    HopsQuery<UpdatedContainerInfoDTO> query = session.createQuery(dobj);

    List<UpdatedContainerInfoDTO> results = query.getResultList();
    LOG.debug("HOP :: ClusterJ UpdatedContainerInfo.findByRMNode - FINISH");
    return createMap(results);
  }

  @Override
  public void addAll(Collection<UpdatedContainerInfo> containers)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<UpdatedContainerInfoDTO> toModify =
        new ArrayList<UpdatedContainerInfoDTO>();
    for (UpdatedContainerInfo entry : containers) {
      toModify.add(createPersistable(entry, session));
    }
    session.savePersistentAll(toModify);
    session.release(toModify);
  }

  @Override
  public void removeAll(Collection<UpdatedContainerInfo> containers)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<UpdatedContainerInfoDTO> toRemove =
        new ArrayList<UpdatedContainerInfoDTO>();
    for (UpdatedContainerInfo entry : containers) {
      toRemove.add(createPersistable(entry, session));
    }
    session.deletePersistentAll(toRemove);
    session.release(toRemove);
  }

  private UpdatedContainerInfoDTO createPersistable(UpdatedContainerInfo hop,
          HopsSession session) throws StorageException {
    UpdatedContainerInfoDTO dto =
        session.newInstance(UpdatedContainerInfoDTO.class);
    dto.setrmnodeid(hop.getRmnodeid());
    dto.setcontainerid(hop.getContainerId());
    dto.setupdatedcontainerinfoid(hop.getUpdatedContainerInfoId());
    dto.setpendingeventid(hop.getPendingEventId());
    return dto;
  }

  /**
   * Transforms a DTO to Hop object.
   *
   * @param rmDTO
   * @return HopRMNode
   */
  private UpdatedContainerInfo createHopUpdatedContainerInfo(
          UpdatedContainerInfoDTO dto) {
    return new UpdatedContainerInfo(dto.getrmnodeid(), dto.getcontainerid(),
            dto.getupdatedcontainerinfoid(), dto.getpendingeventid());
  }

  private Map<Integer, List<UpdatedContainerInfo>> createUpdatedContainerInfoMap(
          List<UpdatedContainerInfoDTO> list) {
    Map<Integer, List<UpdatedContainerInfo>> updatedContainerInfos =
        new HashMap<Integer, List<UpdatedContainerInfo>>();
    for (UpdatedContainerInfoDTO persistable : list) {
      if (!updatedContainerInfos.containsKey(persistable.
              getupdatedcontainerinfoid())) {
        updatedContainerInfos.put(persistable.getupdatedcontainerinfoid(),
                new ArrayList<UpdatedContainerInfo>());
      }
      updatedContainerInfos.get(persistable.getupdatedcontainerinfoid())
              .add(createHopUpdatedContainerInfo(persistable));
    }
    return updatedContainerInfos;
  }

  private List<UpdatedContainerInfo> createUpdatedContainerInfoList(
          List<UpdatedContainerInfoDTO> list) {
    List<UpdatedContainerInfo> updatedContainerInfos
            = new ArrayList<UpdatedContainerInfo>();
    for (UpdatedContainerInfoDTO persistable : list) {

      updatedContainerInfos.add(
              createHopUpdatedContainerInfo(persistable));
    }
    return updatedContainerInfos;
  }

  private Map<String, Map<Integer, List<UpdatedContainerInfo>>> createMap(
          List<UpdatedContainerInfoDTO> results) {
    Map<String, Map<Integer, List<UpdatedContainerInfo>>> map =
        new HashMap<String, Map<Integer, List<UpdatedContainerInfo>>>();
    for (UpdatedContainerInfoDTO persistable : results) {
      UpdatedContainerInfo hop = createHopUpdatedContainerInfo(persistable);
      if (map.get(hop.getRmnodeid()) == null) {
        map.put(hop.getRmnodeid(),
                new HashMap<Integer, List<UpdatedContainerInfo>>());
      }
      if (map.get(hop.getRmnodeid()).get(hop.getUpdatedContainerInfoId()) ==
          null) {
        map.get(hop.getRmnodeid()).put(hop.getUpdatedContainerInfoId(),
                new ArrayList<UpdatedContainerInfo>());
      }
      map.get(hop.getRmnodeid()).get(hop.getUpdatedContainerInfoId()).add(hop);
    }
    return map;
  }
}
