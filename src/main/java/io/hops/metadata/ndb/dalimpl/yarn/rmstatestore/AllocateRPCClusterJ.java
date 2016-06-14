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
package io.hops.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.rmstatestore.AllocateRPCDataAccess;
import io.hops.metadata.yarn.entity.appmasterrpc.AllocateRPC;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AllocateRPCClusterJ implements AllocateRPCDataAccess<AllocateRPC>,
        TablesDef.AllocateRPC, TablesDef.AllocateRPCAsk,
        TablesDef.AllocateRPCBlackListAdd, TablesDef.AllocateRPCBlackListRemove,
        TablesDef.AllocateRPCRelease, TablesDef.AllocateRPCResourceIncrease {

  @PersistenceCapable(table
          = TablesDef.AllocateRPC.TABLE_NAME)
  public interface AllocateRPCDTO {

    @PrimaryKey
    @Column(name = TablesDef.AllocateRPC.RPCID)
    int getrpcid();

    void setrpcid(int id);

    @Column(name = TablesDef.AllocateRPC.PROGRESS)
    float getProgress();

    void setProgress(float progress);

    @Column(name = TablesDef.AllocateRPC.RESPONSEID)
    int getResponseId();

    void setResponseId(int responseId);
  }

  @PersistenceCapable(table
          = TablesDef.AllocateRPCAsk.TABLE_NAME)
  public interface AskDTO {

    @PrimaryKey
    @Column(name = TablesDef.AllocateRPCAsk.RPCID)
    int getrpcid();

    void setrpcid(int id);

    @PrimaryKey
    @Column(name = TablesDef.AllocateRPCAsk.REQUESTID)
    String getRequestId();

    void setRequestId(String requestId);

    @Column(name = TablesDef.AllocateRPCAsk.REQUEST)
    byte[] getRequest();

    void setRequest(byte[] request);
  }

  @PersistenceCapable(table
          = TablesDef.AllocateRPCBlackListAdd.TABLE_NAME)
  public interface BlackListAdditionDTO {

    @PrimaryKey
    @Column(name = TablesDef.AllocateRPCBlackListAdd.RPCID)
    int getrpcid();

    void setrpcid(int id);

    @PrimaryKey
    @Column(name = TablesDef.AllocateRPCBlackListAdd.RESOURCE)
    String getResource();

    void setResource(String resource);
  }

  @PersistenceCapable(table
          = TablesDef.AllocateRPCBlackListRemove.TABLE_NAME)
  public interface BlackListRemovalDTO {

    @PrimaryKey
    @Column(name = TablesDef.AllocateRPCBlackListRemove.RPCID)
    int getrpcid();

    void setrpcid(int id);

    @PrimaryKey
    @Column(name = TablesDef.AllocateRPCBlackListRemove.RESOURCE)
    String getResource();

    void setResource(String resource);
  }

  @PersistenceCapable(table
          = TablesDef.AllocateRPCRelease.TABLE_NAME)
  public interface ReleaseDTO {

    @PrimaryKey
    @Column(name = TablesDef.AllocateRPCRelease.RPCID)
    int getrpcid();

    void setrpcid(int id);

    @PrimaryKey
    @Column(name = TablesDef.AllocateRPCRelease.CONTAINERID)
    String getContainerId();

    void setContainerId(String containerId);
  }

  @PersistenceCapable(table
          = TablesDef.AllocateRPCResourceIncrease.TABLE_NAME)
  public interface ResourceIncreaseRequestDTO {

    @PrimaryKey
    @Column(name = TablesDef.AllocateRPCResourceIncrease.RPCID)
    int getrpcid();

    void setrpcid(int id);

    @PrimaryKey
    @Column(name = TablesDef.AllocateRPCResourceIncrease.REQUESTID)
    String getRequestId();

    void setRequestId(String requestId);

    @Column(name = TablesDef.AllocateRPCResourceIncrease.REQUEST)
    byte[] getRequest();

    void setRequest(byte[] request);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void add(AllocateRPC toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    AllocateRPCDTO allocateDTO = createPersistableAllocateRPC(toAdd, session);

    List<AskDTO> askDTOs = new ArrayList<AskDTO>();
    for (String requestId : toAdd.getAsk().keySet()) {
      askDTOs.add(createPersistableAsk(toAdd.getRpcID(), requestId, toAdd.
              getAsk().get(requestId), session));
    }

    List<BlackListAdditionDTO> blAdd = new ArrayList<BlackListAdditionDTO>();
    for (String blResource : toAdd.getBlackListAddition()) {
      blAdd.add(createPersistableBlackListAddition(toAdd.getRpcID(), blResource,
              session));
    }

    List<BlackListRemovalDTO> blRemoval = new ArrayList<BlackListRemovalDTO>();
    for (String blResource : toAdd.getBlackListAddition()) {
      blRemoval.add(createPersistableBlackListRemoval(toAdd.getRpcID(),
              blResource, session));
    }

    List<ReleaseDTO> releaseDTOs = new ArrayList<ReleaseDTO>();
    for (String release : toAdd.getReleaseList()) {
      releaseDTOs.add(createPersistableRelease(toAdd.getRpcID(), release,
              session));
    }

    List<ResourceIncreaseRequestDTO> resourceIncDTOs
            = new ArrayList<ResourceIncreaseRequestDTO>();
    for (String resourceId : toAdd.getResourceIncreaseRequest().keySet()) {
      resourceIncDTOs.add(createPersistableResourceIncreaseRequest(toAdd.
              getRpcID(), resourceId, toAdd.getResourceIncreaseRequest().get(
                      resourceId), session));
    }

    session.savePersistent(allocateDTO);
    session.savePersistentAll(askDTOs);
    session.savePersistentAll(blAdd);
    session.savePersistentAll(blRemoval);
    session.savePersistentAll(releaseDTOs);
    session.savePersistentAll(resourceIncDTOs);
  }

  @Override
  public Map<Integer, AllocateRPC> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<AllocateRPCDTO> allocDobj = qb.createQueryDefinition(
            AllocateRPCDTO.class);
    HopsQuery<AllocateRPCDTO> allocateRPCQuery = session.createQuery(allocDobj);
    List<AllocateRPCDTO> allocateRPCQueryResults = allocateRPCQuery.
            getResultList();

    HopsQueryDomainType<AskDTO> askDobj = qb.createQueryDefinition(
            AskDTO.class);
    HopsQuery<AskDTO> askQuery = session.createQuery(askDobj);
    List<AskDTO> askQueryResults = askQuery.getResultList();

    HopsQueryDomainType<BlackListAdditionDTO> blAddDobj = qb.
            createQueryDefinition(
                    BlackListAdditionDTO.class);
    HopsQuery<BlackListAdditionDTO> blAddQuery = session.createQuery(blAddDobj);
    List<BlackListAdditionDTO> blAddQueryResults = blAddQuery.getResultList();

    HopsQueryDomainType<BlackListRemovalDTO> blRemoveDobj = qb.
            createQueryDefinition(
                    BlackListRemovalDTO.class);
    HopsQuery<BlackListRemovalDTO> blRemoveQuery = session.createQuery(
            blRemoveDobj);
    List<BlackListRemovalDTO> blRemoveQueryResults = blRemoveQuery.
            getResultList();

    HopsQueryDomainType<ReleaseDTO> releaseDobj = qb.createQueryDefinition(
            ReleaseDTO.class);
    HopsQuery<ReleaseDTO> releaseQuery = session.createQuery(releaseDobj);
    List<ReleaseDTO> releaseQueryResults = releaseQuery.getResultList();

    HopsQueryDomainType<ResourceIncreaseRequestDTO> resourceIncDobj = qb.
            createQueryDefinition(
                    ResourceIncreaseRequestDTO.class);
    HopsQuery<ResourceIncreaseRequestDTO> resourceIncQuery = session.
            createQuery(resourceIncDobj);
    List<ResourceIncreaseRequestDTO> resourceIncQueryResults = resourceIncQuery.
            getResultList();

    Map<Integer, AllocateRPC> result = creatAllocateRPCMap(
            allocateRPCQueryResults, askQueryResults, blAddQueryResults,
            blRemoveQueryResults, releaseQueryResults, resourceIncQueryResults);

    session.release(allocateRPCQueryResults);
    session.release(askQueryResults);
    session.release(blAddQueryResults);
    session.release(blRemoveQueryResults);
    session.release(releaseQueryResults);
    session.release(resourceIncQueryResults);

    return result;
  }

  private AllocateRPCDTO createPersistableAllocateRPC(AllocateRPC rpc,
          HopsSession session) throws StorageException {
    AllocateRPCDTO result = session.newInstance(AllocateRPCDTO.class);
    result.setProgress(rpc.getProgress());
    result.setResponseId(rpc.getResponseId());
    result.setrpcid(rpc.getRpcID());
    return result;
  }

  private AskDTO createPersistableAsk(int rpcId, String requestId,
          byte[] request, HopsSession session) throws StorageException {
    AskDTO result = session.newInstance(AskDTO.class);
    result.setRequest(request);
    result.setRequestId(requestId);
    result.setrpcid(rpcId);
    return result;
  }

  private BlackListAdditionDTO createPersistableBlackListAddition(int rpcId,
          String blResource, HopsSession session) throws StorageException {
    BlackListAdditionDTO result = session.
            newInstance(BlackListAdditionDTO.class);
    result.setResource(blResource);
    result.setrpcid(rpcId);
    return result;
  }

  private BlackListRemovalDTO createPersistableBlackListRemoval(int rpcId,
          String blResource, HopsSession session) throws StorageException {
    BlackListRemovalDTO result = session.
            newInstance(BlackListRemovalDTO.class);
    result.setResource(blResource);
    result.setrpcid(rpcId);
    return result;
  }

  private ReleaseDTO createPersistableRelease(int rpcId,
          String containerId, HopsSession session) throws StorageException {
    ReleaseDTO result = session.newInstance(ReleaseDTO.class);
    result.setContainerId(containerId);
    result.setrpcid(rpcId);
    return result;
  }

  private ResourceIncreaseRequestDTO createPersistableResourceIncreaseRequest(
          int rpcId, String requestId,
          byte[] request, HopsSession session) throws StorageException {
    ResourceIncreaseRequestDTO result = session.newInstance(
            ResourceIncreaseRequestDTO.class);
    result.setRequest(request);
    result.setRequestId(requestId);
    result.setrpcid(rpcId);
    return result;
  }

  private Map<Integer, AllocateRPC> creatAllocateRPCMap(
          List<AllocateRPCDTO> allocateRPCQueryResults,
          List<AskDTO> askQueryResults,
          List<BlackListAdditionDTO> blAddQueryResults,
          List<BlackListRemovalDTO> blRemoveQueryResults,
          List<ReleaseDTO> releaseQueryResults,
          List<ResourceIncreaseRequestDTO> resourceIncQueryResults) {

    Map<Integer, Map<String, byte[]>> askMap
            = new HashMap<Integer, Map<String, byte[]>>();
    for (AskDTO askDTO : askQueryResults) {
      Map<String, byte[]> askList = askMap.get(askDTO.getrpcid());
      if (askList == null) {
        askList = new HashMap<String, byte[]>();
        askMap.put(askDTO.getrpcid(), askList);
      }
      askList.put(askDTO.getRequestId(), askDTO.getRequest());
    }

    Map<Integer, List<String>> blAddMap = new HashMap<Integer, List<String>>();
    for (BlackListAdditionDTO blAddDTO : blAddQueryResults) {
      List<String> blAddList = blAddMap.get(blAddDTO.getrpcid());
      if (blAddList == null) {
        blAddList = new ArrayList<String>();
        blAddMap.put(blAddDTO.getrpcid(), blAddList);
      }
      blAddList.add(blAddDTO.getResource());
    }

    Map<Integer, List<String>> blRemoveMap
            = new HashMap<Integer, List<String>>();
    for (BlackListRemovalDTO blRemoveDTO : blRemoveQueryResults) {
      List<String> blRemoveList = blRemoveMap.get(blRemoveDTO.getrpcid());
      if (blRemoveList == null) {
        blRemoveList = new ArrayList<String>();
        blRemoveMap.put(blRemoveDTO.getrpcid(), blRemoveList);
      }
      blRemoveList.add(blRemoveDTO.getResource());
    }

    Map<Integer, List<String>> releaseMap = new HashMap<Integer, List<String>>();
    for (ReleaseDTO releaseDTO : releaseQueryResults) {
      List<String> releaseList = releaseMap.get(releaseDTO.getrpcid());
      if (releaseList == null) {
        releaseList = new ArrayList<String>();
        releaseMap.put(releaseDTO.getrpcid(), releaseList);
      }
      releaseList.add(releaseDTO.getContainerId());
    }

    Map<Integer, Map<String, byte[]>> resourceIncMap
            = new HashMap<Integer, Map<String, byte[]>>();
    for (ResourceIncreaseRequestDTO resourceIncDTO : resourceIncQueryResults) {
      Map<String, byte[]> resourceIncList = resourceIncMap.get(resourceIncDTO.
              getrpcid());
      if (resourceIncList == null) {
        resourceIncList = new HashMap<String, byte[]>();
        resourceIncMap.put(resourceIncDTO.getrpcid(), resourceIncList);
      }
      resourceIncList.put(resourceIncDTO.getRequestId(), resourceIncDTO.
              getRequest());
    }

    Map<Integer, AllocateRPC> result = new HashMap<Integer, AllocateRPC>();
    for (AllocateRPCDTO allocateDTO : allocateRPCQueryResults) {
      int rpcId = allocateDTO.getrpcid();
      result.put(rpcId, new AllocateRPC(rpcId, allocateDTO.getResponseId(),
              allocateDTO.getProgress(), releaseMap.get(rpcId), askMap.
              get(rpcId), resourceIncMap.get(rpcId), blAddMap.get(rpcId),
              blRemoveMap.get(rpcId)));
    }
    return result;
  }
}
