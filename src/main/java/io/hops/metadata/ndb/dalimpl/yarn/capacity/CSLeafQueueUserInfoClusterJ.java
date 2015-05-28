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
package io.hops.metadata.ndb.dalimpl.yarn.capacity;

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
import io.hops.metadata.yarn.entity.capacity.CSLeafQueueUserInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CSLeafQueueUserInfoClusterJ implements
        TablesDef.CSLeafQueueUserInfoTableDef,
        io.hops.metadata.yarn.dal.capacity.CSLeafQueueUserInfoDataAccess<CSLeafQueueUserInfo> {

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface CSLeafQueueUserInfoDTO {

    @PrimaryKey
    @Column(name = USER_NAME)
    String getusername();

    void setusername(String username);

    @Column(name = CONSUMED_RESOURCE_MEMORY)
    int getconsumedresourcememory();

    void setconsumedresourcememory(int consumedresourcememory);

    @Column(name = CONSUMED_RESOURCE_VCORES)
    int getconsumedresourcevcores();

    void setconsumedresourcevcores(int consumedresourcevcores);

    @Column(name = PENDING_APPLICATIONS)
    int getpendingapplications();

    void setpendingapplications(int pendingapplications);

    @Column(name = ACTIVE_APPLICATIONS)
    int getactiveapplications();

    void setactiveapplications(int activeapplications);

  }

  @Override
  public CSLeafQueueUserInfo findById(String id) throws StorageException {
    HopsSession session = connector.obtainSession();

    CSLeafQueueUserInfoDTO csLeafOueueUserInfoDTO = null;
    if (session != null) {
      csLeafOueueUserInfoDTO = session.find(CSLeafQueueUserInfoDTO.class, id);
    }
    return createCSLeafQueueUserInfo(csLeafOueueUserInfoDTO);
  }

  @Override
  public void createCSLeafQueueUserInfo(
          CSLeafQueueUserInfo csleafqueueuserinfo) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(csleafqueueuserinfo, session));

  }

  @Override
  public List<CSLeafQueueUserInfo> findAll() throws StorageException,
          IOException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<CSLeafQueueUserInfoClusterJ.CSLeafQueueUserInfoDTO> dobj
            = qb.createQueryDefinition(
                    CSLeafQueueUserInfoClusterJ.CSLeafQueueUserInfoDTO.class);
    HopsQuery<CSLeafQueueUserInfoClusterJ.CSLeafQueueUserInfoDTO> query
            = session.createQuery(dobj);
    List<CSLeafQueueUserInfoClusterJ.CSLeafQueueUserInfoDTO> results = query.
            getResultList();
    session.flush();

    return createCSLeafQueueUserInfoList(results);
  }

  private List<CSLeafQueueUserInfo> createCSLeafQueueUserInfoList(
          List<CSLeafQueueUserInfoClusterJ.CSLeafQueueUserInfoDTO> list) throws
          IOException {
    List<CSLeafQueueUserInfo> csLeafQueueUserInfoList
            = new ArrayList<CSLeafQueueUserInfo>();
    for (CSLeafQueueUserInfoClusterJ.CSLeafQueueUserInfoDTO persistable : list) {
      csLeafQueueUserInfoList.add(createCSLeafQueueUserInfo(persistable));
    }
    return csLeafQueueUserInfoList;
  }

  @Override
  public void addAll(Collection<CSLeafQueueUserInfo> modified) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    try {
      if (modified != null) {
        List<CSLeafQueueUserInfoDTO> toModify
                = new ArrayList<CSLeafQueueUserInfoDTO>(modified.size());
        for (CSLeafQueueUserInfo hop : modified) {
          toModify.add(createPersistable(hop, session));
        }
        session.savePersistentAll(toModify);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void removeAll(Collection<CSLeafQueueUserInfo> removed) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    try {
      if (removed != null) {
        List<CSLeafQueueUserInfoDTO> toRemove
                = new ArrayList<CSLeafQueueUserInfoDTO>(removed.size());
        for (CSLeafQueueUserInfo hop : removed) {
          toRemove.add(session.newInstance(
                  CSLeafQueueUserInfoClusterJ.CSLeafQueueUserInfoDTO.class, hop.
                  getUserName()));
        }
        session.deletePersistentAll(toRemove);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private CSLeafQueueUserInfo createCSLeafQueueUserInfo(
          CSLeafQueueUserInfoDTO csLeafQueueUserInfoDTO) {

    return new CSLeafQueueUserInfo(
            csLeafQueueUserInfoDTO.getusername(),
            csLeafQueueUserInfoDTO.getconsumedresourcememory(),
            csLeafQueueUserInfoDTO.getconsumedresourcevcores(),
            csLeafQueueUserInfoDTO.getpendingapplications(),
            csLeafQueueUserInfoDTO.getactiveapplications()
    );

  }

  private CSLeafQueueUserInfoDTO createPersistable(CSLeafQueueUserInfo hop,
          HopsSession session) throws StorageException {
    CSLeafQueueUserInfoClusterJ.CSLeafQueueUserInfoDTO csLeafQueueUserInfoDTO
            = session.newInstance(
                    CSLeafQueueUserInfoClusterJ.CSLeafQueueUserInfoDTO.class);

    csLeafQueueUserInfoDTO.setusername(hop.getUserName());
    csLeafQueueUserInfoDTO.setconsumedresourcememory(hop.getConsumedMemory());
    csLeafQueueUserInfoDTO.setconsumedresourcevcores(hop.getConsumedVCores());
    csLeafQueueUserInfoDTO.setpendingapplications(hop.getPendingApplications());
    csLeafQueueUserInfoDTO.setactiveapplications(hop.getActiveApplications());
    return csLeafQueueUserInfoDTO;
  }
}
