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
package io.hops.metadata.ndb.dalimpl.election;

import com.mysql.clusterj.annotation.PartitionKey;
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.election.TablesDef;
import io.hops.metadata.election.dal.LeDescriptorDataAccess;
import io.hops.metadata.election.entity.LeDescriptor;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.dalimpl.ClusterjDataAccess;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class LeDescriptorClusterj extends ClusterjDataAccess
    implements TablesDef.LeDescriptorTableDef, LeDescriptorDataAccess<LeDescriptor> {

  @PartitionKey(column = PARTITION_VAL)
  Class dto;

  public interface LeaderDTO {

    long getId();

    void setId(long id);

    int getPartitionVal();

    void setPartitionVal(int partitionVal);

    long getCounter();

    void setCounter(long counter);

    String getHostname();

    void setHostname(String hostname);

    String getHttpAddress();

    void setHttpAddress(String httpAddress);
  }

  public LeDescriptorClusterj(ClusterjConnector connector, Class dto) {
    super(connector);
    this.dto = dto;
  }

  @Override
  public LeDescriptor findByPkey(long id, int partitionKey)
      throws StorageException {
    HopsSession dbSession = getConnector().obtainSession();
    Object[] keys = new Object[]{partitionKey, id};
    LeaderDTO lTable = (LeaderDTO) dbSession.find(dto, keys);
    if (lTable != null) {
      LeDescriptor leader = createDescriptor(lTable);
      return leader;
    }
    return null;
  }

  @Override
  public Collection<LeDescriptor> findAll() throws StorageException {
    HopsSession dbSession = getConnector().obtainSession();
    HopsQueryBuilder qb = dbSession.getQueryBuilder();
    HopsQueryDomainType<LeaderDTO> dobj = qb.createQueryDefinition(dto);
    HopsQuery<LeaderDTO> query = dbSession.createQuery(dobj);
    return createList(query.getResultList());

  }

  @Override
  public void prepare(Collection<LeDescriptor> removed,
                      Collection<LeDescriptor> newed, Collection<LeDescriptor> modified)
      throws StorageException {
    HopsSession dbSession = getConnector().obtainSession();
    List<LeaderDTO> changes = new ArrayList<LeaderDTO>();
    List<LeaderDTO> deletions = new ArrayList<LeaderDTO>();
    for (LeDescriptor l : newed) {

      LeaderDTO lTable = (LeaderDTO) dbSession.newInstance(dto);
      createPersistableLeaderInstance(l, lTable);
      changes.add(lTable);
    }

    for (LeDescriptor l : modified) {
      LeaderDTO lTable = (LeaderDTO) dbSession.newInstance(dto);
      createPersistableLeaderInstance(l, lTable);
      changes.add(lTable);
    }

    for (LeDescriptor l : removed) {
      LeaderDTO lTable = (LeaderDTO) dbSession.newInstance(dto);
      createPersistableLeaderInstance(l, lTable);
      deletions.add(lTable);
    }
    dbSession.deletePersistentAll(deletions);
    dbSession.savePersistentAll(changes);
  }

  private Collection<LeDescriptor> createList(final List<LeaderDTO> list) {
    Collection<LeDescriptor> listRet = new ArrayList<LeDescriptor>(list.size());
    for (LeaderDTO dto : list) {
      listRet.add(createDescriptor(dto));
    }

    return listRet;
  }

  protected abstract LeDescriptor createDescriptor(LeaderDTO lTable);

  private void createPersistableLeaderInstance(LeDescriptor leader,
                                               LeaderDTO lTable) {
    lTable.setId(leader.getId());
    lTable.setCounter(leader.getCounter());
    lTable.setHostname(leader.getHostName());
    lTable.setHttpAddress(leader.getHttpAddress());
    lTable.setPartitionVal(leader.getPartitionVal());
  }
}
