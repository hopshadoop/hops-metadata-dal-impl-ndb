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

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.StorageConnector;
import io.hops.metadata.election.TablesDef;
import io.hops.metadata.election.entity.LeDescriptor;
import io.hops.metadata.ndb.ClusterjConnector;

import java.security.InvalidParameterException;

public class YarnLeaderClusterj extends LeDescriptorClusterj
    implements TablesDef.YarnLeaderTableDef {

  @PersistenceCapable(table = TABLE_NAME)
  public interface YarnLeaderDTO extends LeaderDTO {

    @PrimaryKey
    @Column(name = TablesDef.LeDescriptorTableDef.ID)
    long getId();

    void setId(long id);

    @PrimaryKey
    @Column(name = TablesDef.LeDescriptorTableDef.PARTITION_VAL)
    int getPartitionVal();

    void setPartitionVal(int partitionVal);

    @Column(name = TablesDef.LeDescriptorTableDef.COUNTER)
    long getCounter();

    void setCounter(long counter);

    @Column(name = TablesDef.LeDescriptorTableDef.HOSTNAME)
    String getHostname();

    void setHostname(String hostname);

    @Column(name = TablesDef.LeDescriptorTableDef.HTTP_ADDRESS)
    String getHttpAddress();

    void setHttpAddress(String httpAddress);

  }

  @Override
  protected LeDescriptor createDescriptor(LeaderDTO lTable) {
    if (lTable.getPartitionVal() != 0) {
      throw new InvalidParameterException("Psrtition key should be zero");
    }
    return new LeDescriptor.YarnLeDescriptor(lTable.getId(),
        lTable.getCounter(), lTable.getHostname(), lTable.getHttpAddress());
  }

  public YarnLeaderClusterj(ClusterjConnector connector) {
    super(connector, YarnLeaderDTO.class);
  }
}
