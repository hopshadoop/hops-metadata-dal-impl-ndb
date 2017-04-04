/* * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
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
package io.hops.metadata.ndb;

import io.hops.DalStorageFactory;
import io.hops.MultiZoneStorageConnector;
import io.hops.StorageConnector;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.election.dal.HdfsLeDescriptorDataAccess;
import io.hops.metadata.election.dal.YarnLeDescriptorDataAccess;
import io.hops.metadata.hdfs.dal.*;
import io.hops.metadata.ndb.dalimpl.election.HdfsLeaderClusterj;
import io.hops.metadata.ndb.dalimpl.election.YarnLeaderClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.*;
import io.hops.metadata.ndb.dalimpl.yarn.*;
import io.hops.metadata.ndb.dalimpl.yarn.quota.*;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.ApplicationAttemptStateClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.ApplicationStateClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.DelegationKeyClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.DelegationTokenClusterJ;
import io.hops.metadata.yarn.dal.*;
import io.hops.metadata.yarn.dal.quota.*;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationKeyDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationTokenDataAccess;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class NdbStorageFactory implements DalStorageFactory {

  private interface DataAccessBuilder {
    EntityDataAccess build(ClusterjConnector connector);
  }

  private Map<Class, DataAccessBuilder> dataAccessMap = new HashMap<>();
  private MultiZoneClusterjConnector connector;

  @Override
  public void setConfiguration(Properties conf)
      throws StorageInitializtionException {
    try {
      connector = new MultiZoneClusterjConnector(conf);
      initDataAccessMap();
    } catch (IOException ex) {
      throw new StorageInitializtionException(ex);
    }
  }

  @Override
  public MultiZoneStorageConnector getMultiZoneConnector() {
    return connector;
  }

  /**
   * getDataAccess returns a data access class instance configured with the provided connector.
   */
  @Override
  public EntityDataAccess getDataAccess(StorageConnector connector, Class type) {
    return dataAccessMap.get(type).build((ClusterjConnector) connector);
  }

  private void initDataAccessMap() {
    dataAccessMap.put(ResourceDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new ResourceClusterJ(connector);
      }
    });
    dataAccessMap.put(ResourceDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new ResourceClusterJ(connector);
      }
    });
    dataAccessMap.put(RMNodeDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new RMNodeClusterJ(connector);
      }
    });
    dataAccessMap.put(ContainerStatusDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new ContainerStatusClusterJ(connector);
      }
    });
    dataAccessMap.put(UpdatedContainerInfoDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new UpdatedContainerInfoClusterJ(connector);
      }
    });
    dataAccessMap.put(ContainerIdToCleanDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new ContainerIdToCleanClusterJ(connector);
      }
    });
    dataAccessMap.put(FinishedApplicationsDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new FinishedApplicationsClusterJ(connector);
      }
    });
    dataAccessMap.put(BlockInfoDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new BlockInfoClusterj(connector);
      }
    });
    dataAccessMap.put(PendingBlockDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new PendingBlockClusterj(connector);
      }
    });
    dataAccessMap.put(ReplicaUnderConstructionDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new ReplicaUnderConstructionClusterj(connector);
      }
    });
    dataAccessMap.put(INodeDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new INodeClusterj(connector);
      }
    });
    dataAccessMap.put(INodeAttributesDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new INodeAttributesClusterj(connector);
      }
    });
    dataAccessMap.put(LeaseDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new LeaseClusterj(connector);
      }
    });
    dataAccessMap.put(LeasePathDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new LeasePathClusterj(connector);
      }
    });
    dataAccessMap.put(OngoingSubTreeOpsDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new OnGoingSubTreeOpsClusterj(connector);
      }
    });
    dataAccessMap.put(HdfsLeDescriptorDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new HdfsLeaderClusterj(connector);
      }
    });
    dataAccessMap.put(YarnLeDescriptorDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new YarnLeaderClusterj(connector);
      }
    });
    dataAccessMap.put(ReplicaDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new ReplicaClusterj(connector);
      }
    });
    dataAccessMap.put(CorruptReplicaDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new CorruptReplicaClusterj(connector);
      }
    });
    dataAccessMap.put(ExcessReplicaDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new ExcessReplicaClusterj(connector);
      }
    });
    dataAccessMap.put(InvalidateBlockDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new InvalidatedBlockClusterj(connector);
      }
    });
    dataAccessMap.put(UnderReplicatedBlockDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new UnderReplicatedBlockClusterj(connector);
      }
    });
    dataAccessMap.put(VariableDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new VariableClusterj(connector);
      }
    });
    dataAccessMap.put(StorageIdMapDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new StorageIdMapClusterj(connector);
      }
    });
    dataAccessMap.put(EncodingStatusDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new EncodingStatusClusterj(connector);
      }
    });
    dataAccessMap.put(BlockLookUpDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new BlockLookUpClusterj(connector);
      }
    });
    dataAccessMap.put(SafeBlocksDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new SafeBlocksClusterj(connector);
      }
    });
    dataAccessMap.put(MisReplicatedRangeQueueDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new MisReplicatedRangeQueueClusterj(connector);
      }
    });
    dataAccessMap.put(QuotaUpdateDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new QuotaUpdateClusterj(connector);
      }
    });
    dataAccessMap.put(PendingEventDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new PendingEventClusterJ(connector);
      }
    });
    dataAccessMap.put(BlockChecksumDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new BlockChecksumClusterj(connector);
      }
    });
    dataAccessMap.put(NextHeartbeatDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new NextHeartbeatClusterJ(connector);
      }
    });
    dataAccessMap.put(RMLoadDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new RMLoadClusterJ(connector);
      }
    });
    dataAccessMap.put(FullRMNodeDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new FullRMNodeClusterJ(connector);
      }
    });
    dataAccessMap.put(MetadataLogDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new MetadataLogClusterj(connector);
      }
    });
    dataAccessMap.put(AccessTimeLogDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new AccessTimeLogClusterj(connector);
      }
    });
    dataAccessMap.put(SizeLogDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new SizeLogClusterj(connector);
      }
    });
    dataAccessMap.put(EncodingJobsDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new EncodingJobsClusterj(connector);
      }
    });
    dataAccessMap.put(RepairJobsDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new RepairJobsClusterj(connector);
      }
    });
    dataAccessMap.put(UserDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new UserClusterj(connector);
      }
    });
    dataAccessMap.put(GroupDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new GroupClusterj(connector);
      }
    });
    dataAccessMap.put(UserGroupDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new UserGroupClusterj(connector);
      }
    });
    dataAccessMap.put(ApplicationAttemptStateDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new ApplicationAttemptStateClusterJ(connector);
      }
    });
    dataAccessMap.put(ApplicationStateDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new ApplicationStateClusterJ(connector);
      }
    });
    dataAccessMap.put(DelegationTokenDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new DelegationTokenClusterJ(connector);
      }
    });
    dataAccessMap.put(DelegationKeyDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new DelegationKeyClusterJ(connector);
      }
    });
    dataAccessMap.put(ProjectQuotaDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new ProjectQuotaClusterJ(connector);
      }
    });
    dataAccessMap.put(ContainersLogsDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new ContainersLogsClusterJ(connector);
      }
    });
    dataAccessMap.put(ContainersCheckPointsDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new ContainersCheckPointsClusterJ(connector);
      }
    });
    dataAccessMap.put(ProjectsDailyCostDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new ProjectsDailyCostClusterJ(connector);
      }
    });
    dataAccessMap.put(PriceMultiplicatorDataAccess.class, new DataAccessBuilder() {
      @Override
      public EntityDataAccess build(ClusterjConnector connector) {
        return new PriceMultiplicatorClusterJ(connector);
      }
    });
  }
}
