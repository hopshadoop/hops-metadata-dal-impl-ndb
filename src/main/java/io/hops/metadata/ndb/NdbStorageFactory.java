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
package io.hops.metadata.ndb;

import io.hops.DalStorageFactory;
import io.hops.StorageConnector;
import io.hops.exception.StorageInitializtionException;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.election.dal.HdfsLeDescriptorDataAccess;
import io.hops.metadata.election.dal.YarnLeDescriptorDataAccess;
import io.hops.metadata.hdfs.dal.AccessTimeLogDataAccess;
import io.hops.metadata.hdfs.dal.BlockChecksumDataAccess;
import io.hops.metadata.hdfs.dal.BlockInfoDataAccess;
import io.hops.metadata.hdfs.dal.BlockLookUpDataAccess;
import io.hops.metadata.hdfs.dal.CorruptReplicaDataAccess;
import io.hops.metadata.hdfs.dal.EncodingJobsDataAccess;
import io.hops.metadata.hdfs.dal.EncodingStatusDataAccess;
import io.hops.metadata.hdfs.dal.ExcessReplicaDataAccess;
import io.hops.metadata.hdfs.dal.GroupDataAccess;
import io.hops.metadata.hdfs.dal.INodeAttributesDataAccess;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.dal.InvalidateBlockDataAccess;
import io.hops.metadata.hdfs.dal.LeaseDataAccess;
import io.hops.metadata.hdfs.dal.LeasePathDataAccess;
import io.hops.metadata.hdfs.dal.MetadataLogDataAccess;
import io.hops.metadata.hdfs.dal.MisReplicatedRangeQueueDataAccess;
import io.hops.metadata.hdfs.dal.OngoingSubTreeOpsDataAccess;
import io.hops.metadata.hdfs.dal.PendingBlockDataAccess;
import io.hops.metadata.hdfs.dal.QuotaUpdateDataAccess;
import io.hops.metadata.hdfs.dal.RepairJobsDataAccess;
import io.hops.metadata.hdfs.dal.ReplicaDataAccess;
import io.hops.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
import io.hops.metadata.hdfs.dal.SafeBlocksDataAccess;
import io.hops.metadata.hdfs.dal.SizeLogDataAccess;
import io.hops.metadata.hdfs.dal.StorageIdMapDataAccess;
import io.hops.metadata.hdfs.dal.UnderReplicatedBlockDataAccess;
import io.hops.metadata.hdfs.dal.UserDataAccess;
import io.hops.metadata.hdfs.dal.UserGroupDataAccess;
import io.hops.metadata.hdfs.dal.VariableDataAccess;
import io.hops.metadata.ndb.dalimpl.election.HdfsLeaderClusterj;
import io.hops.metadata.ndb.dalimpl.election.YarnLeaderClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.AccessTimeLogClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.BlockChecksumClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.BlockInfoClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.BlockLookUpClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.CorruptReplicaClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.EncodingJobsClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.EncodingStatusClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.ExcessReplicaClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.GroupClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.INodeAttributesClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.INodeClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.InvalidatedBlockClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.LeaseClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.LeasePathClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.MetadataLogClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.MisReplicatedRangeQueueClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.OnGoingSubTreeOpsClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.PendingBlockClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.QuotaUpdateClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.RepairJobsClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.ReplicaClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.ReplicaUnderConstructionClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.SafeBlocksClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.SizeLogClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.StorageIdMapClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.UnderReplicatedBlockClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.UserClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.UserGroupClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.VariableClusterj;
import io.hops.metadata.ndb.dalimpl.yarn.AppSchedulingInfoBlacklistClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.AppSchedulingInfoClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.ContainerClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.ContainerIdToCleanClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.ContainerStatusClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.ContainersCheckPointsClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.ContainersLogsClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.FiCaSchedulerNodeClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.FinishedApplicationsClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.FullRMNodeClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.JustLaunchedContainersClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.LaunchedContainersClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.NextHeartbeatClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.NodeClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.NodeHBResponseClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.PendingEventClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.QueueMetricsClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.RMContainerClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.RMContextActiveNodesClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.RMContextInactiveNodesClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.RMLoadClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.RMNodeClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.ResourceClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.ResourceRequestClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.SchedulerApplicationClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.UpdatedContainerInfoClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.YarnVariablesClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppLastScheduledContainerClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppReservationsClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.capacity.FiCaSchedulerAppReservedContainersClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.FiCaSchedulerAppSchedulingOpportunitiesClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.JustFinishedContainersClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.YarnProjectsDailyCostClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.YarnProjectsQuotaClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.capacity.CSLeafQueuesPendingAppsClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.fair.FSSchedulerNodeClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.fair.LocalityLevelClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.fair.PreemptionMapClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.fair.RunnableAppsClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.AllocateRPCClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.AllocateResponseClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.AllocatedContainersClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.ApplicationAttemptStateClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.ApplicationStateClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.CompletedContainersStatusClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.DelegationKeyClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.DelegationTokenClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.HeartBeatRPCClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.RMStateVersionClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.RPCClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.RanNodeClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.SecretMamagerKeysClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.SequenceNumberClusterJ;
import io.hops.metadata.ndb.dalimpl.yarn.rmstatestore.UpdatedNodeClusterJ;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.yarn.dal.AppSchedulingInfoBlacklistDataAccess;
import io.hops.metadata.yarn.dal.AppSchedulingInfoDataAccess;
import io.hops.metadata.yarn.dal.ContainerDataAccess;
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.ContainersCheckPointsDataAccess;
import io.hops.metadata.yarn.dal.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.FullRMNodeDataAccess;
import io.hops.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.LaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.dal.NodeDataAccess;
import io.hops.metadata.yarn.dal.NodeHBResponseDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.QueueMetricsDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.RMContextActiveNodesDataAccess;
import io.hops.metadata.yarn.dal.RMContextInactiveNodesDataAccess;
import io.hops.metadata.yarn.dal.RMLoadDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.ResourceRequestDataAccess;
import io.hops.metadata.yarn.dal.SchedulerApplicationDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.dal.YarnVariablesDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppLastScheduledContainerDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppReservationsDataAccess;
import io.hops.metadata.yarn.dal.capacity.FiCaSchedulerAppReservedContainersDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppSchedulingOpportunitiesDataAccess;
import io.hops.metadata.yarn.dal.JustFinishedContainersDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsQuotaDataAccess;
import io.hops.metadata.yarn.dal.capacity.CSLeafQueuesPendingAppsDataAccess;
import io.hops.metadata.yarn.dal.fair.FSSchedulerNodeDataAccess;
import io.hops.metadata.yarn.dal.fair.LocalityLevelDataAccess;
import io.hops.metadata.yarn.dal.fair.PreemptionMapDataAccess;
import io.hops.metadata.yarn.dal.fair.RunnableAppsDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.AllocateRPCDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.AllocateResponseDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.AllocatedContainersDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationStateDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.CompletedContainersStatusDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationKeyDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.DelegationTokenDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.HeartBeatRPCDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.RMStateVersionDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.RPCDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.RanNodeDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.SecretMamagerKeysDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.SequenceNumberDataAccess;
import io.hops.metadata.yarn.dal.rmstatestore.UpdatedNodeDataAccess;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class NdbStorageFactory implements DalStorageFactory {

  private Map<Class, EntityDataAccess> dataAccessMap =
      new HashMap<Class, EntityDataAccess>();

  @Override
  public void setConfiguration(Properties conf)
          throws StorageInitializtionException {
    try {
      ClusterjConnector.getInstance().setConfiguration(conf);
      MysqlServerConnector.getInstance().setConfiguration(conf);
      initDataAccessMap();
    } catch (IOException ex) {
      throw new StorageInitializtionException(ex);
    }
  }

  private void initDataAccessMap() {
    dataAccessMap
            .put(RMStateVersionDataAccess.class, new RMStateVersionClusterJ());
    dataAccessMap
            .put(ApplicationStateDataAccess.class, new ApplicationStateClusterJ());
    dataAccessMap.put(UpdatedNodeDataAccess.class, new UpdatedNodeClusterJ());
    dataAccessMap.put(ApplicationAttemptStateDataAccess.class,
            new ApplicationAttemptStateClusterJ());
    dataAccessMap.put(RanNodeDataAccess.class, new RanNodeClusterJ());
    dataAccessMap
            .put(DelegationTokenDataAccess.class, new DelegationTokenClusterJ());
    dataAccessMap
            .put(SequenceNumberDataAccess.class, new SequenceNumberClusterJ());
    dataAccessMap
            .put(DelegationKeyDataAccess.class, new DelegationKeyClusterJ());
    dataAccessMap
            .put(YarnVariablesDataAccess.class, new YarnVariablesClusterJ());
    dataAccessMap.put(RPCDataAccess.class, new RPCClusterJ());
    dataAccessMap.put(HeartBeatRPCDataAccess.class, new HeartBeatRPCClusterJ());
    dataAccessMap.put(AllocateRPCDataAccess.class, new AllocateRPCClusterJ());
    dataAccessMap.put(QueueMetricsDataAccess.class, new QueueMetricsClusterJ());
    dataAccessMap.put(FiCaSchedulerNodeDataAccess.class,
            new FiCaSchedulerNodeClusterJ());
    dataAccessMap.put(ResourceDataAccess.class, new ResourceClusterJ());
    dataAccessMap.put(NodeDataAccess.class, new NodeClusterJ());
    dataAccessMap.put(ResourceDataAccess.class, new ResourceClusterJ());
    dataAccessMap.put(RMNodeDataAccess.class, new RMNodeClusterJ());
    dataAccessMap.put(RMContextActiveNodesDataAccess.class,
            new RMContextActiveNodesClusterJ());
    dataAccessMap.put(RMContextInactiveNodesDataAccess.class,
            new RMContextInactiveNodesClusterJ());
    dataAccessMap
            .put(ContainerStatusDataAccess.class, new ContainerStatusClusterJ());
    dataAccessMap
            .put(ContainersLogsDataAccess.class, new ContainersLogsClusterJ());
    dataAccessMap
            .put(NodeHBResponseDataAccess.class, new NodeHBResponseClusterJ());
    dataAccessMap.put(UpdatedContainerInfoDataAccess.class,
            new UpdatedContainerInfoClusterJ());
    dataAccessMap.put(ContainerIdToCleanDataAccess.class,
            new ContainerIdToCleanClusterJ());
    dataAccessMap.put(JustLaunchedContainersDataAccess.class,
            new JustLaunchedContainersClusterJ());
    dataAccessMap.put(LaunchedContainersDataAccess.class,
            new LaunchedContainersClusterJ());
    dataAccessMap.put(FinishedApplicationsDataAccess.class,
            new FinishedApplicationsClusterJ());
    dataAccessMap.put(SchedulerApplicationDataAccess.class,
            new SchedulerApplicationClusterJ());
    dataAccessMap.put(FiCaSchedulerAppSchedulingOpportunitiesDataAccess.class,
            new FiCaSchedulerAppSchedulingOpportunitiesClusterJ());
    dataAccessMap.put(FiCaSchedulerAppLastScheduledContainerDataAccess.class,
            new FiCaSchedulerAppLastScheduledContainerClusterJ());
    dataAccessMap.put(FiCaSchedulerAppReservedContainersDataAccess.class,
            new FiCaSchedulerAppReservedContainersClusterJ());
    dataAccessMap.put(FiCaSchedulerAppReservationsDataAccess.class,
            new FiCaSchedulerAppReservationsClusterJ());
    dataAccessMap.put(RMContainerDataAccess.class, new RMContainerClusterJ());
    dataAccessMap.put(ContainerDataAccess.class, new ContainerClusterJ());
    dataAccessMap.put(AppSchedulingInfoDataAccess.class,
            new AppSchedulingInfoClusterJ());
    dataAccessMap.put(AppSchedulingInfoBlacklistDataAccess.class,
            new AppSchedulingInfoBlacklistClusterJ());
    dataAccessMap
            .put(ResourceRequestDataAccess.class, new ResourceRequestClusterJ());
    dataAccessMap.put(BlockInfoDataAccess.class, new BlockInfoClusterj());
    dataAccessMap.put(PendingBlockDataAccess.class, new PendingBlockClusterj());
    dataAccessMap.put(ReplicaUnderConstructionDataAccess.class,
            new ReplicaUnderConstructionClusterj());
    dataAccessMap.put(INodeDataAccess.class, new INodeClusterj());
    dataAccessMap
            .put(INodeAttributesDataAccess.class, new INodeAttributesClusterj());
    dataAccessMap.put(LeaseDataAccess.class, new LeaseClusterj());
    dataAccessMap.put(LeasePathDataAccess.class, new LeasePathClusterj());
    dataAccessMap.put(OngoingSubTreeOpsDataAccess.class, new OnGoingSubTreeOpsClusterj());
    dataAccessMap
            .put(HdfsLeDescriptorDataAccess.class, new HdfsLeaderClusterj());
    dataAccessMap
            .put(YarnLeDescriptorDataAccess.class, new YarnLeaderClusterj());
    dataAccessMap.put(ReplicaDataAccess.class, new ReplicaClusterj());
    dataAccessMap
            .put(CorruptReplicaDataAccess.class, new CorruptReplicaClusterj());
    dataAccessMap
            .put(ExcessReplicaDataAccess.class, new ExcessReplicaClusterj());
    dataAccessMap
            .put(InvalidateBlockDataAccess.class, new InvalidatedBlockClusterj());
    dataAccessMap.put(UnderReplicatedBlockDataAccess.class,
            new UnderReplicatedBlockClusterj());
    dataAccessMap.put(VariableDataAccess.class, new VariableClusterj());
    dataAccessMap.put(StorageIdMapDataAccess.class, new StorageIdMapClusterj());
    dataAccessMap
            .put(EncodingStatusDataAccess.class, new EncodingStatusClusterj() {
            });
    dataAccessMap.put(BlockLookUpDataAccess.class, new BlockLookUpClusterj());
    dataAccessMap
            .put(FSSchedulerNodeDataAccess.class, new FSSchedulerNodeClusterJ());
    dataAccessMap.put(SafeBlocksDataAccess.class, new SafeBlocksClusterj());
    dataAccessMap.put(MisReplicatedRangeQueueDataAccess.class,
            new MisReplicatedRangeQueueClusterj());
    dataAccessMap.put(QuotaUpdateDataAccess.class, new QuotaUpdateClusterj());
    dataAccessMap.put(SecretMamagerKeysDataAccess.class,
            new SecretMamagerKeysClusterJ());
    dataAccessMap
            .put(AllocateResponseDataAccess.class, new AllocateResponseClusterJ());
    dataAccessMap
            .put(AllocatedContainersDataAccess.class, new AllocatedContainersClusterJ());
    dataAccessMap.
            put(CompletedContainersStatusDataAccess.class,
                    new CompletedContainersStatusClusterJ());
    dataAccessMap.put(PendingEventDataAccess.class, new PendingEventClusterJ());
    dataAccessMap
            .put(BlockChecksumDataAccess.class, new BlockChecksumClusterj());
    dataAccessMap
            .put(NextHeartbeatDataAccess.class, new NextHeartbeatClusterJ());
    dataAccessMap.put(RMLoadDataAccess.class, new RMLoadClusterJ());
    dataAccessMap.put(FullRMNodeDataAccess.class, new FullRMNodeClusterJ());
    dataAccessMap.put(MetadataLogDataAccess.class, new MetadataLogClusterj());
    dataAccessMap.put(AccessTimeLogDataAccess.class,
            new AccessTimeLogClusterj());
    dataAccessMap.put(SizeLogDataAccess.class, new SizeLogClusterj());
    dataAccessMap.put(EncodingJobsDataAccess.class, new EncodingJobsClusterj());
    dataAccessMap.put(RepairJobsDataAccess.class, new RepairJobsClusterj());
    dataAccessMap.
            put(LocalityLevelDataAccess.class, new LocalityLevelClusterJ());
    dataAccessMap.put(RunnableAppsDataAccess.class, new RunnableAppsClusterJ());
    dataAccessMap.
            put(PreemptionMapDataAccess.class, new PreemptionMapClusterJ());
    dataAccessMap.put(UserDataAccess.class, new UserClusterj());
    dataAccessMap.put(GroupDataAccess.class, new GroupClusterj());
    dataAccessMap.put(UserGroupDataAccess.class, new UserGroupClusterj());
    dataAccessMap.put(CSLeafQueuesPendingAppsDataAccess.class,
            new CSLeafQueuesPendingAppsClusterJ());
    dataAccessMap.put(JustFinishedContainersDataAccess.class,
            new JustFinishedContainersClusterJ());
    // Quota Scheduling
    dataAccessMap.put(YarnProjectsQuotaDataAccess.class, new YarnProjectsQuotaClusterJ());
    dataAccessMap.put(YarnProjectsDailyCostDataAccess.class, new YarnProjectsDailyCostClusterJ());
    dataAccessMap.put(ContainersCheckPointsDataAccess.class, new ContainersCheckPointsClusterJ());
  }

  @Override
  public StorageConnector getConnector() {
    return ClusterjConnector.getInstance();
  }

  @Override
  public EntityDataAccess getDataAccess(Class type) {
    return dataAccessMap.get(type);
  }
  }
