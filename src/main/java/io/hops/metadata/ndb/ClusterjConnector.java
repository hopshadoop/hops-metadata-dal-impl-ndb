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

import com.mysql.clusterj.Constants;
import com.mysql.clusterj.LockMode;
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.election.TablesDef;
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
import io.hops.metadata.ndb.dalimpl.hdfs.BlockChecksumClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.BlockInfoClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.CorruptReplicaClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.EncodingStatusClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.ExcessReplicaClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.INodeAttributesClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.INodeClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.InvalidatedBlockClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.LeaseClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.LeasePathClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.OnGoingSubTreeOpsClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.PendingBlockClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.QuotaUpdateClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.ReplicaClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.ReplicaUnderConstructionClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.UnderReplicatedBlockClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.VariableClusterj;
import io.hops.metadata.ndb.dalimpl.yarn.YarnVariablesClusterJ;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.ndb.wrapper.HopsTransaction;
import io.hops.metadata.yarn.dal.AppSchedulingInfoBlacklistDataAccess;
import io.hops.metadata.yarn.dal.AppSchedulingInfoDataAccess;
import io.hops.metadata.yarn.dal.ContainerDataAccess;
import io.hops.metadata.yarn.dal.ContainerIdToCleanDataAccess;
import io.hops.metadata.yarn.dal.ContainerStatusDataAccess;
import io.hops.metadata.yarn.dal.ContainersCheckPointsDataAccess;
import io.hops.metadata.yarn.dal.ContainersLogsDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerNodeDataAccess;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.JustLaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.LaunchedContainersDataAccess;
import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.dal.NodeDataAccess;
import io.hops.metadata.yarn.dal.PendingEventDataAccess;
import io.hops.metadata.yarn.dal.QueueMetricsDataAccess;
import io.hops.metadata.yarn.dal.RMContainerDataAccess;
import io.hops.metadata.yarn.dal.RMContextActiveNodesDataAccess;
import io.hops.metadata.yarn.dal.RMContextInactiveNodesDataAccess;
import io.hops.metadata.yarn.dal.RMLoadDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.ResourceDataAccess;
import io.hops.metadata.yarn.dal.ResourceRequestDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppLastScheduledContainerDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppReservationsDataAccess;
import io.hops.metadata.yarn.dal.FiCaSchedulerAppSchedulingOpportunitiesDataAccess;
import io.hops.metadata.yarn.dal.JustFinishedContainersDataAccess;
import io.hops.metadata.yarn.dal.NodeHBResponseDataAccess;
import io.hops.metadata.yarn.dal.SchedulerApplicationDataAccess;
import io.hops.metadata.yarn.dal.UpdatedContainerInfoDataAccess;
import io.hops.metadata.yarn.dal.YarnHistoryPriceDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.dal.YarnProjectsQuotaDataAccess;
import io.hops.metadata.yarn.dal.YarnRunningPriceDataAccess;
import io.hops.metadata.yarn.dal.YarnVariablesDataAccess;
import io.hops.metadata.yarn.dal.capacity.CSLeafQueuesPendingAppsDataAccess;
import io.hops.metadata.yarn.dal.capacity.FiCaSchedulerAppReservedContainersDataAccess;
import io.hops.metadata.yarn.dal.fair.LocalityLevelDataAccess;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.util.Properties;

public class ClusterjConnector implements StorageConnector<DBSession> {

  private final static ClusterjConnector instance = new ClusterjConnector();
  private static boolean isInitialized = false;
  private DBSessionProvider dbSessionProvider = null;
  static ThreadLocal<DBSession> sessions = new ThreadLocal<DBSession>();
  static final Log LOG = LogFactory.getLog(ClusterjConnector.class);
  private String clusterConnectString;
  private String databaseName;
  
  private ClusterjConnector() {
  }

  public static ClusterjConnector getInstance() {
    return instance;
  }

  @Override
  public void setConfiguration(Properties conf) throws StorageException {
    if (isInitialized) {
      LOG.warn("SessionFactory is already initialized");
      return;
    }
    
    clusterConnectString = (String) conf.get(Constants.PROPERTY_CLUSTER_CONNECTSTRING);
    LOG.info("Database connect string: " +
        conf.get(Constants.PROPERTY_CLUSTER_CONNECTSTRING));
    databaseName = (String) conf.get(Constants.PROPERTY_CLUSTER_DATABASE);
    LOG.info("Database name: " + conf.get(Constants.PROPERTY_CLUSTER_DATABASE));
    LOG.info("Max Transactions: " +
        conf.get(Constants.PROPERTY_CLUSTER_MAX_TRANSACTIONS));

    int initialPoolSize =
        Integer.parseInt((String) conf.get("io.hops.session.pool.size"));
    int reuseCount =
        Integer.parseInt((String) conf.get("io.hops.session.reuse.count"));
    dbSessionProvider =
        new DBSessionProvider(conf, reuseCount, initialPoolSize);
    
    isInitialized = true;
  }

  /*
   * Return a dbSession from a random dbSession factory in our pool.
   *
   * NOTE: Do not close the dbSession returned by this call or you will die.
   */
  @Override
  public HopsSession obtainSession() throws StorageException {
    DBSession dbSession = sessions.get();
    if (dbSession == null) {
      dbSession = dbSessionProvider.getSession();
      sessions.set(dbSession);
    }
    return dbSession.getSession();
  }

  private void returnSession(boolean error) throws StorageException {
    DBSession dbSession = sessions.get();
    sessions.remove(); // remove, and return to the pool
    dbSessionProvider.returnSession(dbSession,
        error); // if there was an error then close the session
  }

  /**
   * begin a transaction.
   *
   * @throws io.hops.exception.StorageException
   */
  @Override
  public void beginTransaction() throws StorageException {
    HopsSession session = obtainSession();
    if (session.currentTransaction().isActive()) {
      LOG.fatal("Prevented starting transaction within a transaction.");
      throw new Error("Can not start Tx inside another Tx");
    }
    session.currentTransaction().begin();
  }

  /**
   * Commit a transaction.
   *
   * @throws io.hops.exception.StorageException
   */
  @Override
  public void commit() throws StorageException {
    HopsSession session = null;
    boolean dbError = false;
    try {
      session = obtainSession();
      HopsTransaction tx = session.currentTransaction();
      if (!tx.isActive()) {
        throw new StorageException("The transaction is not began!");
      }
      tx.commit();
    } catch (StorageException e) {
      dbError = true;
      throw e;
    } finally {
      returnSession(dbError);
    }
  }
 
  /**
   * It rolls back only when the transaction is active.
   */
  @Override
  public void rollback() throws StorageException {
    HopsSession session = null;
    boolean dbError = false;
    try {
      session = obtainSession();
      HopsTransaction tx = session.currentTransaction();
      if (tx.isActive()) {
        tx.rollback();
      }
    } catch (StorageException e) {
      dbError = true;
      throw e;
    } finally {
      returnSession(dbError);
    }
  }

  /**
   * This is called only when MiniDFSCluster wants to format the Namenode.
   */
  @Override
  public boolean formatStorage() throws StorageException {
    return formatAll(true);
  }

  @Override
  public boolean formatYarnStorage() throws StorageException {
    return formatYarn(true);
  }

  @Override
  public boolean formatHDFSStorage() throws StorageException {
    return formatHDFS(true);
  }

  @Override
  public boolean formatStorage(Class<? extends EntityDataAccess>... das)
      throws StorageException {
    return format(true, das);
  }


  @Override
  public boolean isTransactionActive() throws StorageException {
    return obtainSession().currentTransaction().isActive();
  }

  @Override
  public void stopStorage() throws StorageException {
    dbSessionProvider.stop();
  }

  @Override
  public void readLock() throws StorageException {
    HopsSession session = obtainSession();
    session.setLockMode(LockMode.SHARED);
  }

  @Override
  public void writeLock() throws StorageException {
    HopsSession session = obtainSession();
    session.setLockMode(LockMode.EXCLUSIVE);
  }

  @Override
  public void readCommitted() throws StorageException {
    HopsSession session = obtainSession();
    session.setLockMode(LockMode.READ_COMMITTED);
  }

  @Override
  public void setPartitionKey(Class className, Object key)
      throws StorageException {
    Class cls = null;
    if (className == BlockInfoDataAccess.class) {
      cls = BlockInfoClusterj.BlockInfoDTO.class;
    } else if (className == PendingBlockDataAccess.class) {
      cls = PendingBlockClusterj.PendingBlockDTO.class;
    } else if (className == ReplicaUnderConstructionDataAccess.class) {
      cls = ReplicaUnderConstructionClusterj.ReplicaUcDTO.class;
    } else if (className == INodeDataAccess.class) {
      cls = INodeClusterj.InodeDTO.class;
    } else if (className == INodeAttributesDataAccess.class) {
      cls = INodeAttributesClusterj.INodeAttributesDTO.class;
    } else if (className == LeaseDataAccess.class) {
      cls = LeaseClusterj.LeaseDTO.class;
    } else if (className == LeasePathDataAccess.class) {
      cls = LeasePathClusterj.LeasePathsDTO.class;
    } else if (className == HdfsLeDescriptorDataAccess.class) {
      cls = HdfsLeaderClusterj.HdfsLeaderDTO.class;
    } else if (className == YarnLeDescriptorDataAccess.class) {
      cls = YarnLeaderClusterj.YarnLeaderDTO.class;
    } else if (className == ReplicaDataAccess.class) {
      cls = ReplicaClusterj.ReplicaDTO.class;
    } else if (className == CorruptReplicaDataAccess.class) {
      cls = CorruptReplicaClusterj.CorruptReplicaDTO.class;
    } else if (className == ExcessReplicaDataAccess.class) {
      cls = ExcessReplicaClusterj.ExcessReplicaDTO.class;
    } else if (className == InvalidateBlockDataAccess.class) {
      cls = InvalidatedBlockClusterj.InvalidateBlocksDTO.class;
    } else if (className == UnderReplicatedBlockDataAccess.class) {
      cls = UnderReplicatedBlockClusterj.UnderReplicatedBlocksDTO.class;
    } else if (className == VariableDataAccess.class) {
      cls = VariableClusterj.VariableDTO.class;
    } else if (className == QuotaUpdateDataAccess.class) {
      cls = QuotaUpdateClusterj.QuotaUpdateDTO.class;
    } else if (className == EncodingStatusDataAccess.class) {
      cls = EncodingStatusClusterj.EncodingStatusDto.class;
    } else if (className == BlockChecksumDataAccess.class) {
      cls = BlockChecksumClusterj.BlockChecksumDto.class;
    } else if (className == OngoingSubTreeOpsDataAccess.class) {
      cls = OnGoingSubTreeOpsClusterj.OnGoingSubTreeOpsDTO.class;
    }

    HopsSession session = obtainSession();
    session.setPartitionKey(cls, key);
    session.flush();
  }

  @Override
  public boolean formatAllStorageNonTransactional() throws StorageException {
    return formatAll(false);
  }

  @Override
  public boolean formatYarnStorageNonTransactional() throws StorageException {
    return formatAll(false);
  }

  @Override
  public boolean formatHDFSStorageNonTransactional() throws StorageException {
    return formatHDFS(false);
  }

  private boolean formatYarn(boolean transactional) throws StorageException{
    return format(transactional,
    RPCDataAccess.class, HeartBeatRPCDataAccess.class,
        AllocateRPCDataAccess.class,
        ApplicationStateDataAccess.class,
        UpdatedNodeDataAccess.class,
        ApplicationAttemptStateDataAccess.class,
        RanNodeDataAccess.class,
        DelegationKeyDataAccess.class,
        DelegationTokenDataAccess.class, SequenceNumberDataAccess.class,
        RMStateVersionDataAccess.class, YarnVariablesDataAccess.class,
        AppSchedulingInfoDataAccess.class,
        AppSchedulingInfoBlacklistDataAccess.class, ContainerDataAccess.class,
        ContainerIdToCleanDataAccess.class, ContainerStatusDataAccess.class,
        ContainersLogsDataAccess.class,
        FiCaSchedulerAppLastScheduledContainerDataAccess.class,
        FiCaSchedulerAppReservationsDataAccess.class,
        FiCaSchedulerAppReservedContainersDataAccess.class,
        FiCaSchedulerAppSchedulingOpportunitiesDataAccess.class,
        FiCaSchedulerNodeDataAccess.class,
        JustLaunchedContainersDataAccess.class,
        LaunchedContainersDataAccess.class, NodeDataAccess.class,
        QueueMetricsDataAccess.class, ResourceDataAccess.class,
        ResourceRequestDataAccess.class, RMContainerDataAccess.class,
        RMNodeDataAccess.class, SchedulerApplicationDataAccess.class,
        SequenceNumberDataAccess.class, FinishedApplicationsDataAccess.class,
        RMContextInactiveNodesDataAccess.class,
        RMContextActiveNodesDataAccess.class,
        UpdatedContainerInfoDataAccess.class, YarnLeDescriptorDataAccess.class,
        SecretMamagerKeysDataAccess.class, AllocateResponseDataAccess.class,
        AllocatedContainersDataAccess.class, CompletedContainersStatusDataAccess.class,
        RMLoadDataAccess.class, PendingEventDataAccess.class,
        LocalityLevelDataAccess.class,
        RunnableAppsDataAccess.class,
        NextHeartbeatDataAccess.class,
        NodeHBResponseDataAccess.class, 
        ContainersCheckPointsDataAccess.class,
        CSLeafQueuesPendingAppsDataAccess.class,
        JustFinishedContainersDataAccess.class);
  }
  
  private boolean formatHDFS(boolean transactional) throws StorageException{
    return format(transactional,
        INodeDataAccess.class, BlockInfoDataAccess.class, LeaseDataAccess.class,
        LeasePathDataAccess.class, ReplicaDataAccess.class,
        ReplicaUnderConstructionDataAccess.class,
        InvalidateBlockDataAccess.class, ExcessReplicaDataAccess.class,
        PendingBlockDataAccess.class, CorruptReplicaDataAccess.class,
        UnderReplicatedBlockDataAccess.class, HdfsLeDescriptorDataAccess.class,
        INodeAttributesDataAccess.class, StorageIdMapDataAccess.class,
        BlockLookUpDataAccess.class, SafeBlocksDataAccess.class,
        MisReplicatedRangeQueueDataAccess.class, QuotaUpdateDataAccess.class,
        EncodingStatusDataAccess.class, BlockChecksumDataAccess.class,
        OngoingSubTreeOpsDataAccess.class,
        MetadataLogDataAccess.class, AccessTimeLogDataAccess.class,
        SizeLogDataAccess.class, EncodingJobsDataAccess.class,
        RepairJobsDataAccess.class, UserDataAccess.class, GroupDataAccess.class,
        UserGroupDataAccess.class,VariableDataAccess.class);
  }
  
  private boolean formatAll(boolean transactional) throws StorageException {
    //HDFS
    if (!formatHDFS(transactional)) {
      return false;
    }
    //YARN
    if (!formatYarn(transactional)) {
      return false;
    }

    // shared
    return format(transactional,
            VariableDataAccess.class,
            YarnProjectsQuotaDataAccess.class,
            YarnProjectsDailyCostDataAccess.class,
            YarnHistoryPriceDataAccess.class,
            YarnRunningPriceDataAccess.class
    );
  }

  private boolean format(boolean transactional,
      Class<? extends EntityDataAccess>... das) throws StorageException {
    
    final int RETRIES = 5; // in test
    for (int i = 0; i < RETRIES; i++) {
      try {
        for (Class e : das) {
          if (e == INodeDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.INodeTableDef.TABLE_NAME);
          } else if (e == BlockInfoDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.BlockInfoTableDef.TABLE_NAME);
          } else if (e == LeaseDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.LeaseTableDef.TABLE_NAME);
          } else if (e == LeasePathDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.LeasePathTableDef.TABLE_NAME);
          } else if (e == OngoingSubTreeOpsDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.OnGoingSubTreeOpsDef.TABLE_NAME);
          } else if (e == ReplicaDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.ReplicaTableDef.TABLE_NAME);
          } else if (e == ReplicaUnderConstructionDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.ReplicaUnderConstructionTableDef.TABLE_NAME);
          } else if (e == InvalidateBlockDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.InvalidatedBlockTableDef.TABLE_NAME);
          } else if (e == ExcessReplicaDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.ExcessReplicaTableDef.TABLE_NAME);
          } else if (e == PendingBlockDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.PendingBlockTableDef.TABLE_NAME);
          } else if (e == CorruptReplicaDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.CorruptReplicaTableDef.TABLE_NAME);
          } else if (e == UnderReplicatedBlockDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.UnderReplicatedBlockTableDef.TABLE_NAME);
          } else if (e == HdfsLeDescriptorDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, TablesDef.HdfsLeaderTableDef.TABLE_NAME);
          } else if (e == INodeAttributesDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.INodeAttributesTableDef.TABLE_NAME);
          } else if (e == VariableDataAccess.class) {
            HopsSession session = obtainSession();
            session.currentTransaction().begin();
            session.deletePersistentAll(VariableClusterj.VariableDTO.class);
            for (Variable.Finder varType : Variable.Finder.values()) {
              VariableClusterj.VariableDTO vd =
                  session.newInstance(VariableClusterj.VariableDTO.class);
              vd.setId(varType.getId());
              vd.setValue(varType.getDefaultValue());
              session.savePersistent(vd);
            }
            session.currentTransaction().commit();
          } else if (e == StorageIdMapDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.StorageIdMapTableDef.TABLE_NAME);
          } else if (e == BlockLookUpDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.BlockLookUpTableDef.TABLE_NAME);
          } else if (e == SafeBlocksDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.SafeBlocksTableDef.TABLE_NAME);
          } else if (e == MisReplicatedRangeQueueDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.MisReplicatedRangeQueueTableDef.TABLE_NAME);
          } else if (e == QuotaUpdateDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.QuotaUpdateTableDef.TABLE_NAME);
          } else if (e == EncodingStatusDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.EncodingStatusTableDef.TABLE_NAME);
          } else if (e == BlockChecksumDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.BlockChecksumTableDef.TABLE_NAME);
          } else if (e == MetadataLogDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.MetadataLogTableDef.TABLE_NAME);
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.MetadataLogTableDef.LOOKUP_TABLE_NAME);
          } else if (e == AccessTimeLogDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.AccessTimeLogTableDef.TABLE_NAME);
          } else if (e == SizeLogDataAccess.class) {
            MysqlServerConnector.truncateTable(transactional,
                io.hops.metadata.hdfs.TablesDef.SizeLogTableDef.TABLE_NAME);
          } else if (e == EncodingJobsDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.EncodingJobsTableDef.TABLE_NAME);
          } else if (e == RepairJobsDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.RepairJobsTableDef.TABLE_NAME);
          } else if (e == UserDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.UsersTableDef.TABLE_NAME);
          }else if (e == GroupDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.GroupsTableDef.TABLE_NAME);
          }else if (e == UserGroupDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional, io.hops.metadata.hdfs.TablesDef.UsersGroupsTableDef.TABLE_NAME);
          } else if (e == YarnLeDescriptorDataAccess.class) {
            MysqlServerConnector
                .truncateTable(transactional,
                    TablesDef.YarnLeaderTableDef.TABLE_NAME);
          } else if (e == AppSchedulingInfoDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.AppSchedulingInfoTableDef.TABLE_NAME);
          } else if (e == AppSchedulingInfoBlacklistDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.AppSchedulingInfoBlacklistTableDef.TABLE_NAME);
          } else if (e == ContainerDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.ContainerTableDef.TABLE_NAME);
          } else if (e == ContainerIdToCleanDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.ContainerIdToCleanTableDef.TABLE_NAME);
          } else if (e == ContainerStatusDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.ContainerStatusTableDef.TABLE_NAME);
          } else if (e == ContainersLogsDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.ContainersLogsTableDef.TABLE_NAME);
          }else if(e==YarnProjectsQuotaDataAccess.class) {
            truncate(transactional,
                    io.hops.metadata.yarn.TablesDef.YarnProjectsQuotaTableDef.TABLE_NAME);
          } else if (e == YarnProjectsDailyCostDataAccess.class) {
            truncate(transactional,
                    io.hops.metadata.yarn.TablesDef.YarnProjectsDailyCostTableDef.TABLE_NAME);
          } else if (e == ContainersCheckPointsDataAccess.class) {
            truncate(transactional,
                    io.hops.metadata.yarn.TablesDef.ContainersCheckPointsTableDef.TABLE_NAME);
          } else if (e ==
              FiCaSchedulerAppLastScheduledContainerDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.FiCaSchedulerAppLastScheduledContainerTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppReservationsDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.FiCaSchedulerAppReservationsTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerAppReservedContainersDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.FiCaSchedulerAppReservedContainersTableDef.TABLE_NAME);
          } else if (e ==
              FiCaSchedulerAppSchedulingOpportunitiesDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.FiCaSchedulerAppSchedulingOpportunitiesTableDef.TABLE_NAME);
          } else if (e == FiCaSchedulerNodeDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.FiCaSchedulerNodeTableDef.TABLE_NAME);
          } else if (e == JustLaunchedContainersDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.JustLaunchedContainersTableDef.TABLE_NAME);
          } else if (e == LaunchedContainersDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.LaunchedContainersTableDef.TABLE_NAME);
          } else if (e == NodeDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.NodeTableDef.TABLE_NAME);
          } else if (e == QueueMetricsDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.QueueMetricsTableDef.TABLE_NAME);
          } else if (e == ResourceDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.ResourceTableDef.TABLE_NAME);
          } else if (e == ResourceRequestDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.ResourceRequestTableDef.TABLE_NAME);
          } else if (e == RMContainerDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.RMContainerTableDef.TABLE_NAME);
          } else if (e == RMNodeDataAccess.class) {
            // Truncate does not work with foreign keys
            truncate(true,
                io.hops.metadata.yarn.TablesDef.RMNodeTableDef.TABLE_NAME);
          } else if (e == SchedulerApplicationDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.SchedulerApplicationTableDef.TABLE_NAME);
          } else if (e == SequenceNumberDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.SequenceNumberTableDef.TABLE_NAME);
          } else if (e == FinishedApplicationsDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.FinishedApplicationsTableDef.TABLE_NAME);
          } else if (e == RMContextInactiveNodesDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.RMContextInactiveNodesTableDef.TABLE_NAME);
          } else if (e == RMContextActiveNodesDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.RMContextActiveNodesTableDef.TABLE_NAME);
          } else if (e == UpdatedContainerInfoDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.UpdatedContainerInfoTableDef.TABLE_NAME);
          } else if (e == SecretMamagerKeysDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.SecretMamagerKeysTableDef.TABLE_NAME);
          } else if (e == AllocateResponseDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.AllocateResponseTableDef.TABLE_NAME);
          } else if (e == AllocatedContainersDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.AllocatedContainersTableDef.TABLE_NAME);
          } else if (e == CompletedContainersStatusDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.CompletedContainersStatusTableDef.TABLE_NAME);
          } else if (e == DelegationKeyDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.DelegationKeyTableDef.TABLE_NAME);
          } else if (e == DelegationTokenDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.DelegationTokenTableDef.TABLE_NAME);
          } else if (e == RMStateVersionDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.RMStateVersionTableDef.TABLE_NAME);
          } else if (e == ApplicationAttemptStateDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.ApplicationAttemptStateTableDef.TABLE_NAME);
          } else if (e == ApplicationStateDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.ApplicationStateTableDef.TABLE_NAME);
          } else if (e == RanNodeDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.RanNodeTableDef.TABLE_NAME);
          }else if (e == UpdatedNodeDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.UpdatedNodeTableDef.TABLE_NAME);
          }else if (e == RPCDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.RPCTableDef.TABLE_NAME);
          } else if (e == HeartBeatRPCDataAccess.class) {
            truncate(transactional,
                    io.hops.metadata.yarn.TablesDef.HeartBeatContainerStatusesTableDef.TABLE_NAME);
            truncate(transactional,
                    io.hops.metadata.yarn.TablesDef.HeartBeatKeepAliveApplications.TABLE_NAME);
            truncate(transactional,
                    io.hops.metadata.yarn.TablesDef.HeartBeatRPCTableDef.TABLE_NAME);
          } else if (e == HeartBeatRPCDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.AllocateRPC.TABLE_NAME);
            truncate(transactional, io.hops.metadata.yarn.TablesDef.AllocateRPCAsk.TABLE_NAME);
            truncate(transactional, io.hops.metadata.yarn.TablesDef.AllocateRPCBlackListAdd.TABLE_NAME);
            truncate(transactional, io.hops.metadata.yarn.TablesDef.AllocateRPCBlackListRemove.TABLE_NAME);
            truncate(transactional, io.hops.metadata.yarn.TablesDef.AllocateRPCRelease.TABLE_NAME);
            truncate(transactional, io.hops.metadata.yarn.TablesDef.AllocateRPCResourceIncrease.TABLE_NAME);
          } else if (e == RMLoadDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.RMLoadTableDef.TABLE_NAME);
          } else if (e == NodeHBResponseDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.NodeHBResponseTableDef.TABLE_NAME);
          }else if (e == YarnVariablesDataAccess.class) {
            HopsSession session = obtainSession();
            session.currentTransaction().begin();
            session.deletePersistentAll(
                YarnVariablesClusterJ.YarnVariablesDTO.class);
            for (int j = 0; j <= 19; j++) {
              YarnVariablesClusterJ.YarnVariablesDTO vd = session
                  .newInstance(YarnVariablesClusterJ.YarnVariablesDTO.class);
              vd.setid(j);
              vd.setvalue(0);
              session.savePersistent(vd);
            }
            session.currentTransaction().commit();
          } else if (e == PendingEventDataAccess.class) {
            truncate(transactional,
                io.hops.metadata.yarn.TablesDef.PendingEventTableDef.TABLE_NAME);
          } else if (e == NextHeartbeatDataAccess.class) {
            truncate(transactional, io.hops.metadata.yarn.TablesDef.NextHeartbeatTableDef.TABLE_NAME);
          } else if (e==LocalityLevelDataAccess.class){
            truncate(transactional, io.hops.metadata.yarn.TablesDef.LocalityLevelTableDef.TABLE_NAME);
          } else if (e== RunnableAppsDataAccess.class){
            truncate(transactional, io.hops.metadata.yarn.TablesDef.RunnableAppsTableDef.TABLE_NAME);
          } else if (e==CSLeafQueuesPendingAppsDataAccess.class){
            truncate(transactional, io.hops.metadata.yarn.TablesDef.CSLeafQueuesPendingAppsTableDef.TABLE_NAME);
          } else if (e==JustFinishedContainersDataAccess.class){
            truncate(transactional, io.hops.metadata.yarn.TablesDef.JustFinishedContainersTableDef.TABLE_NAME);
          } else if (e==YarnHistoryPriceDataAccess.class){
            truncate(transactional, io.hops.metadata.yarn.TablesDef.YarnHistoryPriceTableDef.TABLE_NAME);
          } else if (e==YarnRunningPriceDataAccess.class){
            truncate(transactional, io.hops.metadata.yarn.TablesDef.YarnRunningPriceTableDef.TABLE_NAME);
          }
        }
        MysqlServerConnector.truncateTable(transactional,
            "hdfs_path_memcached");
        return true;

      } catch (SQLException ex) {
        LOG.error(ex.getMessage(), ex);
      }
    } // end retry loop
    return false;
  }
  
  private void truncate(boolean transactional, String tableName)
      throws StorageException, SQLException {
    MysqlServerConnector.truncateTable(transactional, tableName);
  }

  @Override
  public void dropAndRecreateDB() throws StorageException {
    MysqlServerConnector.getInstance().dropAndRecreateDB();
  }
  
  @Override
  public void flush() throws StorageException {
    DBSession dbSession = sessions.get();
    if (dbSession == null) {
      dbSession = dbSessionProvider.getSession();
      sessions.set(dbSession);
    }
    dbSession.getSession().flush();
  }

  public String getClusterConnectString() {
    return clusterConnectString;
  }

  public String getDatabaseName() {
    return databaseName;
  }
  
}
