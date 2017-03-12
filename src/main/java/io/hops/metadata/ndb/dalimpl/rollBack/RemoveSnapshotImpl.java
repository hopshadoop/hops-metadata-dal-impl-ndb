package io.hops.metadata.ndb.dalimpl.rollBack;

import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.snapshots.SnapShotConstants;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.dalimpl.hdfs.BlockInfoClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.INodeAttributesClusterj;
import io.hops.metadata.ndb.dalimpl.hdfs.INodeClusterj;
import io.hops.metadata.ndb.wrapper.*;
import io.hops.metadata.rollBack.dal.RemoveSnapshotAccess;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static io.hops.metadata.ndb.dalimpl.rollBack.RollBackImpl.execMySqlQuery;
import io.hops.metadata.hdfs.TablesDef.*;


/**
 * Copyright (c) 01/08/16 DigitalRoute
 * All rights reserved.
 * Usage of this program and the accompanying materials is subject to license terms
 *
 * @author pushparaj.motamari
 */
public class RemoveSnapshotImpl implements RemoveSnapshotAccess {
    private static final Log LOG = LogFactory.getLog(RollBackImpl.class);
    private static int BUFFER_SIZE = 50000;
    private int rootId = 2;

    @Override
    public boolean processInodesPhase1() throws IOException {
        ExecutorService pool = Executors.newCachedThreadPool();
        int intervalStart;
        int minId;
        //Remove all back-up rows..i.e ..rows with negative id.
        try {
            minId = execMySqlQuery("select min(id) from hdfs_inodes where  id<0");

        } catch (StorageException ex) {
            // Logger.getLogger(RollBackImpl.class.getName()).log(Level.SEVERE, null, ex);
            throw new StorageException(ex);
        }

        intervalStart = 0;
        InodesPhase1Callable mr;
        while (intervalStart > minId) {
            mr = new InodesPhase1Callable(intervalStart, intervalStart - BUFFER_SIZE);
            intervalStart = intervalStart - BUFFER_SIZE;
            pool.submit(mr);
        }

        //Wait for completion of above task.
        RollBackImpl.shutDownPool(pool);

        //Confirm that the task has done.
        int count = 1;
        try {
            count = execMySqlQuery("select count(*) from hdfs_inodes where  id <0 ");

        } catch (StorageException ex) {
            //Logger.getLogger(RollBackImpl.class.getName()).log(Level.SEVERE, null, ex);
            throw new StorageException(ex);
        }

        return count == 0;
    }
    private static class InodesPhase1Callable implements Callable {

        int startId, endId;

        InodesPhase1Callable(int startId, int endId) {
            this.startId = startId;
            this.endId = endId;
        }

        @Override
        public Boolean call() throws StorageException {
            HopsSession session = ClusterjConnector.getInstance().obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();
            HopsQueryDomainType<INodeClusterj.InodeDTO> deleteInodes = qb.createQueryDefinition(INodeClusterj.InodeDTO.class);

            HopsPredicate pred5 = deleteInodes.get(INodeTableDef.ID).lessEqual(deleteInodes.param("startId"));
            HopsPredicate pred6 = deleteInodes.get(INodeTableDef.ID).greaterEqual(deleteInodes.param("endId"));
            deleteInodes.where(pred6.and(pred5));

            HopsQuery<INodeClusterj.InodeDTO> deleteQuery = session.createQuery(deleteInodes);

            deleteQuery.setParameter("startId", startId);
            deleteQuery.setParameter("endId", endId);

            deleteQuery.deletePersistentAll();

            session.flush();
            session.close();
            return true;
        }
    }

    @Override
    public boolean processInodesPhase2() throws IOException {
        ExecutorService pool = Executors.newCachedThreadPool();
        int intervalStart;
        int minId,maxId;
        // Change all new and modified rows to Original.
        try {
            maxId = execMySqlQuery("select max(id) from hdfs_inodes where  id>0 " +
                    " and (status ="+SnapShotConstants.Modified+" or status = "+SnapShotConstants.New+")");
            minId = execMySqlQuery("select min(id) from hdfs_inodes where  id>0" +
                    " and (status ="+SnapShotConstants.Modified+" or status = "+SnapShotConstants.New+")");

        } catch (StorageException ex) {
            throw new StorageException(ex);
        }

        intervalStart = minId;
        InodesPhase2Callable mr;
        while (intervalStart < maxId) {
            mr = new InodesPhase2Callable(intervalStart, intervalStart + BUFFER_SIZE);
            intervalStart = intervalStart + BUFFER_SIZE;
            pool.submit(mr);
        }

        //Wait for completion of above task.
        RollBackImpl.shutDownPool(pool);

        //Confirm that the task has done.
        int count = 1;
        try {
            count = execMySqlQuery("select count(*) from hdfs_inodes where  id >0 and (status =2 or status = 3) ");

        } catch (StorageException ex) {
            throw new StorageException(ex);
        }

        return count == 0;
    }
    private static class InodesPhase2Callable implements Callable {

        int startId, endId;

        InodesPhase2Callable(int startId, int endId) {
            this.startId = startId;
            this.endId = endId;
        }

        @Override
        public Boolean call() throws StorageException {
            HopsSession session = ClusterjConnector.getInstance().obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();
            HopsQueryDomainType<INodeClusterj.InodeDTO> updateInodes = qb.createQueryDefinition(INodeClusterj.InodeDTO.class);

            HopsPredicate pred3 = updateInodes.get(INodeTableDef.STATUS).equal(updateInodes.param("statusParam1"));
            HopsPredicate pred4 = updateInodes.get(INodeTableDef.STATUS).equal(updateInodes.param("statusParam2"));
            HopsPredicate pred5 = updateInodes.get(INodeTableDef.ID).greaterEqual(updateInodes.param("startId"));
            HopsPredicate pred6 = updateInodes.get(INodeTableDef.ID).lessEqual(updateInodes.param("endId"));
            updateInodes.where(pred6.and(pred5).and(pred3.or(pred4)));

            HopsQuery<INodeClusterj.InodeDTO> updateQuery = session.createQuery(updateInodes);

            updateQuery.setParameter("statusParameter1",SnapShotConstants.Modified);
            updateQuery.setParameter("statusParameter2",SnapShotConstants.New);
            updateQuery.setParameter("startId", startId);
            updateQuery.setParameter("endId", endId);

            List<INodeClusterj.InodeDTO> resultList = updateQuery.getResultList();

            for(INodeClusterj.InodeDTO inode : resultList){
                inode.setStatus(SnapShotConstants.Original);
            }

            session.updatePersistentAll(resultList);
            session.flush();
            session.close();
            return true;
        }
    }

    @Override
    public boolean processInodeAttributesPhase1() throws IOException {
        ExecutorService pool = Executors.newCachedThreadPool();
        int intervalStart;
        int minId;
        //Remove all back-up rows..i.e ..rows with negative id.
        try {
            minId = execMySqlQuery("select min("+INodeAttributesTableDef.ID +")  from hdfs_inode_attributes where  "+INodeAttributesTableDef.ID +"<0");

        } catch (StorageException ex) {
            // Logger.getLogger(RollBackImpl.class.getName()).log(Level.SEVERE, null, ex);
            throw new StorageException(ex);
        }

        intervalStart = 0;
        InodeAttributesPhase1Callable mr;
        while (intervalStart > minId) {
            mr = new InodeAttributesPhase1Callable(intervalStart, intervalStart - BUFFER_SIZE);
            intervalStart = intervalStart - BUFFER_SIZE;
            pool.submit(mr);
        }

        //Wait for completion of above task.
        RollBackImpl.shutDownPool(pool);

        //Confirm that the task has done.
        int count = 1;
        try {
            count = execMySqlQuery("select count(*) from hdfs_inode_attributes where  "+INodeAttributesTableDef.ID +" <0 ");

        } catch (StorageException ex) {
            //Logger.getLogger(RollBackImpl.class.getName()).log(Level.SEVERE, null, ex);
            throw new StorageException(ex);
        }

        return count == 0;
    }
    private static class InodeAttributesPhase1Callable implements Callable {

        int startId, endId;

        InodeAttributesPhase1Callable(int startId, int endId) {
            this.startId = startId;
            this.endId = endId;
        }

        @Override
        public Boolean call() throws StorageException {
            HopsSession session = ClusterjConnector.getInstance().obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();
            HopsQueryDomainType<INodeAttributesClusterj.INodeAttributesDTO> deleteInodeAttributes = qb.createQueryDefinition(INodeAttributesClusterj.INodeAttributesDTO.class);

            HopsPredicate pred5 = deleteInodeAttributes.get(INodeAttributesTableDef.ID).lessEqual(deleteInodeAttributes.param("startId"));
            HopsPredicate pred6 = deleteInodeAttributes.get(INodeAttributesTableDef.ID).greaterEqual(deleteInodeAttributes.param("endId"));
            deleteInodeAttributes.where(pred6.and(pred5));

            HopsQuery<INodeAttributesClusterj.INodeAttributesDTO> deleteQuery = session.createQuery(deleteInodeAttributes);

            deleteQuery.setParameter("startId", startId);
            deleteQuery.setParameter("endId", endId);

            deleteQuery.deletePersistentAll();

            session.flush();
            session.close();
            return true;
        }
    }

    @Override
    public boolean processInodeAttributesPhase2() throws IOException {
        ExecutorService pool = Executors.newCachedThreadPool();
        int intervalStart;
        int minId,maxId;
        // Change all new and modified rows to Original.
        try {
            maxId = execMySqlQuery("select max("+INodeAttributesTableDef.ID +") from hdfs_inode_attributes where  "+INodeAttributesTableDef.ID +">0" +
                    " and (status ="+SnapShotConstants.Modified+" or status = "+SnapShotConstants.New+")");
            minId = execMySqlQuery("select min("+INodeAttributesTableDef.ID +") from hdfs_inode_attributes where  "+INodeAttributesTableDef.ID +">0 " +
                    " and (status ="+SnapShotConstants.Modified+" or status = "+SnapShotConstants.New+")");
        } catch (StorageException ex) {
            throw new StorageException(ex);
        }

        intervalStart = minId;
        InodeAttributesPhase2Callable mr;
        while (intervalStart < maxId) {
            mr = new InodeAttributesPhase2Callable(intervalStart, intervalStart + BUFFER_SIZE);
            intervalStart = intervalStart + BUFFER_SIZE;
            pool.submit(mr);
        }

        //Wait for completion of above task.
        RollBackImpl.shutDownPool(pool);

        //Confirm that the task has done.
        int count = 1;
        try {
            count = execMySqlQuery("select count(*) from hdfs_inode_attributes where  "+INodeAttributesTableDef.ID +" >0 and (status =2 or status = 3) ");

        } catch (StorageException ex) {
            throw new StorageException(ex);
        }

        return count == 0;
    }

    private static class InodeAttributesPhase2Callable implements Callable {

        int startId, endId;

        InodeAttributesPhase2Callable(int startId, int endId) {
            this.startId = startId;
            this.endId = endId;
        }

        @Override
        public Boolean call() throws StorageException {
            HopsSession session = ClusterjConnector.getInstance().obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();
            HopsQueryDomainType<INodeAttributesClusterj.INodeAttributesDTO> updateInodeAttributes = qb.createQueryDefinition(INodeAttributesClusterj.INodeAttributesDTO.class);

            HopsPredicate pred3 = updateInodeAttributes.get(INodeAttributesTableDef.STATUS).equal(updateInodeAttributes.param("statusParam1"));
            HopsPredicate pred4 = updateInodeAttributes.get(INodeAttributesTableDef.STATUS).equal(updateInodeAttributes.param("statusParam2"));
            HopsPredicate pred5 = updateInodeAttributes.get(INodeAttributesTableDef.ID).greaterEqual(updateInodeAttributes.param("startId"));
            HopsPredicate pred6 = updateInodeAttributes.get(INodeAttributesTableDef.ID).lessEqual(updateInodeAttributes.param("endId"));
            updateInodeAttributes.where(pred6.and(pred5).and(pred3.or(pred4)));

            HopsQuery<INodeAttributesClusterj.INodeAttributesDTO> updateQuery = session.createQuery(updateInodeAttributes);

            updateQuery.setParameter("statusParameter1",SnapShotConstants.Modified);
            updateQuery.setParameter("statusParameter2",SnapShotConstants.New);
            updateQuery.setParameter("startId", startId);
            updateQuery.setParameter("endId", endId);

            List<INodeAttributesClusterj.INodeAttributesDTO> resultList = updateQuery.getResultList();

            for(INodeAttributesClusterj.INodeAttributesDTO inodeAttribute : resultList){
                inodeAttribute.setStatus(SnapShotConstants.Original);
            }

            session.updatePersistentAll(resultList);
            session.flush();
            session.close();
            return true;
        }
    }

    public boolean processBlocksPhase1() throws IOException {
        ExecutorService pool = Executors.newCachedThreadPool();
        int intervalStart;
        int minId;
        //Remove all back-up rows..i.e ..rows with negative id.
        try {
            minId = execMySqlQuery("select min("+BlockInfoTableDef.BLOCK_ID +") from hdfs_block_infos where  "+BlockInfoTableDef.BLOCK_ID +"<0");
        } catch (StorageException ex) {
            // Logger.getLogger(RollBackImpl.class.getName()).log(Level.SEVERE, null, ex);
            throw new StorageException(ex);
        }

        intervalStart = 0;
        processBlocksPhase1Callable mr;
        while (intervalStart > minId) {
            mr = new processBlocksPhase1Callable(intervalStart, intervalStart - BUFFER_SIZE);
            intervalStart = intervalStart - BUFFER_SIZE;
            pool.submit(mr);
        }

        //Wait for completion of above task.
        RollBackImpl.shutDownPool(pool);

        //Confirm that the task has done.
        int count = 1;
        try {
            count = execMySqlQuery("select count(*) from hdfs_block_infos where "+BlockInfoTableDef.BLOCK_ID +" <0 ");

        } catch (StorageException ex) {
            //Logger.getLogger(RollBackImpl.class.getName()).log(Level.SEVERE, null, ex);
            throw new StorageException(ex);
        }

        return count == 0;
    }
    private static class processBlocksPhase1Callable implements Callable {

        int startId, endId;

        processBlocksPhase1Callable(int startId, int endId) {
            this.startId = startId;
            this.endId = endId;
        }

        @Override
        public Boolean call() throws StorageException {
            HopsSession session = ClusterjConnector.getInstance().obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();
            HopsQueryDomainType<BlockInfoClusterj.BlockInfoDTO> deleteBlocks = qb.createQueryDefinition(BlockInfoClusterj.BlockInfoDTO.class);

            HopsPredicate pred5 = deleteBlocks.get(BlockInfoTableDef.BLOCK_ID).lessEqual(deleteBlocks.param("startId"));
            HopsPredicate pred6 = deleteBlocks.get(BlockInfoTableDef.BLOCK_ID).greaterEqual(deleteBlocks.param("endId"));
            deleteBlocks.where(pred6.and(pred5));

            HopsQuery<BlockInfoClusterj.BlockInfoDTO> deleteQuery = session.createQuery(deleteBlocks);

            deleteQuery.setParameter("startId", startId);
            deleteQuery.setParameter("endId", endId);

            deleteQuery.deletePersistentAll();

            session.flush();
            session.close();
            return true;
        }
    }

    @Override
    public boolean processBlocksPhase2() throws IOException {
        ExecutorService pool = Executors.newCachedThreadPool();
        int intervalStart;
        int minId,maxId;
        // Change all new and modified rows to Original.
        try {
            maxId = execMySqlQuery("select max("+BlockInfoTableDef.BLOCK_ID +") from hdfs_block_infos where  "+BlockInfoTableDef.BLOCK_ID +">0" +
                    " and (status ="+SnapShotConstants.Modified+" or status = "+SnapShotConstants.New+")");
            minId = execMySqlQuery("select min("+BlockInfoTableDef.BLOCK_ID +") from hdfs_block_infos where  "+BlockInfoTableDef.BLOCK_ID +">0 " +
                    " and (status ="+SnapShotConstants.Modified+" or status = "+SnapShotConstants.New+")");
        } catch (StorageException ex) {
            throw new StorageException(ex);
        }

        intervalStart = minId;
        processBlocksPhase2Callable mr;
        while (intervalStart < maxId) {
            mr = new processBlocksPhase2Callable(intervalStart, intervalStart + BUFFER_SIZE);
            intervalStart = intervalStart + BUFFER_SIZE;
            pool.submit(mr);
        }

        //Wait for completion of above task.
        RollBackImpl.shutDownPool(pool);

        //Confirm that the task has done.
        int count = 1;
        try {
            count = execMySqlQuery("select count(*) from hdfs_block_infos where "+BlockInfoTableDef.BLOCK_ID +" >0 " +
                    "and ( "+BlockInfoTableDef.STATUS +" =2 or "+BlockInfoTableDef.STATUS +" = 3) ");

        } catch (StorageException ex) {
            throw new StorageException(ex);
        }

        return count == 0;
    }
    private static class processBlocksPhase2Callable implements Callable {

        int startId, endId;

        processBlocksPhase2Callable(int startId, int endId) {
            this.startId = startId;
            this.endId = endId;
        }

        @Override
        public Boolean call() throws StorageException {
            HopsSession session = ClusterjConnector.getInstance().obtainSession();
            HopsQueryBuilder qb = session.getQueryBuilder();
            HopsQueryDomainType<BlockInfoClusterj.BlockInfoDTO> updateBlockInfos = qb.createQueryDefinition(BlockInfoClusterj.BlockInfoDTO.class);

            HopsPredicate pred3 = updateBlockInfos.get(BlockInfoTableDef.STATUS).equal(updateBlockInfos.param("statusParam1"));
            HopsPredicate pred4 = updateBlockInfos.get(BlockInfoTableDef.STATUS).equal(updateBlockInfos.param("statusParam2"));
            HopsPredicate pred5 = updateBlockInfos.get(BlockInfoTableDef.BLOCK_ID).greaterEqual(updateBlockInfos.param("startId"));
            HopsPredicate pred6 = updateBlockInfos.get(BlockInfoTableDef.BLOCK_ID).lessEqual(updateBlockInfos.param("endId"));
            updateBlockInfos.where(pred6.and(pred5).and(pred3.or(pred4)));

            HopsQuery<BlockInfoClusterj.BlockInfoDTO> updateQuery = session.createQuery(updateBlockInfos);

            updateQuery.setParameter("statusParameter1",SnapShotConstants.Modified);
            updateQuery.setParameter("statusParameter2",SnapShotConstants.New);
            updateQuery.setParameter("startId", startId);
            updateQuery.setParameter("endId", endId);

            List<BlockInfoClusterj.BlockInfoDTO> resultList = updateQuery.getResultList();

            for(BlockInfoClusterj.BlockInfoDTO inodeAttribute : resultList){
                inodeAttribute.setStatus(SnapShotConstants.Original);
            }

            session.updatePersistentAll(resultList);
            session.flush();
            session.close();
            return true;
        }
    }
    public  boolean waitForSubTreeOperations() throws IOException {
        String query = "select count(*) from hdfs_on_going_sub_tree_ops";
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 60 * 1000) {
            int onGoingSubTreeOpsCount = execMySqlQuery(query);
            if (onGoingSubTreeOpsCount == 0) {
                return true;
            }
        }
        return false;
    }

    public boolean waitForQuotaUpdates() throws IOException {
        String query = "select count(*) from hdfs_quota_update";
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 60 * 1000) {
            int quotaUpdatesYetToBeApplied = execMySqlQuery(query);
            if (quotaUpdatesYetToBeApplied == 0) {
                return true;
            }
        }
        return false;
    }

}
