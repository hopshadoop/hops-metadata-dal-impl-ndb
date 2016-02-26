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

import io.hops.exception.StorageException;
import io.hops.metadata.yarn.dal.FinishedApplicationsDataAccess;
import io.hops.metadata.yarn.dal.NextHeartbeatDataAccess;
import io.hops.metadata.yarn.dal.RMNodeDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.FinishedApplications;
import io.hops.metadata.yarn.entity.NextHeartbeat;
import io.hops.metadata.yarn.entity.RMNode;
import io.hops.transaction.handler.LightWeightRequestHandler;
import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class TestDeletionCascade extends NDBBaseTest {
    @Test
    public void testRemoveHBForRMNode() throws StorageException, IOException {
        final RMNode hopsRMNode0 =
                new RMNode("host0:1234", "hostname0", 1234, 8080, "127.0.0.1", "hop.sics.se",
                        "healthy", -10L, "running", "blah", 10, 3, 0);
        final NextHeartbeat nextHB0 =
                new NextHeartbeat(hopsRMNode0.getNodeId(), true, 22);

        final RMNode hopsRMNode1 =
                new RMNode("host1:1234", "hostname1", 1234, 8080, "127.0.0.1", "hop.sics.se",
                        "healthy", -10L, "running", "blah", 10, 3, 0);
        final NextHeartbeat nextHB1 =
                new NextHeartbeat(hopsRMNode1.getNodeId(), true, 22);

        final RMNode hopsRMNode2 =
                new RMNode("host2:1234", "hostname2", 1234, 8080, "127.0.0.1", "hop.sics.se",
                        "healthy", -10L, "running", "blah", 10, 3, 0);
        final NextHeartbeat nextHB2 =
                new NextHeartbeat(hopsRMNode2.getNodeId(), true, 22);

        final List<RMNode> rmNodes = new ArrayList<RMNode>();
        rmNodes.add(hopsRMNode0);
        rmNodes.add(hopsRMNode1);
        rmNodes.add(hopsRMNode2);

        final List<NextHeartbeat> HBs = new ArrayList<NextHeartbeat>();
        HBs.add(nextHB0);
        HBs.add(nextHB1);
        HBs.add(nextHB2);

        // Persist them in DB
        LightWeightRequestHandler populate = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws StorageException {
                connector.beginTransaction();
                connector.writeLock();

                RMNodeDataAccess rmNodeDAO = (RMNodeDataAccess) storageFactory.
                        getDataAccess(RMNodeDataAccess.class);

                NextHeartbeatDataAccess nextHBDAO = (NextHeartbeatDataAccess) storageFactory.
                        getDataAccess(NextHeartbeatDataAccess.class);

                nextHBDAO.updateAll(HBs);

                rmNodeDAO.addAll(rmNodes);

                connector.commit();
                return null;
            }
        };
        populate.handle();

        // Verify RMNodes are there
        LightWeightRequestHandler queryRMNodes = new QueryRMNodes(YARNOperationType.TEST);
        Map<String, RMNode> rmNodeResult = (Map<String, RMNode>) queryRMNodes.handle();

        org.junit.Assert.assertEquals("There should be three RMNodes persisted", 3, rmNodeResult.size());

        // Verify nextHBs are there
        LightWeightRequestHandler queryHB = new QueryHB(YARNOperationType.TEST);

        Map<String, Boolean> hbResult = (Map<String, Boolean>) queryHB.handle();

        org.junit.Assert.assertEquals("There should be three next heartbeats persisted", 3, hbResult.size());

        // Remove one RMNode should trigger corresponding next HB to be removed
        List<RMNode> toBeRemoved = new ArrayList<RMNode>();
        toBeRemoved.add(rmNodes.get(0));
        LightWeightRequestHandler removerRMNodes = new RemoveRMNodes(YARNOperationType.TEST, toBeRemoved);
        removerRMNodes.handle();

        rmNodeResult = (Map<String, RMNode>) queryRMNodes.handle();
        org.junit.Assert.assertEquals("There should be two RMNodes persisted", 2, rmNodeResult.size());
        hbResult = (Map<String, Boolean>) queryHB.handle();
        org.junit.Assert.assertEquals("There should be two next heartbeats persisted", 2, hbResult.size());

        // Remove the rest of the RMNodes and corresponding HBs
        toBeRemoved.clear();
        toBeRemoved.add(rmNodes.get(1));
        toBeRemoved.add(rmNodes.get(2));
        removerRMNodes = new RemoveRMNodes(YARNOperationType.TEST, toBeRemoved);
        removerRMNodes.handle();

        rmNodeResult = (Map<String, RMNode>) queryRMNodes.handle();
        org.junit.Assert.assertEquals("There should be none RMNodes persisted", 0, rmNodeResult.size());
        hbResult = (Map<String, Boolean>) queryHB.handle();
        org.junit.Assert.assertEquals("There should be none next heartbeats persisted", 0, hbResult.size());

        // Add RMNode but not HB
        populate = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws StorageException {
                connector.beginTransaction();
                connector.writeLock();

                RMNodeDataAccess rmNodeDAO = (RMNodeDataAccess) storageFactory.
                        getDataAccess(RMNodeDataAccess.class);

                rmNodeDAO.addAll(rmNodes);

                connector.commit();
                return null;
            }
        };
        populate.handle();

        rmNodeResult = (Map<String, RMNode>) queryRMNodes.handle();
        org.junit.Assert.assertEquals("There should be three RMNodes persisted", 3, rmNodeResult.size());

        hbResult = (Map<String, Boolean>) queryHB.handle();
        org.junit.Assert.assertEquals("There should be none next heartbeats persisted", 0, hbResult.size());

        removerRMNodes = new RemoveRMNodes(YARNOperationType.TEST, rmNodes);
        removerRMNodes.handle();

        rmNodeResult = (Map<String, RMNode>) queryRMNodes.handle();
        org.junit.Assert.assertEquals("There should be none RMNodes persisted", 0, rmNodeResult.size());
    }

    @Test
    public void testRemoveFinishedApplicationsForRMNode()
            throws StorageException, IOException {
        final RMNode hopsRMNode0 =
                new RMNode("host0:1234", "hostname0", 1234, 8080, "127.0.0.1", "hop.sics.se",
                        "healthy", -10L, "running", "blah", 10, 3, 0);
        final List<FinishedApplications> finishedApps0 = new ArrayList<FinishedApplications>();
        finishedApps0.add(new FinishedApplications("host0:1234", "app0_0", 1));
        finishedApps0.add(new FinishedApplications("host0:1234", "app0_1", 1));

        final RMNode hopsRMNode1 =
                new RMNode("host1:1234", "hostname1", 1234, 8080, "127.0.0.1", "hop.sics.se",
                        "healthy", -10L, "running", "blah", 10, 3, 0);
        final List<FinishedApplications> finishedApps1 = new ArrayList<FinishedApplications>();
        finishedApps1.add(new FinishedApplications("host1:1234", "app1_0", 1));
        finishedApps1.add(new FinishedApplications("host1:1234", "app1_1", 1));

        final List<RMNode> rmNodes = new ArrayList<RMNode>();
        rmNodes.add(hopsRMNode0);
        rmNodes.add(hopsRMNode1);

        // Persist them in DB
        LightWeightRequestHandler populate = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
                connector.beginTransaction();
                connector.writeLock();

                RMNodeDataAccess rmNodeDAO = (RMNodeDataAccess) storageFactory
                        .getDataAccess(RMNodeDataAccess.class);

                FinishedApplicationsDataAccess finishedAppsDAO = (FinishedApplicationsDataAccess)
                        storageFactory.getDataAccess(FinishedApplicationsDataAccess.class);

                finishedAppsDAO.addAll(finishedApps0);
                finishedAppsDAO.addAll(finishedApps1);

                rmNodeDAO.addAll(rmNodes);

                connector.commit();
                return null;
            }
        };
        populate.handle();

        // Verify FinishedApplications are there
        LightWeightRequestHandler queryFinishedApps = new QueryFinishedApps(YARNOperationType.TEST);
        Map<String, List<FinishedApplications>> finishedAppsResult =
                (Map<String, List<FinishedApplications>>) queryFinishedApps.handle();

        Assert.assertEquals("There should be 2 FinishedApplications groups, grouped by RMNode", 2,
                finishedAppsResult.size());
        Assert.assertEquals("RMode 0 should have two finished apps", 2,
                finishedAppsResult.get(hopsRMNode0.getNodeId()).size());
        Assert.assertEquals("RMode 1 should have two finished apps", 2,
                finishedAppsResult.get(hopsRMNode1.getNodeId()).size());

        // Remove first RMNode
        List<RMNode> toBeRemoved = new ArrayList<RMNode>();
        toBeRemoved.add(rmNodes.get(0));
        LightWeightRequestHandler rmNodeRemover = new RemoveRMNodes(YARNOperationType.TEST, toBeRemoved);
        rmNodeRemover.handle();

        // Verify FinishedApplications are removed as well
        finishedAppsResult = (Map<String, List<FinishedApplications>>)
                queryFinishedApps.handle();
        Assert.assertEquals("Only one set of FinishedApplications should exist", 1,
                finishedAppsResult.size());
        Assert.assertFalse("RMNode0 should not have FinishedApplications",
                finishedAppsResult.containsKey(hopsRMNode0.getNodeId()));
        Assert.assertEquals("RMNode1 FinishedApplications should be two", 2,
                finishedAppsResult.get(hopsRMNode1.getNodeId()).size());

        // Remove second RMNode
        toBeRemoved.clear();
        toBeRemoved.add(rmNodes.get(1));
        // TODO: Change this with private class RemoveRMNodes
        rmNodeRemover = new RemoveRMNodes(YARNOperationType.TEST, toBeRemoved);
        rmNodeRemover.handle();

        // No FinishedApplications should be there
        finishedAppsResult = (Map<String, List<FinishedApplications>>)
                queryFinishedApps.handle();
        Assert.assertEquals("No FinishedApplications should exist at that point", 0,
                finishedAppsResult.size());
    }

    private class RemoveRMNodes extends LightWeightRequestHandler {
        private List<RMNode> rmNodes;

        public RemoveRMNodes(OperationType opType, List<RMNode> rmNodes) {
            super(opType);
            this.rmNodes = rmNodes;
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();

            RMNodeDataAccess rmNodeDAO = (RMNodeDataAccess) storageFactory.
                    getDataAccess(RMNodeDataAccess.class);
            Collection<RMNode> toBeDeleted = new HashSet<RMNode>(rmNodes);

            rmNodeDAO.removeAll(toBeDeleted);
            connector.commit();

            return null;
        }
    }

    private class QueryHB extends LightWeightRequestHandler {

        public QueryHB(OperationType opType) {
            super(opType);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();

            NextHeartbeatDataAccess nextHBDAO = (NextHeartbeatDataAccess) storageFactory
                    .getDataAccess(NextHeartbeatDataAccess.class);
            Map<String, Boolean> result = nextHBDAO.getAll();
            connector.commit();

            return result;
        }
    }

    private class QueryRMNodes extends LightWeightRequestHandler {

        public QueryRMNodes(OperationType opType) {
            super(opType);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();
            RMNodeDataAccess rmNodeDAO = (RMNodeDataAccess) storageFactory.
                    getDataAccess(RMNodeDataAccess.class);
            Map<String, RMNode> result = rmNodeDAO.getAll();

            connector.commit();
            return result;
        }
    }

    private class QueryFinishedApps extends LightWeightRequestHandler {

        public QueryFinishedApps(OperationType opType) {
            super(opType);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();
            FinishedApplicationsDataAccess finishedAppsDAO = (FinishedApplicationsDataAccess)
                    storageFactory.getDataAccess(FinishedApplicationsDataAccess.class);
            Map<String, List<FinishedApplications>> result = finishedAppsDAO.getAll();
            connector.commit();

            return result;
        }
    }
}
