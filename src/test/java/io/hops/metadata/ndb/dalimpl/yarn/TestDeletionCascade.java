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
import io.hops.metadata.yarn.dal.*;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.*;
import io.hops.transaction.handler.LightWeightRequestHandler;
import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class TestDeletionCascade extends NDBBaseTest {
    final static RMNode hopsRMNode0 =
            new RMNode("host0:1234", "hostname0", 1234, 8080, "127.0.0.1", "hop.sics.se",
                    "healthy", -10L, "running", "blah", 10, 3, 0);
    final static RMNode hopsRMNode1 =
            new RMNode("host1:1234", "hostname1", 1234, 8080, "127.0.0.1", "hop.sics.se",
                    "healthy", -10L, "running", "blah", 10, 3, 0);
    final static RMNode hopsRMNode2 =
            new RMNode("host2:1234", "hostname2", 1234, 8080, "127.0.0.1", "hop.sics.se",
                    "healthy", -10L, "running", "blah", 10, 3, 0);
    final static List<RMNode> rmNodes = new ArrayList<RMNode>();
    static {
        rmNodes.add(hopsRMNode0);
        rmNodes.add(hopsRMNode1);
        rmNodes.add(hopsRMNode2);
    }

    @Test
    public void testRemoveHBForRMNode() throws StorageException, IOException {
        final NextHeartbeat nextHB0 =
                new NextHeartbeat(hopsRMNode0.getNodeId(), true, 22);

        final NextHeartbeat nextHB1 =
                new NextHeartbeat(hopsRMNode1.getNodeId(), true, 22);

        final NextHeartbeat nextHB2 =
                new NextHeartbeat(hopsRMNode2.getNodeId(), true, 22);

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

        final List<FinishedApplications> finishedApps0 = new ArrayList<FinishedApplications>();
        finishedApps0.add(new FinishedApplications("host0:1234", "app0_0", 1));
        finishedApps0.add(new FinishedApplications("host0:1234", "app0_1", 1));

        final List<FinishedApplications> finishedApps1 = new ArrayList<FinishedApplications>();
        finishedApps1.add(new FinishedApplications("host1:1234", "app1_0", 1));
        finishedApps1.add(new FinishedApplications("host1:1234", "app1_1", 1));

        final List<FinishedApplications> finishedApps2 = new ArrayList<FinishedApplications>();
        finishedApps2.add(new FinishedApplications("host2:1234", "app1_0", 1));
        finishedApps2.add(new FinishedApplications("host2:1234", "app1_1", 1));

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
                finishedAppsDAO.addAll(finishedApps2);

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

        Assert.assertEquals("There should be 3 FinishedApplications groups, grouped by RMNode", 3,
                finishedAppsResult.size());
        Assert.assertEquals("RMode 0 should have two finished apps", 2,
                finishedAppsResult.get(hopsRMNode0.getNodeId()).size());
        Assert.assertEquals("RMode 1 should have two finished apps", 2,
                finishedAppsResult.get(hopsRMNode1.getNodeId()).size());
        Assert.assertEquals("RMode 2 should have two finished apps", 2,
                finishedAppsResult.get(hopsRMNode2.getNodeId()).size());

        // Remove first RMNode
        List<RMNode> toBeRemoved = new ArrayList<RMNode>();
        toBeRemoved.add(rmNodes.get(0));
        LightWeightRequestHandler rmNodeRemover = new RemoveRMNodes(YARNOperationType.TEST, toBeRemoved);
        rmNodeRemover.handle();

        // Verify FinishedApplications are removed as well
        finishedAppsResult = (Map<String, List<FinishedApplications>>)
                queryFinishedApps.handle();
        Assert.assertEquals("Only two sets of FinishedApplications should exist", 2,
                finishedAppsResult.size());
        Assert.assertFalse("RMNode0 should not have FinishedApplications",
                finishedAppsResult.containsKey(hopsRMNode0.getNodeId()));
        Assert.assertEquals("RMNode1 FinishedApplications should be two", 2,
                finishedAppsResult.get(hopsRMNode1.getNodeId()).size());
        Assert.assertEquals("RMNode2 FinishedApplications should be two", 2,
                finishedAppsResult.get(hopsRMNode2.getNodeId()).size());

        // Remove the rest of RMNodes
        toBeRemoved.clear();
        toBeRemoved.add(rmNodes.get(1));
        toBeRemoved.add(rmNodes.get(2));
        rmNodeRemover = new RemoveRMNodes(YARNOperationType.TEST, toBeRemoved);
        rmNodeRemover.handle();

        // No FinishedApplications should be there
        finishedAppsResult = (Map<String, List<FinishedApplications>>)
                queryFinishedApps.handle();
        Assert.assertEquals("No FinishedApplications should exist at that point", 0,
                finishedAppsResult.size());
    }

    @Test
    public void testRemoveNodeForRMNode() throws StorageException, IOException {
        final Node hopsNode0 =
                new Node("host0:1234", "host0", "rack0", 1, "parent", 1);

        final Node hopsNode1 =
                new Node("host1:1234", "host1", "rack0", 1, "parent", 1);

        final Node hopsNode2 =
                new Node("host2:1234", "host2", "rack1", 1, "parent", 1);

        final List<Node> nodes = new ArrayList<Node>();
        nodes.add(hopsNode0);
        nodes.add(hopsNode1);
        nodes.add(hopsNode2);

        // Persist them in DB
        LightWeightRequestHandler populate = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
                connector.beginTransaction();
                connector.writeLock();

                RMNodeDataAccess rmNodeDAO = (RMNodeDataAccess) storageFactory
                        .getDataAccess(RMNodeDataAccess.class);

                NodeDataAccess nodeDAO = (NodeDataAccess) storageFactory
                        .getDataAccess(NodeDataAccess.class);

                nodeDAO.addAll(nodes);

                rmNodeDAO.addAll(rmNodes);

                connector.commit();
                return null;
            }
        };
        populate.handle();

        // Verify Nodes are there
        LightWeightRequestHandler queryNodes = new QueryNodes(YARNOperationType.TEST);
        Map<String, Node> queryNodesResult = (Map<String, Node>) queryNodes.handle();
        Assert.assertEquals("We should have 3 Nodes", 3, queryNodesResult.size());

        // Remove first RMNode
        List<RMNode> toBeRemoved = new ArrayList<RMNode>();
        toBeRemoved.add(rmNodes.get(0));
        LightWeightRequestHandler rmNodeRemover = new RemoveRMNodes(YARNOperationType.TEST, toBeRemoved);
        rmNodeRemover.handle();

        // Verify Node entry is removed as well
        queryNodesResult = (Map<String, Node>) queryNodes.handle();
        Assert.assertEquals("Now we should have 2 Nodes", 2, queryNodesResult.size());
        Assert.assertFalse("host0 should not have any Node", queryNodesResult.containsKey(
                rmNodes.get(0).getNodeId()));

        // Remove rest of RMNodes
        toBeRemoved.clear();
        toBeRemoved.add(rmNodes.get(1));
        toBeRemoved.add(rmNodes.get(2));
        rmNodeRemover = new RemoveRMNodes(YARNOperationType.TEST, toBeRemoved);
        rmNodeRemover.handle();

        // Verify no Node entry is there
        queryNodesResult = (Map<String, Node>) queryNodes.handle();
        Assert.assertTrue("There should be no Node entry", queryNodesResult.isEmpty());
    }

    @Test
    public void testRemoveUpdatedContainersInfoForRMNode() throws Exception {
        final List<UpdatedContainerInfo> containerInfos0 =
                new ArrayList<UpdatedContainerInfo>();
        containerInfos0.add(new UpdatedContainerInfo("host0:1234", "cont0", 1, 1));
        containerInfos0.add(new UpdatedContainerInfo("host0:1234", "cont1", 1, 1));
        containerInfos0.add(new UpdatedContainerInfo("host0:1234", "cont2", 1, 1));

        final List<UpdatedContainerInfo> containerInfos1 =
                new ArrayList<UpdatedContainerInfo>();
        containerInfos0.add(new UpdatedContainerInfo("host1:1234", "cont0", 1, 1));
        containerInfos0.add(new UpdatedContainerInfo("host1:1234", "cont1", 1, 1));
        containerInfos0.add(new UpdatedContainerInfo("host1:1234", "cont2", 1, 1));

        final List<UpdatedContainerInfo> containerInfos2 =
                new ArrayList<UpdatedContainerInfo>();
        containerInfos0.add(new UpdatedContainerInfo("host2:1234", "cont0", 1, 1));
        containerInfos0.add(new UpdatedContainerInfo("host2:1234", "cont1", 1, 1));
        containerInfos0.add(new UpdatedContainerInfo("host2:1234", "cont2", 1, 1));

        // Persist them in DB
        LightWeightRequestHandler populate = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
                connector.beginTransaction();
                connector.writeLock();

                RMNodeDataAccess rmNodeDAO = (RMNodeDataAccess) storageFactory
                        .getDataAccess(RMNodeDataAccess.class);

                UpdatedContainerInfoDataAccess updatedContDAO = (UpdatedContainerInfoDataAccess)
                        storageFactory.getDataAccess(UpdatedContainerInfoDataAccess.class);

                updatedContDAO.addAll(containerInfos0);
                updatedContDAO.addAll(containerInfos1);
                updatedContDAO.addAll(containerInfos2);

                rmNodeDAO.addAll(rmNodes);

                connector.commit();
                return null;
            }
        };
        populate.handle();

        // Verify UpdatedContainers are there
        QueryUpdatedContainers queryUpdatedCont = new QueryUpdatedContainers(YARNOperationType.TEST);
        Map<String, Map<Integer, List<UpdatedContainerInfo>>> updatedContResult =
                (Map<String, Map<Integer, List<UpdatedContainerInfo>>>) queryUpdatedCont.handle();

        /*for (Map.Entry<String, Map<Integer, List<UpdatedContainerInfo>>> cont : updatedContResult.entrySet()) {
            System.out.println("RMNode ID: " + cont.getKey());
            for (Map.Entry<Integer, List<UpdatedContainerInfo>> val : cont.getValue().entrySet()) {
                System.out.println("Container info ID: " + val.getKey());
                for (UpdatedContainerInfo contInfo : val.getValue()) {
                    System.out.println("Container info: " + contInfo);
                }
            }
        }*/

        Assert.assertEquals("Three groups of RMNodes", 3, updatedContResult.size());
        Assert.assertEquals("host0:1234 should have one entry for container info id", 1,
                updatedContResult.get(rmNodes.get(0).getNodeId()).size());
        Assert.assertEquals("host1:1234 should have one entry for container info id", 1,
                updatedContResult.get(rmNodes.get(1).getNodeId()).size());
        Assert.assertEquals("host2:1234 should have one entry for container info id", 1,
                updatedContResult.get(rmNodes.get(2).getNodeId()).size());
        Assert.assertEquals("host0:1234 should have three updates for containerinfoid: 1", 3,
                updatedContResult.get(rmNodes.get(0).getNodeId()).get(1).size());
        Assert.assertEquals("host1:1234 should have three updates for containerinfoid: 1", 3,
                updatedContResult.get(rmNodes.get(1).getNodeId()).get(1).size());
        Assert.assertEquals("host2:1234 should have three updates for containerinfoid: 1", 3,
                updatedContResult.get(rmNodes.get(2).getNodeId()).get(1).size());

        // Remove first RMNode
        final List<RMNode> toBeRemoved = new ArrayList<RMNode>();
        toBeRemoved.add(rmNodes.get(0));
        LightWeightRequestHandler rmNodeRemover = new RemoveRMNodes(YARNOperationType.TEST, toBeRemoved);
        rmNodeRemover.handle();

        // Verify no updatedcontainers exist for host0:1234
        updatedContResult = (Map<String, Map<Integer, List<UpdatedContainerInfo>>>)
                queryUpdatedCont.handle();
        Assert.assertEquals("There should be two groups of updatedcontainersinfo", 2,
                updatedContResult.size());
        Assert.assertFalse("There should be no entry for host0:1234",
                updatedContResult.containsKey(hopsRMNode0.getNodeId()));

        // Remove the rest of the RMNodes
        toBeRemoved.clear();
        toBeRemoved.add(rmNodes.get(1));
        toBeRemoved.add(rmNodes.get(2));
        rmNodeRemover = new RemoveRMNodes(YARNOperationType.TEST, toBeRemoved);
        rmNodeRemover.handle();

        // Verify that no updatedcontainersinfo is there
        updatedContResult = (Map<String, Map<Integer, List<UpdatedContainerInfo>>>)
                queryUpdatedCont.handle();
        Assert.assertTrue("There should be no updatedcontainersinfo entries",
                updatedContResult.isEmpty());
    }

    @Test
    public void testRemoveContainerStatusForRMNode() throws Exception {
        final List<ContainerStatus> containerStatus0 =
                new ArrayList<ContainerStatus>();
        containerStatus0.add(new ContainerStatus("cont0_0", "running", "healthy", 0, "host0:1234", 1));
        containerStatus0.add(new ContainerStatus("cont0_1", "running", "healthy", 0, "host0:1234", 1));

        final List<ContainerStatus> containerStatus1 =
                new ArrayList<ContainerStatus>();
        containerStatus1.add(new ContainerStatus("cont1_0", "running", "healthy", 0, "host1:1234", 1));
        containerStatus1.add(new ContainerStatus("cont1_1", "running", "healthy", 0, "host1:1234", 1));

        final List<ContainerStatus> containerStatus2 =
                new ArrayList<ContainerStatus>();
        containerStatus2.add(new ContainerStatus("cont2_0", "running", "healthy", 0, "host2:1234", 1));
        containerStatus2.add(new ContainerStatus("cont2_1", "running", "healthy", 0, "host2:1234", 1));

        // Persist them in DB
        LightWeightRequestHandler populate = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
                connector.beginTransaction();
                connector.writeLock();

                RMNodeDataAccess rmNodeDAO = (RMNodeDataAccess) storageFactory
                        .getDataAccess(RMNodeDataAccess.class);

                ContainerStatusDataAccess contStatusDAO = (ContainerStatusDataAccess)
                        storageFactory.getDataAccess(ContainerStatusDataAccess.class);

                contStatusDAO.addAll(containerStatus0);
                contStatusDAO.addAll(containerStatus1);
                contStatusDAO.addAll(containerStatus2);

                rmNodeDAO.addAll(rmNodes);

                connector.commit();
                return null;
            }
        };
        populate.handle();

        // Verify container statuses are there
        LightWeightRequestHandler queryContStatus = new QueryContainerStatus(YARNOperationType.TEST);
        Map<String, ContainerStatus> contStatusResult = (Map<String, ContainerStatus>)
                queryContStatus.handle();

        Assert.assertEquals("There are six different containers", 6,
                contStatusResult.size());

        // Remove first RMNode
        List<RMNode> toBeDeleted = new ArrayList<RMNode>();
        toBeDeleted.add(hopsRMNode0);
        LightWeightRequestHandler rmNodeRemover = new RemoveRMNodes(YARNOperationType.TEST, toBeDeleted);
        rmNodeRemover.handle();

        contStatusResult = (Map<String, ContainerStatus>) queryContStatus.handle();

        Assert.assertEquals("There should be four containers now", 4,
                contStatusResult.size());
        Assert.assertFalse("cont0_0 should not be there", contStatusResult.containsKey(containerStatus0.get(0)));
        Assert.assertFalse("cont0_1 should not be there", contStatusResult.containsKey(containerStatus0.get(1)));

        // Remove the rest of the RMNodes
        toBeDeleted.clear();
        toBeDeleted.add(hopsRMNode1);
        toBeDeleted.add(hopsRMNode2);
        rmNodeRemover = new RemoveRMNodes(YARNOperationType.TEST, toBeDeleted);
        rmNodeRemover.handle();

        contStatusResult = (Map<String, ContainerStatus>) queryContStatus.handle();

        Assert.assertTrue("There should be none container statuses by now", contStatusResult.isEmpty());
    }

    @Test
    public void testRemoveJustLaunchedContainersForRMNode()
        throws Exception {
        final List<JustLaunchedContainers> justLaunCont0 =
                new ArrayList<JustLaunchedContainers>();
        justLaunCont0.add(new JustLaunchedContainers(hopsRMNode0.getNodeId(), "cont0_0"));
        justLaunCont0.add(new JustLaunchedContainers(hopsRMNode0.getNodeId(), "cont0_1"));

        final List<JustLaunchedContainers> justLaunCont1 =
                new ArrayList<JustLaunchedContainers>();
        justLaunCont1.add(new JustLaunchedContainers(hopsRMNode1.getNodeId(), "cont1_0"));
        justLaunCont1.add(new JustLaunchedContainers(hopsRMNode1.getNodeId(), "cont1_1"));

        final List<JustLaunchedContainers> justLaunCont2 =
                new ArrayList<JustLaunchedContainers>();
        justLaunCont2.add(new JustLaunchedContainers(hopsRMNode2.getNodeId(), "cont2_0"));
        justLaunCont2.add(new JustLaunchedContainers(hopsRMNode2.getNodeId(), "cont2_1"));

        // Persist them in DB
        LightWeightRequestHandler populate = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
                connector.beginTransaction();
                connector.writeLock();

                RMNodeDataAccess rmNodeDAO = (RMNodeDataAccess) storageFactory
                        .getDataAccess(RMNodeDataAccess.class);
                JustLaunchedContainersDataAccess jLaunched = (JustLaunchedContainersDataAccess)
                        storageFactory.getDataAccess(JustLaunchedContainersDataAccess.class);

                jLaunched.addAll(justLaunCont0);
                jLaunched.addAll(justLaunCont1);
                jLaunched.addAll(justLaunCont2);

                rmNodeDAO.addAll(rmNodes);

                connector.commit();
                return null;
            }
        };
        populate.handle();

        // Verify all just launched containers are there
        LightWeightRequestHandler queryJLaunched = new QueryJustLaunchedContainers(YARNOperationType.TEST);
        Map<String, List<JustLaunchedContainers>> jLaunchedResult = (Map<String, List<JustLaunchedContainers>>)
                queryJLaunched.handle();

        Assert.assertEquals("There should be three RMNode IDs", 3, jLaunchedResult.size());

        // Remove the first node
        List<RMNode> toBeDeleted = new ArrayList<RMNode>();
        toBeDeleted.add(hopsRMNode0);
        LightWeightRequestHandler rmNodeRemover = new RemoveRMNodes(YARNOperationType.TEST, toBeDeleted);
        rmNodeRemover.handle();

        jLaunchedResult = (Map<String, List<JustLaunchedContainers>>)
                queryJLaunched.handle();

        Assert.assertEquals("There should be two RMNode IDs now", 2, jLaunchedResult.size());
        Assert.assertFalse("host0:1234 should not exist", jLaunchedResult.containsKey(hopsRMNode0.getNodeId()));
        Assert.assertEquals("host1:1234 should have two launched containers", 2,
                jLaunchedResult.get(hopsRMNode1.getNodeId()).size());
        Assert.assertEquals("host2:1234 should have two launched containers", 2,
                jLaunchedResult.get(hopsRMNode2.getNodeId()).size());

        // Remove the rest of the RMNodes
        toBeDeleted.clear();
        toBeDeleted.add(hopsRMNode1);
        toBeDeleted.add(hopsRMNode2);
        rmNodeRemover = new RemoveRMNodes(YARNOperationType.TEST, toBeDeleted);
        rmNodeRemover.handle();

        jLaunchedResult = (Map<String, List<JustLaunchedContainers>>)
                queryJLaunched.handle();
        Assert.assertNull("By now there should be no just launched containers",
                jLaunchedResult);
    }

    @Test
    public void testRemoveNodeHBResponseForRMNode() throws Exception {
        final List<NodeHBResponse> nodeHBResponses =
                new ArrayList<NodeHBResponse>();
        nodeHBResponses.add(new NodeHBResponse(hopsRMNode0.getNodeId(), "some_response".getBytes()));
        nodeHBResponses.add(new NodeHBResponse(hopsRMNode1.getNodeId(), "some_response".getBytes()));
        nodeHBResponses.add(new NodeHBResponse(hopsRMNode2.getNodeId(), "some_response".getBytes()));

        // Persist them in DB
        LightWeightRequestHandler populate = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
                connector.beginTransaction();
                connector.writeLock();

                RMNodeDataAccess rmNodeDAO = (RMNodeDataAccess) storageFactory
                        .getDataAccess(RMNodeDataAccess.class);
                NodeHBResponseDataAccess nodeHBResposeDAO = (NodeHBResponseDataAccess)
                        storageFactory.getDataAccess(NodeHBResponseDataAccess.class);

                nodeHBResposeDAO.addAll(nodeHBResponses);
                rmNodeDAO.addAll(rmNodes);

                connector.commit();
                return null;
            }
        };
        populate.handle();

        // Verify NodeHBResponses are there
        LightWeightRequestHandler queryNodeHBResponse =
                new QueryNodeHBRespose(YARNOperationType.TEST);
        Map<String, NodeHBResponse> nodeHBResult = (Map<String, NodeHBResponse>)
                queryNodeHBResponse.handle();

        Assert.assertEquals("There should be three node HB responses", 3,
                nodeHBResult.size());
        Assert.assertTrue("hops0:1234 should be there", nodeHBResult.containsKey(
                hopsRMNode0.getNodeId()));
        Assert.assertTrue("hops1:1234 should be there", nodeHBResult.containsKey(
                hopsRMNode0.getNodeId()));
        Assert.assertTrue("hops2:1234 should be there", nodeHBResult.containsKey(
                hopsRMNode0.getNodeId()));

        // Remove first node
        List<RMNode> toBeRemoved = new ArrayList<RMNode>();
        toBeRemoved.add(hopsRMNode0);
        LightWeightRequestHandler rmNodeRemover = new RemoveRMNodes(YARNOperationType.TEST, toBeRemoved);
        rmNodeRemover.handle();

        nodeHBResult = (Map<String, NodeHBResponse>) queryNodeHBResponse.handle();
        Assert.assertEquals("There should be two node HB responses", 2,
                nodeHBResult.size());
        Assert.assertFalse("host0:1234 should not have an entry", nodeHBResult.containsKey(
                hopsRMNode0.getNodeId()));

        // Remove the rest of the RM nodes
        toBeRemoved.clear();
        toBeRemoved.add(hopsRMNode1);
        toBeRemoved.add(hopsRMNode2);
        rmNodeRemover = new RemoveRMNodes(YARNOperationType.TEST, toBeRemoved);
        rmNodeRemover.handle();

        nodeHBResult = (Map<String, NodeHBResponse>) queryNodeHBResponse.handle();
        Assert.assertTrue("Response should not have any RM nodes", nodeHBResult.isEmpty());
    }

    /**
     * Helper classes
     */
    private class QueryNodeHBRespose extends LightWeightRequestHandler {

        public QueryNodeHBRespose(OperationType opType) {
            super(opType);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();

            NodeHBResponseDataAccess nodeHBResponseDAO = (NodeHBResponseDataAccess)
                    storageFactory.getDataAccess(NodeHBResponseDataAccess.class);

            Map<String, NodeHBResponse> result = nodeHBResponseDAO.getAll();
            connector.commit();

            return result;
        }
    }

    private class QueryJustLaunchedContainers extends LightWeightRequestHandler {

        public QueryJustLaunchedContainers(OperationType opType) {
            super(opType);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();

            JustLaunchedContainersDataAccess jLaunchedDAO = (JustLaunchedContainersDataAccess)
                    storageFactory.getDataAccess(JustLaunchedContainersDataAccess.class);

            Map<String, List<JustLaunchedContainers>> result = jLaunchedDAO.getAll();
            connector.commit();

            return result;
        }
    }

    private class QueryContainerStatus extends LightWeightRequestHandler {

        public QueryContainerStatus(OperationType opType) {
            super(opType);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();

            ContainerStatusDataAccess contStatusDAO = (ContainerStatusDataAccess)
                    storageFactory.getDataAccess(ContainerStatusDataAccess.class);
            Map<String, ContainerStatus> result = contStatusDAO.getAll();
            connector.commit();

            return result;
        }
    }

    private class QueryUpdatedContainers extends LightWeightRequestHandler {

        public QueryUpdatedContainers(OperationType opType) {
            super(opType);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();

            UpdatedContainerInfoDataAccess updatedContDAO = (UpdatedContainerInfoDataAccess)
                    storageFactory.getDataAccess(UpdatedContainerInfoDataAccess.class);

            Map<String, Map<Integer, List<UpdatedContainerInfo>>> result =
                    updatedContDAO.getAll();

            connector.commit();
            return result;
        }
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

    private class QueryNodes extends  LightWeightRequestHandler {

        public QueryNodes(OperationType opType) {
            super(opType);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();
            NodeDataAccess nodeDAO = (NodeDataAccess) storageFactory.getDataAccess(NodeDataAccess.class);
            Map<String, Node> result = nodeDAO.getAll();
            connector.commit();

            return result;
        }
    }
}
