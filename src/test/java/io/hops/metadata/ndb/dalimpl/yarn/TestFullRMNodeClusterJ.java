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
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.*;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.*;
import io.hops.transaction.handler.LightWeightRequestHandler;
import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class TestFullRMNodeClusterJ extends NDBBaseTest {

  private final int DEFAULT_PENDIND_ID = 0;

  @Test
  public void testFindByNodeId() throws StorageException, IOException {
    //TODO: Add test for nextheartbeat
    //fill the database with a RMNode
    final RMNode hopRMNodeOrigin
            = new RMNode("70", "rmnode70", 9999, 9876, "127.0.0.1",
                    "hop.sics.se",
                    "life is good ", -10L, "relax", "hayarn", 10, 3, 0);
    final Node hopNodeOrigin
            = new Node("70", "rmnode70", "ici", 1000, "papa", 0);
    final List<NodeHBResponse> hopNHBROrigin = new ArrayList<NodeHBResponse>();
    hopNHBROrigin.add(new NodeHBResponse("70", new byte[]{new Integer(1).
      byteValue()}));
    final Resource hopResourceOrigin
            = new Resource("70", Resource.TOTAL_CAPABILITY, Resource.RMNODE, 1,
                    100, 0);

    final List<JustLaunchedContainers> hopJustLaunchedContainers
            = new ArrayList<JustLaunchedContainers>();
    hopJustLaunchedContainers
            .add(new JustLaunchedContainers("70", "container1"));
    hopJustLaunchedContainers
            .add(new JustLaunchedContainers("70", "container2"));
    final List<UpdatedContainerInfo> hopUpdatedContainers
            = new ArrayList<UpdatedContainerInfo>();
    hopUpdatedContainers.add(new UpdatedContainerInfo("70", "container3", 1,
            DEFAULT_PENDIND_ID));
    hopUpdatedContainers.add(new UpdatedContainerInfo("70", "container4", 2,
            DEFAULT_PENDIND_ID));

    final List<ContainerId> hopContainerIds = new ArrayList<ContainerId>();
    hopContainerIds.add(new ContainerId("70", "container5", 0));
    hopContainerIds.add(new ContainerId("70", "container6", 0));

    final List<FinishedApplications> hopFinishedApps
            = new ArrayList<FinishedApplications>();
    hopFinishedApps.add(new FinishedApplications("70", "app1", 0));
    hopFinishedApps.add(new FinishedApplications("70", "app2", 0));

    final List<ContainerStatus> hopContainersStatus
            = new ArrayList<ContainerStatus>();
    hopContainersStatus.add(
            new ContainerStatus("container1",
                    TablesDef.ContainerStatusTableDef.STATE_RUNNING,
                    "every thing is good", 0, "70", DEFAULT_PENDIND_ID,
                    ContainerStatus.Type.JUST_LAUNCHED));
    hopContainersStatus.add(
            new ContainerStatus("container2",
                    TablesDef.ContainerStatusTableDef.STATE_RUNNING,
                    "every thing is good", 0, "70", DEFAULT_PENDIND_ID,
                    ContainerStatus.Type.JUST_LAUNCHED));
    hopContainersStatus.add(
            new ContainerStatus("container3",
                    TablesDef.ContainerStatusTableDef.STATE_RUNNING,
                    "every thing is good", 0, "70", DEFAULT_PENDIND_ID,
                    ContainerStatus.Type.UCI));
    hopContainersStatus.add(
            new ContainerStatus("container4",
                    TablesDef.ContainerStatusTableDef.STATE_RUNNING,
                    "every thing is good", 0, "70", DEFAULT_PENDIND_ID,
                    ContainerStatus.Type.UCI));
    hopContainersStatus.add(new ContainerStatus("container5",
            TablesDef.ContainerStatusTableDef.STATE_COMPLETED,
            "every thing is good", 0,
            "70", DEFAULT_PENDIND_ID, ContainerStatus.Type.UCI));
    hopContainersStatus.add(new ContainerStatus("container6",
            TablesDef.ContainerStatusTableDef.STATE_COMPLETED, "finish", 1, "70",
            DEFAULT_PENDIND_ID, ContainerStatus.Type.UCI));

    LightWeightRequestHandler fillDB
            = new LightWeightRequestHandler(YARNOperationType.TEST) {
              @Override
              public Object performTask() throws StorageException {
                connector.beginTransaction();
                connector.writeLock();

                RMNodeDataAccess rmNodeDA = (RMNodeDataAccess) storageFactory.
                getDataAccess(RMNodeDataAccess.class);
                rmNodeDA.add(hopRMNodeOrigin);

                NodeDataAccess nodeDA = (NodeDataAccess) storageFactory.
                getDataAccess(NodeDataAccess.class);
                nodeDA.createNode(hopNodeOrigin);

                NodeHBResponseDataAccess nodeHBRDA
                = (NodeHBResponseDataAccess) storageFactory
                .getDataAccess(NodeHBResponseDataAccess.class);
                nodeHBRDA.addAll(hopNHBROrigin);

                ResourceDataAccess resourceDA
                = (ResourceDataAccess) storageFactory
                .getDataAccess(ResourceDataAccess.class);

                resourceDA.add(hopResourceOrigin);

                JustLaunchedContainersDataAccess justLaunchedContainerDA
                = (JustLaunchedContainersDataAccess) storageFactory.
                getDataAccess(JustLaunchedContainersDataAccess.class);
                justLaunchedContainerDA.addAll(hopJustLaunchedContainers);

                UpdatedContainerInfoDataAccess updatedContainerDA
                = (UpdatedContainerInfoDataAccess) storageFactory
                .getDataAccess(UpdatedContainerInfoDataAccess.class);
                updatedContainerDA.addAll(hopUpdatedContainers);

                ContainerIdToCleanDataAccess containersIdDA
                = (ContainerIdToCleanDataAccess) storageFactory
                .getDataAccess(ContainerIdToCleanDataAccess.class);
                containersIdDA.addAll(hopContainerIds);

                FinishedApplicationsDataAccess finishedAppDA
                = (FinishedApplicationsDataAccess) storageFactory
                .getDataAccess(FinishedApplicationsDataAccess.class);
                finishedAppDA.addAll(hopFinishedApps);

                ContainerStatusDataAccess containerStatusDA
                = (ContainerStatusDataAccess) storageFactory
                .getDataAccess(ContainerStatusDataAccess.class);
                containerStatusDA.addAll(hopContainersStatus);
                connector.commit();
                return null;
              }
            };
    fillDB.handle();

    //get the RMNode
    LightWeightRequestHandler getHopRMNode =
            new QueryFullRMNode(YARNOperationType.TEST, "70");

    RMNodeComps hopRMNodeFull = (RMNodeComps) getHopRMNode.handle();

    //check if the fetched RMNode is correct
    final RMNode rmNodeFinal = hopRMNodeFull.getHopRMNode();
    Assert.assertTrue(rmNodeFinal.getNodeId().equals(hopRMNodeOrigin.
            getNodeId()));
    Assert.assertTrue(rmNodeFinal.getCurrentState().equals(hopRMNodeOrigin.
            getCurrentState()));

    Node nodeFinal = hopRMNodeFull.getHopNode();
    Assert.assertTrue(nodeFinal.getLevel() == hopNodeOrigin.getLevel());

    NodeHBResponse nodeHBRFinal = hopRMNodeFull.getHopNodeHBResponse();
    Assert.assertTrue(nodeHBRFinal.getResponse()[0] == (hopNHBROrigin.get(0).
            getResponse()[0]));

    Resource resourceFinal = hopRMNodeFull.getHopResource();
    Assert.assertTrue(resourceFinal.getParent() == hopResourceOrigin.
            getParent());

    List<JustLaunchedContainers> hopJustLaunchedContainersFinal
            = hopRMNodeFull.getHopJustLaunchedContainers();
    for (JustLaunchedContainers justLaunched : hopJustLaunchedContainersFinal) {
      boolean flag = false;
      for (ContainerStatus containerStatus : hopRMNodeFull.
              getHopContainersStatus()) {
        if (containerStatus.getContainerid().equals(justLaunched.
                getContainerId())) {
          flag = true;
        }
      }

      Assert.assertTrue(flag);
      flag = false;
      for (JustLaunchedContainers justLaunchedOringin
              : hopJustLaunchedContainers) {
        if (justLaunchedOringin.getContainerId().equals(justLaunched.
                getContainerId())) {
          flag = true;
          break;
        }
      }
      Assert.assertTrue(flag);
    }

    List<UpdatedContainerInfo> hopUpdatedContainersFinal
            = hopRMNodeFull.getHopUpdatedContainerInfo();

    for (UpdatedContainerInfo updated : hopUpdatedContainersFinal) {
      boolean flag = false;
      for (ContainerStatus containerStatus : hopRMNodeFull.
              getHopContainersStatus()) {
        if (containerStatus.getContainerid().equals(updated.getContainerId())) {
          flag = true;
        }
      }
      Assert.assertTrue(flag);
      flag = false;
      for (UpdatedContainerInfo updatedOringin : hopUpdatedContainers) {
        if (updated.getContainerId().equals(updatedOringin.getContainerId())) {
          flag = true;
          break;
        }
      }
      Assert.assertTrue(flag);
    }

    List<ContainerId> hopContainerIdsFinal = hopRMNodeFull.
            getHopContainerIdsToClean();
    for (ContainerId updated : hopContainerIdsFinal) {
      boolean flag = false;
      for (ContainerId containerIdOringine : hopContainerIds) {
        if (updated.getContainerId().
                equals(containerIdOringine.getContainerId())) {
          flag = true;
          break;
        }
      }
      Assert.assertTrue(flag);
    }

    List<FinishedApplications> hopFinishedAppsFinal = hopRMNodeFull.
            getHopFinishedApplications();
    for (FinishedApplications finishedApp : hopFinishedAppsFinal) {
      boolean flag = false;
      for (FinishedApplications finishedAppOringine : hopFinishedApps) {
        if (finishedApp.getApplicationId().equals(finishedAppOringine.
                getApplicationId())) {
          flag = true;
          break;
        }
      }
      Assert.assertTrue(flag);
    }

    Collection<ContainerStatus> hopContainersStatusFinal = hopRMNodeFull.
            getHopContainersStatus();
    for (ContainerStatus containerStatus : hopContainersStatusFinal) {
      boolean flag = false;
      for (ContainerStatus containerStatusOringine : hopContainersStatus) {
        if (containerStatus.getContainerid().equals(containerStatusOringine.
                getContainerid()) && containerStatus.getExitstatus()
                == containerStatusOringine.getExitstatus()) {
          flag = true;
          break;
        }
      }
      Assert.assertTrue(flag);
    }

    // Removing RMNode should cascade the removal of all other entries as well
    LightWeightRequestHandler removeRMNode = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();

        RMNodeDataAccess rmNodeDAO = (RMNodeDataAccess)
                storageFactory.getDataAccess(RMNodeDataAccess.class);
        List<RMNode> toBeRemoved = new ArrayList<RMNode>();
        toBeRemoved.add(rmNodeFinal);
        rmNodeDAO.removeAll(toBeRemoved);
        connector.commit();

        return null;
      }
    };
    removeRMNode.handle();

    // Add just an RMNode so we can search for it
    LightWeightRequestHandler populate = new LightWeightRequestHandler(YARNOperationType.TEST) {
      @Override
      public Object performTask() throws IOException {
        connector.beginTransaction();
        connector.writeLock();
        RMNodeDataAccess rmNodeDAO = (RMNodeDataAccess)
                storageFactory.getDataAccess(RMNodeDataAccess.class);
        rmNodeDAO.add(hopRMNodeOrigin);
        connector.commit();

        return null;
      }
    };
    populate.handle();

    // Fetch again the deleted RMNode and its components
    hopRMNodeFull = (RMNodeComps) getHopRMNode.handle();
    // Node#id is set explicitly to RMNode.id, thus it's never null
    Assert.assertNull("HopNode name should not exist (null)", hopRMNodeFull.getHopNode().getName());
    Assert.assertNull("NodeHBResponse response should be null", hopRMNodeFull.getHopNodeHBResponse());
    Assert.assertNull("justlaunchedcontainers should be null", hopRMNodeFull.getHopJustLaunchedContainers());
    Assert.assertNull("updatedcontainerinfo should be null", hopRMNodeFull.getHopUpdatedContainerInfo());
    Assert.assertNull("containeridtoclean should be null", hopRMNodeFull.getHopContainerIdsToClean());
    Assert.assertNull("finishedapplications should be null", hopRMNodeFull.getHopFinishedApplications());
    Assert.assertTrue("containerstatus should not exist", hopRMNodeFull.getHopContainersStatus().isEmpty());
    // That's weird!
    Assert.assertEquals("nextheartbeat#pendingeventid should be zero", 0,
            hopRMNodeFull.getHopNextHeartbeat().getPendingEventId());
  }

  private class QueryFullRMNode extends LightWeightRequestHandler {
    private final String rmNodeId;

    public QueryFullRMNode(OperationType opType, String rmNodeId) {
      super(opType);
      this.rmNodeId = rmNodeId;
    }

    @Override
    public Object performTask() throws IOException {
      connector.beginTransaction();
      connector.writeLock();
      FullRMNodeDataAccess fullRMNodeDA
              = (FullRMNodeDataAccess) storageFactory.
              getDataAccess(FullRMNodeDataAccess.class);
      RMNodeComps hopRMNodeFull = (RMNodeComps) fullRMNodeDA.
              findByNodeId(rmNodeId);
      connector.commit();
      return hopRMNodeFull;
    }
  }
}
