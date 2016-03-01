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

import io.hops.StorageConnector;
import io.hops.metadata.ndb.NdbStorageFactory;
import io.hops.metadata.yarn.dal.capacity.CSPreemptedContainersDataAccess;
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.metadata.yarn.entity.capacity.CSPreemptedContainers;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.handler.RequestHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class TestCSPreemptedContainers {
    NdbStorageFactory storageFactory = new NdbStorageFactory();
    StorageConnector connector = storageFactory.getConnector();

    @Before
    public void setup() throws IOException {
        storageFactory.setConfiguration(getMetadataClusterConfiguration());
        RequestHandler.setStorageConnector(connector);
        LightWeightRequestHandler setRMDTMasterKeyHandler
                = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
                connector.formatStorage();

                return null;
            }
        };
        setRMDTMasterKeyHandler.handle();
    }

    @Test
    public void testCSPreemptedContainers() throws Exception {
        final CSPreemptedContainers cont0 =
                new CSPreemptedContainers("cont0", 10L);
        final CSPreemptedContainers cont1 =
                new CSPreemptedContainers("cont1", 11L);
        final CSPreemptedContainers cont2 =
                new CSPreemptedContainers("cont2", 12L);

        final List<CSPreemptedContainers> conts =
                new ArrayList<CSPreemptedContainers>();
        conts.add(cont0);
        conts.add(cont1);
        conts.add(cont2);

        LightWeightRequestHandler populate = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
                connector.beginTransaction();
                connector.writeLock();

                CSPreemptedContainersDataAccess prContDAO = (CSPreemptedContainersDataAccess)
                        storageFactory.getDataAccess(CSPreemptedContainersDataAccess.class);

                prContDAO.addAll(conts);
                connector.commit();

                return null;
            }
        };
        populate.handle();

        // Verify that they are all there
        LightWeightRequestHandler queryCont = new QueryPreemptedContainers(YARNOperationType.TEST);
        Map<String, CSPreemptedContainers> queryResult =
                (Map<String, CSPreemptedContainers>) queryCont.handle();
        Assert.assertEquals("There should be three entries", 3,
                queryResult.size());
        Assert.assertTrue("Container " + cont0.getRmContainerId() + " should be there",
                queryResult.containsKey(cont0.getRmContainerId()));
        Assert.assertEquals("Container's " + cont0.getRmContainerId() + " preemption" +
                "time should be 10", 10L, queryResult.get(cont0.getRmContainerId())
                    .getPreemptionTime());

        Assert.assertTrue("Container " + cont1.getRmContainerId() + " should be there",
                queryResult.containsKey(cont1.getRmContainerId()));
        Assert.assertEquals("Container's " + cont1.getRmContainerId() + " preemption" +
                "time should be 11", 11L, queryResult.get(cont1.getRmContainerId())
                .getPreemptionTime());

        Assert.assertTrue("Container " + cont2.getRmContainerId() + " should be there",
                queryResult.containsKey(cont2.getRmContainerId()));
        Assert.assertEquals("Container's " + cont2.getRmContainerId() + " preemption" +
                "time should be 12", 12L, queryResult.get(cont2.getRmContainerId())
                .getPreemptionTime());

        // Fetch only the first container
        LightWeightRequestHandler findBy = new LightWeightRequestHandler(YARNOperationType.TEST) {
            @Override
            public Object performTask() throws IOException {
                connector.beginTransaction();
                connector.readLock();

                CSPreemptedContainersDataAccess prContDAO = (CSPreemptedContainersDataAccess)
                        storageFactory.getDataAccess(CSPreemptedContainersDataAccess.class);

                CSPreemptedContainers result = (CSPreemptedContainers)
                        prContDAO.findById(cont0.getRmContainerId());
                connector.commit();

                return result;
            }
        };
        CSPreemptedContainers persCont0 = (CSPreemptedContainers)
                findBy.handle();
        Assert.assertNotNull("Result should not be null", persCont0);
        Assert.assertEquals("Container ID should be cont0", cont0.getRmContainerId(),
                persCont0.getRmContainerId());
        Assert.assertEquals("Container's cont0 preemption time should be 10", 10L,
                persCont0.getPreemptionTime());

        // Remove first container
        List<CSPreemptedContainers> tobeRemoved = new ArrayList<CSPreemptedContainers>();
        tobeRemoved.add(cont0);

        LightWeightRequestHandler contRemover = new RemovePreemptedContainers(
                YARNOperationType.TEST, tobeRemoved);
        contRemover.handle();

        persCont0 = (CSPreemptedContainers) findBy.handle();
        Assert.assertNull("Resultset should be null since cont0 has been removed",
                persCont0);

        // Remove the rest of the containers
        tobeRemoved.clear();
        tobeRemoved.add(cont1);
        tobeRemoved.add(cont2);
        contRemover = new RemovePreemptedContainers(YARNOperationType.TEST, tobeRemoved);
        contRemover.handle();

        queryResult = (Map<String, CSPreemptedContainers>) queryCont.handle();
        Assert.assertTrue("Result set should be empty by now", queryResult.isEmpty());
    }

    /**
     * Helper classes
     */
    private class RemovePreemptedContainers extends LightWeightRequestHandler {
        private final Collection<CSPreemptedContainers> toBeRemoved;

        public RemovePreemptedContainers(OperationType opType,
                                         Collection<CSPreemptedContainers> toBeRemoved) {
            super(opType);
            this.toBeRemoved = toBeRemoved;
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.writeLock();

            CSPreemptedContainersDataAccess prContDAO = (CSPreemptedContainersDataAccess)
                    storageFactory.getDataAccess(CSPreemptedContainersDataAccess.class);

            prContDAO.removeAll(toBeRemoved);
            connector.commit();

            return null;
        }
    }

    private class QueryPreemptedContainers extends LightWeightRequestHandler {

        public QueryPreemptedContainers(OperationType opType) {
            super(opType);
        }

        @Override
        public Object performTask() throws IOException {
            connector.beginTransaction();
            connector.readLock();

            CSPreemptedContainersDataAccess prContDAO = (CSPreemptedContainersDataAccess)
                    storageFactory.getDataAccess(CSPreemptedContainersDataAccess.class);

            Map<String, CSPreemptedContainers> result = prContDAO.getAll();
            connector.commit();

            return result;
        }
    }
    private Properties getMetadataClusterConfiguration()
            throws IOException {
        String configFile = "ndb-config.properties";
        Properties clusterConf = new Properties();
        InputStream inStream = StorageConnector.class.getClassLoader().
                getResourceAsStream(configFile);
        clusterConf.load(inStream);
        return clusterConf;
    }
}
