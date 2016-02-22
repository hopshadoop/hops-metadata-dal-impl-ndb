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
import io.hops.metadata.yarn.dal.util.YARNOperationType;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.handler.RequestHandler;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class NDBBaseTest {
    NdbStorageFactory storageFactory = new NdbStorageFactory();
    StorageConnector connector = storageFactory.getConnector();

    @Before
    public void setup() throws IOException {
        storageFactory.setConfiguration(getMetadataClusterConfiguration());
        RequestHandler.setStorageConnector(connector);
        LightWeightRequestHandler setRMDTMasterKeyHandler =
                new StorageFormatter(YARNOperationType.TEST);
        setRMDTMasterKeyHandler.handle();
    }

    @After
    public void tearDown() throws IOException {
        LightWeightRequestHandler clean =
                new StorageFormatter(YARNOperationType.TEST);
        clean.handle();
    }

    protected Properties getMetadataClusterConfiguration()
            throws IOException {
        String configFile = "ndb-config.properties";
        Properties clusterConf = new Properties();
        InputStream inStream = StorageConnector.class.getClassLoader().
                getResourceAsStream(configFile);
        clusterConf.load(inStream);
        return clusterConf;
    }

    private class StorageFormatter extends LightWeightRequestHandler {

        public StorageFormatter(OperationType opType) {
            super(opType);
        }

        @Override
        public Object performTask() throws IOException {
            connector.formatStorage();
            return null;
        }
    }
}
