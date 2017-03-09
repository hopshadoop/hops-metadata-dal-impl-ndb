package io.hops.metadata.ndb;

import io.hops.MultiZoneStorageConnector;
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.transaction.TransactionCluster;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Properties;

public class MultiZoneClusterjConnector implements MultiZoneStorageConnector {
  private enum Zone {
    PRIMARY,
    SECONDARY,
  }

  private boolean initialized = false;

  private static final String prefixLocal = "io.hops.metadata.local";
  private static final String prefixPrimary = "io.hops.metadata.primary";

  private static final Log LOG = LogFactory.getLog(MultiZoneStorageConnector.class);

  private Zone zone = Zone.PRIMARY;
  private boolean multiZone;

  /**
   * local and primary will contain connections to the local database and primary database.
   * note that if multiZone is false or if the primary cluster is also the local cluster
   * then only local is set and primary == null
   */
  private ClusterjConnector local, primary;

  // singletons stuff
  private static MultiZoneClusterjConnector instance;

  // private constructor for singleton
  private MultiZoneClusterjConnector() {}

  public static MultiZoneClusterjConnector getInstance() {
    if(instance == null) {
      instance = new MultiZoneClusterjConnector();
    }
    return instance;
  }

  /**
   * setConfiguration reads configuration and sets up clusterj connection(s) to the database(s).
   * @param conf configuration to load from
   * @throws StorageException
   */
  @Override
  public void setConfiguration(Properties conf) throws StorageException {
    if(initialized) {
      throw new StorageException("instance already initialized");
    }

    setConfigurationInternal(conf);
    // if it did not throw an exception it is initialized
    initialized = true;
  }

  private void setConfigurationInternal(Properties conf) throws StorageException {
    LOG.info("initializing clusterj connectors");
    LOG.info("loading configuration for local cluster");
    try {
      local = new ClusterjConnector(prefixLocal);
      local.setConfiguration(conf);
    } catch (StorageException exc) {
      this.cleanup();
      throw exc;
    }

    multiZone = Boolean.parseBoolean(
        conf.getProperty("io.hops.metadata.multizone", "false")
    );

    // if we are not running in multi-zone mode, we are done here
    if(!multiZone) {
      LOG.info("multi-zone mode: disabled");
      return;
    }

    String z = conf.getProperty("io.hops.metadata.zone");
    try {
      this.zone = Zone.valueOf(z);
    } catch (IllegalArgumentException exc) {
      throw new StorageException("invalid zone " + z, exc);
    }
    LOG.info("multi-zone mode: enabled, zone: " + this.zone.toString());

    // we already loaded the connection to the local metadata cluster and we are in the primary zone.
    if(zone == Zone.PRIMARY) {
      // we are done
      return;
    }

    LOG.info("loading configuration for primary cluster");
    try {
      primary = new ClusterjConnector(prefixPrimary);
      primary.setConfiguration(conf);
    } catch (StorageException exc) {
      this.cleanup();
      throw exc;
    }
  }

  private void cleanup() throws StorageException {
    if(this.local != null) {
      this.local.stopStorage();
    }

    if(this.primary != null) {
      this.primary.stopStorage();
    }
  }

  public MysqlServerConnector mysqlConnectorFor(TransactionCluster cluster) {
    return ((ClusterjConnector) this.connectorFor(cluster)).getMysqlConnector();
  }

  @Override
  public StorageConnector connectorFor(TransactionCluster cluster) {
    // if multizone is disabled just return local
    if(!multiZone) {
      return this.local;
    }
    // if we are on the primary zone primary==local so return local
    if(zone == Zone.PRIMARY) {
      return this.local;
    }


    // return the correct zoneConnector
    switch(cluster) {
      case PRIMARY:
        return this.primary;
      default:
        return this.local;
    }
  }
}
