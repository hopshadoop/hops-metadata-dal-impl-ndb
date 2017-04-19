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
  public enum Zone {
    PRIMARY,
    SECONDARY,
  }
  private static final Log LOG = LogFactory.getLog(MultiZoneStorageConnector.class);

  private final Zone zone;
  private final boolean multiZone;
  // we keep this in case we need to initialize again
  private final Properties conf;

  /**
   * local and primary will contain connections to the local database and primary database.
   * note that if multiZone is false or if the primary cluster is also the local cluster
   * then only local is set and primary == null
   */
  private ClusterjConnector local;
  private ClusterjConnector primary;

  /**
   * setConfiguration configures and registers the underlying storage connectors.
   * At most there will be two storage connectors registered, the LOCAL and PRIMARY clusters.
   * @param conf configuration parameters for the connectors
   * @throws StorageException
   */
  public MultiZoneClusterjConnector(final Properties conf) throws StorageException {
    this.conf = conf;
    LOG.debug("initializing clusterj connectors");
    LOG.debug("loading configuration for local cluster");
    this.local = new ClusterjConnector("io.hops.metadata.local", conf);

    multiZone = Boolean.parseBoolean(conf.getProperty("io.hops.metadata.multizone", "false"));

    // if we are not running in multi-zone mode, we are done here
    if(!multiZone) {
      LOG.debug("multi-zone mode: disabled");
      this.zone = Zone.PRIMARY;
      return;
    }

    String z = conf.getProperty("io.hops.metadata.zone");
    try {
      this.zone = Zone.valueOf(z);
    } catch (IllegalArgumentException exc) {
      throw new StorageException("invalid zone " + z, exc);
    }
    LOG.debug("multi-zone mode: enabled, zone: " + this.zone.toString());

    // we already loaded the connection to the local metadata cluster and we are in the primary zone.
    if(this.zone == Zone.PRIMARY) {
      // we are done
      return;
    }

    LOG.debug("loading configuration for primary cluster");
    this.primary = new ClusterjConnector("io.hops.metadata.primary", conf);
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
