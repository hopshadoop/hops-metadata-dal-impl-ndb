package io.hops.metadata.ndb.dalimpl;

import io.hops.metadata.common.EntityDataAccess;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;

public class ClusterjDataAccess implements EntityDataAccess {
  private ClusterjConnector connector;

  public ClusterjDataAccess(ClusterjConnector connector) {
    this.connector = connector;
  }

  protected ClusterjConnector getConnector() {
    return this.connector;
  }

  protected MysqlServerConnector getMysqlConnector() {
    return this.connector.getMysqlConnector();
  }
}

