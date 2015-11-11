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

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.YarnVariablesDataAccess;
import io.hops.metadata.yarn.entity.YarnVariables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Table with one row that is used to obtain unique ids for tables. This
 * solution can be dropped once ClusterJ implements auto-increment.
 */
public class YarnVariablesClusterJ
    implements TablesDef.YarnVariablesTableDef, YarnVariablesDataAccess<YarnVariables> {

  private static final Log LOG = LogFactory.getLog(YarnVariablesClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface YarnVariablesDTO {

    @PrimaryKey
    @Column(name = ID)
    int getid();

    void setid(int id);

    @Column(name = VALUE)
    int getvalue();

    void setvalue(int value);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public YarnVariables findById(int id) throws StorageException {
    LOG.debug("HOP :: ClusterJ YarnVariables.findById - START:" + id);
    HopsSession session = connector.obtainSession();
    YarnVariablesDTO yarnDTO;
    if (id == Integer.MIN_VALUE) {
      id = idVal;
    }
    yarnDTO = session.find(YarnVariablesDTO.class, id);
    LOG.debug("HOP :: ClusterJ YarnVariables.findById - FINISH:" + id);
    YarnVariables result = null;
    if (yarnDTO != null) {
      result = new YarnVariables(yarnDTO.getid(), yarnDTO.getvalue());
    }
    session.release(yarnDTO);
    return result;
  }

  @Override
  public void add(YarnVariables toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    YarnVariablesDTO dto = createPersistable(toAdd, session);
    session.savePersistent(dto);
    session.release(dto);
  }

  private YarnVariablesDTO createPersistable(YarnVariables yarnVariables,
      HopsSession session) throws StorageException {
    YarnVariablesDTO yarnDTO = session.newInstance(YarnVariablesDTO.class);
    yarnDTO.setid(yarnVariables.getId());
    yarnDTO.setvalue(yarnVariables.getValue());
    return yarnDTO;
  }
}
