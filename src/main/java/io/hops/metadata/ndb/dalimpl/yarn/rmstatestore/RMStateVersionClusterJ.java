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
package io.hops.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.rmstatestore.RMStateVersionDataAccess;
import io.hops.metadata.yarn.entity.rmstatestore.RMStateVersion;

public class RMStateVersionClusterJ implements TablesDef.RMStateVersionTableDef,
    RMStateVersionDataAccess<RMStateVersion> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface VersionDTO {

    @PrimaryKey
    @Column(name = ID)
    int getid();

    void setid(int id);

    @Column(name = VERSION)
    byte[] getversion();

    void setversion(byte[] version);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public RMStateVersion findById(int id) throws StorageException {
    HopsSession session = connector.obtainSession();

    RMStateVersionClusterJ.VersionDTO versionDTO = null;
    if (session != null) {
      versionDTO = session.find(RMStateVersionClusterJ.VersionDTO.class, id);
    }

    return createHopVersion(versionDTO);
  }

  @Override
  public void add(RMStateVersion toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(toAdd, session));
  }

  private RMStateVersion createHopVersion(VersionDTO versionDTO) {
    if (versionDTO != null) {
      return new RMStateVersion(versionDTO.getid(), versionDTO.getversion());
    } else {
      return null;
    }
  }

  private VersionDTO createPersistable(RMStateVersion hop, HopsSession session)
      throws StorageException {
    RMStateVersionClusterJ.VersionDTO versionDTO =
        session.newInstance(RMStateVersionClusterJ.VersionDTO.class);
    versionDTO.setid(hop.getId());
    versionDTO.setversion(hop.getVersion());

    return versionDTO;
  }
}
