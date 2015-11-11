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
import io.hops.metadata.yarn.dal.rmstatestore.SequenceNumberDataAccess;
import io.hops.metadata.yarn.entity.rmstatestore.SequenceNumber;

public class SequenceNumberClusterJ implements TablesDef.SequenceNumberTableDef,
    SequenceNumberDataAccess<SequenceNumber> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface SequenceNumberDTO {

    @PrimaryKey
    @Column(name = ID)
    int getid();

    void setid(int id);

    @Column(name = SEQUENCE_NUMBER)
    int getsequencenumber();

    void setsequencenumber(int sequencenumber);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public SequenceNumber findById(int id) throws StorageException {
    HopsSession session = connector.obtainSession();

    SequenceNumberDTO sequenceNumberDTO = session.find(SequenceNumberDTO.class, id);


    SequenceNumber result = createHopSequenceNumber(sequenceNumberDTO);
    session.release(sequenceNumberDTO);
    return result;
  }

  @Override
  public void add(SequenceNumber toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    SequenceNumberDTO dto = createPersistable(toAdd, session);
    session.savePersistent(dto);
    session.release(dto);
  }

  private SequenceNumber createHopSequenceNumber(
      SequenceNumberDTO sequenceNumberDTO) {
    if (sequenceNumberDTO != null) {
      return new SequenceNumber(sequenceNumberDTO.getid(),
          sequenceNumberDTO.getsequencenumber());
    } else {
      return null;
    }
  }

  private SequenceNumberDTO createPersistable(SequenceNumber hop,
      HopsSession session) throws StorageException {
    SequenceNumberDTO sequenceNumberDTO =
        session.newInstance(SequenceNumberDTO.class);
    sequenceNumberDTO.setid(hop.getId());
    sequenceNumberDTO.setsequencenumber(hop.getSequencenumber());

    return sequenceNumberDTO;
  }
}
