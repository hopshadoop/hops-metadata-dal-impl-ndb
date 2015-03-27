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
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.rmstatestore.SecretMamagerKeysDataAccess;
import io.hops.metadata.yarn.entity.rmstatestore.SecretMamagerKey;
import io.hops.util.CompressionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;

public class SecretMamagerKeysClusterJ implements
    TablesDef.SecretMamagerKeysTableDef,
    SecretMamagerKeysDataAccess<SecretMamagerKey> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface SecretMamagerKeysDTO {

    @PrimaryKey
    @Column(name = KEYID)
    String getkeyid();

    void setkeyid(String keyid);

    @Column(name = KEY)
    byte[] getkey();

    void setkey(byte[] key);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<SecretMamagerKey> getAll() throws StorageException {

    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<SecretMamagerKeysDTO> dobj =
        qb.createQueryDefinition(SecretMamagerKeysDTO.class);
    HopsQuery<SecretMamagerKeysDTO> query = session.createQuery(dobj);
    List<SecretMamagerKeysDTO> results = query.getResultList();
    return createHopSecretMamagerKeyList(results);
    
  }

  @Override
  public void add(SecretMamagerKey toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(toAdd, session));
  }

  @Override
  public void remove(SecretMamagerKey toRemove) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistent(
        session.newInstance(SecretMamagerKeysDTO.class, toRemove.getKeyType()));
  }
  
  private SecretMamagerKeysDTO createPersistable(SecretMamagerKey hop,
      HopsSession session) throws StorageException {
    SecretMamagerKeysDTO keyDTO = session.
        newInstance(SecretMamagerKeysDTO.class);
    keyDTO.setkeyid(hop.getKeyType());
    try {
      keyDTO.setkey(CompressionUtils.compress(hop.getKey()));
    } catch (IOException e) {
      throw new StorageException(e);
    }

    return keyDTO;
  }

  private SecretMamagerKey createHopSecretMamagerKey(
      SecretMamagerKeysDTO keyDTO) throws StorageException {
    if (keyDTO != null) {
      try {
        return new SecretMamagerKey(keyDTO.getkeyid(),
            CompressionUtils.decompress(keyDTO.getkey()));
      } catch (IOException e) {
        throw new StorageException(e);
      } catch (DataFormatException e) {
        throw new StorageException(e);
      }
    } else {
      return null;
    }
  }
  
  private List<SecretMamagerKey> createHopSecretMamagerKeyList(
      List<SecretMamagerKeysDTO> list) throws StorageException {
    List<SecretMamagerKey> hopList = new ArrayList<SecretMamagerKey>();
    for (SecretMamagerKeysDTO dto : list) {
      hopList.add(createHopSecretMamagerKey(dto));
    }
    return hopList;

  }
}
