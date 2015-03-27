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
import io.hops.metadata.yarn.dal.rmstatestore.ApplicationAttemptStateDataAccess;
import io.hops.metadata.yarn.entity.rmstatestore.ApplicationAttemptState;
import io.hops.util.CompressionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

public class ApplicationAttemptStateClusterJ
    implements TablesDef.ApplicationAttemptStateTableDef,
    ApplicationAttemptStateDataAccess<ApplicationAttemptState> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ApplicationAttemptStateDTO {

    @PrimaryKey
    @Column(name = APPLICATIONID)
    String getapplicationid();

    void setapplicationid(String applicationid);

    @Column(name = APPLICATIONATTEMPTID)
    String getapplicationattemptid();

    void setapplicationattemptid(String applicationattemptid);

    @Column(name = APPLICATIONATTEMPTSTATE)
    byte[] getapplicationattemptstate();

    void setapplicationattemptstate(byte[] applicationattemptstate);

    @Column(name = HOST)
    String getapplicationattempthost();

    void setapplicationattempthost(String host);

    @Column(name = RPCPORT)
    int getapplicationattemptrpcport();

    void setapplicationattemptrpcport(int port);

    @Column(name = TOKENS)
    byte[] getapplicationattempttokens();

    void setapplicationattempttokens(byte[] tokens);

    @Column(name = TRAKINGURL)
    String getapplicationattempttrakingurl();

    void setapplicationattempttrakingurl(String trakingUrl);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public ApplicationAttemptState findEntry(String applicationid,
      String applicationattemptid) throws StorageException {
    HopsSession session = connector.obtainSession();
    Object[] objarr = new Object[2];
    objarr[0] = applicationid;
    objarr[1] = applicationattemptid;
    ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO entry;
    if (session != null) {
      entry = session.find(
          ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO.class,
          objarr);
      if (entry != null) {
        return createHopApplicationAttemptState(entry);
      }
    }
    return null;
  }

  @Override
  public Map<String, List<ApplicationAttemptState>> getAll()
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<ApplicationAttemptStateDTO> dobj = qb.
        createQueryDefinition(ApplicationAttemptStateDTO.class);
    HopsQuery<ApplicationAttemptStateDTO> query = session.createQuery(dobj);
    List<ApplicationAttemptStateDTO> results = query.getResultList();

    return createMap(results);
  }

  @Override
  public void createApplicationAttemptStateEntry(ApplicationAttemptState entry)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(entry, session));
  }

  @Override
  public void addAll(Collection<ApplicationAttemptState> toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ApplicationAttemptStateDTO> toPersist =
        new ArrayList<ApplicationAttemptStateDTO>();
    for (ApplicationAttemptState req : toAdd) {
      toPersist.add(createPersistable(req, session));
    }
    session.savePersistentAll(toPersist);
  }

  @Override
  public void removeAll(Collection<ApplicationAttemptState> removed)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ApplicationAttemptStateDTO> toRemove =
        new ArrayList<ApplicationAttemptStateDTO>();
    for (ApplicationAttemptState hop : removed) {
      Object[] objarr = new Object[2];
      objarr[0] = hop.getApplicationId();
      objarr[1] = hop.getApplicationattemptid();
      toRemove.add(session.newInstance(
          ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO.class,
          objarr));
    }
    session.deletePersistentAll(toRemove);
  }

  private ApplicationAttemptState createHopApplicationAttemptState(
      ApplicationAttemptStateDTO entry) throws StorageException {
    ByteBuffer buffer;
    if (entry.getapplicationattempttokens() != null) {
      buffer = ByteBuffer.wrap(entry.getapplicationattempttokens());
    } else {
      buffer = null;
    }
    try {
      return new ApplicationAttemptState(entry.getapplicationid(),
          entry.getapplicationattemptid(),
          CompressionUtils.decompress(entry.getapplicationattemptstate()),
          entry.getapplicationattempthost(),
          entry.getapplicationattemptrpcport(), buffer,
          entry.getapplicationattempttrakingurl());
    } catch (IOException e) {
      throw new StorageException(e);
    } catch (DataFormatException e) {
      throw new StorageException(e);
    }
  }

  private ApplicationAttemptStateDTO createPersistable(
      ApplicationAttemptState hop, HopsSession session)
      throws StorageException {
    ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO
        applicationAttemptStateDTO = session.newInstance(
        ApplicationAttemptStateClusterJ.ApplicationAttemptStateDTO.class);

    applicationAttemptStateDTO.setapplicationid(hop.getApplicationId());
    applicationAttemptStateDTO.setapplicationattemptid(hop.
        getApplicationattemptid());
    try {
      applicationAttemptStateDTO.setapplicationattemptstate(CompressionUtils.
          compress(hop.
              getApplicationattemptstate()));
    } catch (IOException e) {
      throw new StorageException(e);
    }
    applicationAttemptStateDTO.setapplicationattempthost(hop.getHost());
    applicationAttemptStateDTO.setapplicationattemptrpcport(hop.getRpcPort());
    if (hop.getAppAttemptTokens() != null) {
      applicationAttemptStateDTO.setapplicationattempttokens(hop.
          getAppAttemptTokens().array());
    }
    applicationAttemptStateDTO.setapplicationattempttrakingurl(hop.getUrl());
    return applicationAttemptStateDTO;
  }

  private Map<String, List<ApplicationAttemptState>> createMap(
      List<ApplicationAttemptStateDTO> results) throws StorageException {
    Map<String, List<ApplicationAttemptState>> map =
        new HashMap<String, List<ApplicationAttemptState>>();
    for (ApplicationAttemptStateDTO persistable : results) {
      ApplicationAttemptState hop =
          createHopApplicationAttemptState(persistable);
      if (map.get(hop.getApplicationId()) == null) {
        map.put(hop.getApplicationId(),
            new ArrayList<ApplicationAttemptState>());
      }
      map.get(hop.getApplicationId()).add(hop);
    }
    return map;
  }
}
