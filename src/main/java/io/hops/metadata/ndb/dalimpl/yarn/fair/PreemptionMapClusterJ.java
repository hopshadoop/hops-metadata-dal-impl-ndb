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
package io.hops.metadata.ndb.dalimpl.yarn.fair;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.fair.PreemptionMapDataAccess;
import io.hops.metadata.yarn.entity.fair.PreemptionMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PreemptionMapClusterJ implements TablesDef.PreemptionMapTableDef,
        PreemptionMapDataAccess<PreemptionMap> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface PreemptionMapDTO {

    @PrimaryKey
    @Column(name = SCHEDULERAPP_ID)
    String getschedulerapp_id();

    void setschedulerapp_id(String schedulerapp_id);

    @Column(name = RMCONTAINER_ID)
    String getrmcontainer_id();

    void setrmcontainer_id(String rmcontainer_id);

    @Column(name = VALUE)
    long getvalue();

    void setvalue(long value);

  }
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<PreemptionMap> findById(String appAttemptId) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<PreemptionMapClusterJ.PreemptionMapDTO> dobj = qb.
            createQueryDefinition(PreemptionMapClusterJ.PreemptionMapDTO.class);
    HopsPredicate pred1 = dobj.get("schedulerapp_id").equal(dobj.param(
            "schedulerapp_id"));
    dobj.where(pred1);
    HopsQuery<PreemptionMapClusterJ.PreemptionMapDTO> query = session.
            createQuery(dobj);
    query.setParameter("schedulerapp_id", appAttemptId);

    List<PreemptionMapClusterJ.PreemptionMapDTO> results = query.
            getResultList();
    return createPreemtionMapList(results);
  }

  @Override
  public Map<String, List<PreemptionMap>> getAll() throws IOException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<PreemptionMapClusterJ.PreemptionMapDTO> dobj
            = qb.createQueryDefinition(
                    PreemptionMapClusterJ.PreemptionMapDTO.class);
    HopsQuery<PreemptionMapClusterJ.PreemptionMapDTO> query = session.
            createQuery(dobj);
    List<PreemptionMapClusterJ.PreemptionMapDTO> results = query.
            getResultList();
    return createMap(results);
  }

  @Override
  public void addAll(Collection<PreemptionMap> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    if (modified != null) {
      List<PreemptionMapClusterJ.PreemptionMapDTO> toModify
              = new ArrayList<PreemptionMapClusterJ.PreemptionMapDTO>();
      for (PreemptionMap hop : modified) {
        PreemptionMapClusterJ.PreemptionMapDTO persistable
                = createPersistable(hop, session);
        toModify.add(persistable);
      }
      session.savePersistentAll(toModify);
    }
  }

  @Override
  public void removeAll(Collection<PreemptionMap> removed) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    if (removed != null) {
      List<PreemptionMapClusterJ.PreemptionMapDTO> toRemove
              = new ArrayList<PreemptionMapClusterJ.PreemptionMapDTO>();
      for (PreemptionMap hop : removed) {
        Object[] objarr = new Object[2];
        objarr[0] = hop.getSchedulerappId();
        objarr[1] = hop.getRmcontainerId();
        toRemove.add(session.newInstance(
                PreemptionMapClusterJ.PreemptionMapDTO.class, objarr));
      }
      session.deletePersistentAll(toRemove);
    }
  }

  private PreemptionMap createPreemptionMap(PreemptionMapDTO preemptionMapDTO) {
    return new PreemptionMap(preemptionMapDTO.getschedulerapp_id(),
            preemptionMapDTO.getrmcontainer_id(),
            preemptionMapDTO.getvalue());
  }

  private PreemptionMapDTO createPersistable(PreemptionMap hop,
          HopsSession session) throws StorageException {
    PreemptionMapClusterJ.PreemptionMapDTO preemptionMapDTO = session.
            newInstance(PreemptionMapClusterJ.PreemptionMapDTO.class);

    preemptionMapDTO.setschedulerapp_id(hop.getSchedulerappId());
    preemptionMapDTO.setrmcontainer_id(hop.getRmcontainerId());
    preemptionMapDTO.setvalue(hop.getValue());

    return preemptionMapDTO;
  }

  private List<PreemptionMap> createPreemtionMapList(
          List<PreemptionMapDTO> results) {
    List<PreemptionMap> hopPreemptionMaps = new ArrayList<PreemptionMap>();
    for (PreemptionMapDTO persistable : results) {
      hopPreemptionMaps.add(createPreemptionMap(persistable));
    }
    return hopPreemptionMaps;
  }

  private Map<String, List<PreemptionMap>> createMap(
          List<PreemptionMapDTO> results) {
    Map<String, List<PreemptionMap>> map
            = new HashMap<String, List<PreemptionMap>>();
    for (PreemptionMapClusterJ.PreemptionMapDTO dto : results) {
      PreemptionMap hop
              = createPreemptionMap(dto);
      if (map.get(hop.getSchedulerappId()) == null) {
        map.put(hop.getSchedulerappId(),
                new ArrayList<PreemptionMap>());
      }
      map.get(hop.getSchedulerappId()).add(hop);
    }
    return map;
  }
}
