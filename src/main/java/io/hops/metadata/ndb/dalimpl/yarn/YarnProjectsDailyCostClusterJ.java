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
import io.hops.metadata.ndb.wrapper.HopsPredicate;

import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import static io.hops.metadata.yarn.TablesDef.YarnProjectsDailyCostTableDef.PROJECTNAME;
import static io.hops.metadata.yarn.TablesDef.YarnProjectsDailyCostTableDef.USER;
import static io.hops.metadata.yarn.TablesDef.YarnProjectsDailyCostTableDef.DAY;
import static io.hops.metadata.yarn.TablesDef.YarnProjectsDailyCostTableDef.CREDITS_USED;
import io.hops.metadata.yarn.dal.YarnProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.entity.YarnProjectsDailyCost;
import io.hops.metadata.yarn.entity.YarnProjectsDailyId;
import java.util.ArrayList;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author rizvi
 */
public class YarnProjectsDailyCostClusterJ implements
        TablesDef.YarnProjectsDailyCostTableDef,
        YarnProjectsDailyCostDataAccess<YarnProjectsDailyCost> {

  private static final Log LOG = LogFactory.getLog(
          YarnProjectsDailyCostClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface YarnProjectsDailyCostDTO {

    @PrimaryKey
    @Column(name = PROJECTNAME)
    String getProjectName();

    void setProjectName(String projectName);

    @PrimaryKey
    @Column(name = USER)
    String getUser();

    void setUser(String user);

    @PrimaryKey
    @Column(name = DAY)
    long getDay();

    void setDay(long day);

    @Column(name = CREDITS_USED)
    int getCreditUsed();

    void setCreditUsed(int credit);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Map<YarnProjectsDailyId, YarnProjectsDailyCost> getAll() throws
          StorageException {
    LOG.debug("HOP :: ClusterJ YarnProjectsDailyCost.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<YarnProjectsDailyCostDTO> dobj = qb.
            createQueryDefinition(YarnProjectsDailyCostDTO.class);
    HopsQuery<YarnProjectsDailyCostDTO> query = session.createQuery(dobj);

    List<YarnProjectsDailyCostDTO> queryResults = query.getResultList();
    LOG.debug("HOP :: ClusterJ YarnProjectsDailyCost.getAll - STOP");
    Map<YarnProjectsDailyId, YarnProjectsDailyCost> result = createMap(
            queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public Map<YarnProjectsDailyId, YarnProjectsDailyCost> getByDay(long day)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType dobj = qb.createQueryDefinition(
            YarnProjectsDailyCostDTO.class);

    HopsPredicate predicate = dobj.get(DAY).equal(dobj.param(DAY));
    dobj.where(predicate);
    HopsQuery query = session.createQuery(dobj);
    query.setParameter(DAY, day);

    List<YarnProjectsDailyCostDTO> dtos = query.getResultList();
    Map<YarnProjectsDailyId, YarnProjectsDailyCost> result = createMap(dtos);
    session.release(dtos);
    return result;

  }

  public static Map<YarnProjectsDailyId, YarnProjectsDailyCost> createMap(
          List<YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO> results) {
    Map<YarnProjectsDailyId, YarnProjectsDailyCost> map
            = new HashMap<YarnProjectsDailyId, YarnProjectsDailyCost>();
    for (YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO persistable
            : results) {
      YarnProjectsDailyCost hop = createHopYarnProjectsDailyCost(persistable);
      map.
              put(new YarnProjectsDailyId(hop.getProjectName(), hop.
                              getProjectUser(), hop.getDay()), hop);
    }
    return map;
  }

  private static YarnProjectsDailyCost createHopYarnProjectsDailyCost(
          YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO csDTO) {
    YarnProjectsDailyCost hop
            = new YarnProjectsDailyCost(csDTO.getProjectName(), csDTO.getUser(),
                    csDTO.getDay(), csDTO.getCreditUsed());
    return hop;
  }

  @Override
  public void addAll(Collection<YarnProjectsDailyCost> yarnProjectsDailyCost)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO> toAdd
            = new ArrayList<YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO>();
    for (YarnProjectsDailyCost _yarnProjectsDailyCost : yarnProjectsDailyCost) {
      toAdd.add(createPersistable(_yarnProjectsDailyCost, session));
    }
    session.savePersistentAll(toAdd);
    //    session.flush();
    session.release(toAdd);
  }

  private YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO createPersistable(
          YarnProjectsDailyCost hopPQ, HopsSession session) throws
          StorageException {
    YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO pqDTO = session.
            newInstance(
                    YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO.class);
    //Set values to persist new ContainerStatus
    pqDTO.setProjectName(hopPQ.getProjectName());
    pqDTO.setUser(hopPQ.getProjectUser());
    pqDTO.setDay(hopPQ.getDay());
    pqDTO.setCreditUsed(hopPQ.getCreditsUsed());

    return pqDTO;
  }
}
