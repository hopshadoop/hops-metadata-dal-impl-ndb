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
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.QueueMetricsDataAccess;
import io.hops.metadata.yarn.entity.QueueMetrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class QueueMetricsClusterJ
    implements TablesDef.QueueMetricsTableDef, QueueMetricsDataAccess<QueueMetrics> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface QueueMetricsDTO {

    @PrimaryKey
    @Column(name = QUEUE_NAME)
    String getqueuename();

    void setqueuename(String queuename);
    @Column(name = APPS_SUBMITTED)
    int getappssubmitted();

    void setappssubmitted(int appssubmitted);

    @Column(name = APPS_RUNNING)
    int getappsrunning();

    void setappsrunning(int appsrunning);

    @Column(name = APPS_PENDING)
    int getappspending();

    void setappspending(int appspending);

    @Column(name = APPS_COMPLETED)
    int getappscompleted();

    void setappscompleted(int appscompleted);

    @Column(name = APPS_KILLED)
    int getappskilled();

    void setappskilled(int appskilled);

    @Column(name = APPS_FAILED)
    int getappsfailed();

    void setappsfailed(int appsfailed);

    @Column(name = ALLOCATED_MB)
    int getallocatedmb();

    void setallocatedmb(int allocatedmb);

    @Column(name = ALLOCATED_VCORES)
    int getallocatedvcores();

    void setallocatedvcores(int allocatedvcores);

    @Column(name = ALLOCATED_CONTAINERS)
    int getallocatedcontainers();

    void setallocatedcontainers(int allocatedcontainers);

    @Column(name = AGGREGATE_CONTAINERS_ALLOCATED)
    long getaggregatecontainersallocated();

    void setaggregatecontainersallocated(long aggregatecontainersallocated);

    @Column(name = AGGREGATE_CONTAINERS_RELEASED)
    long getaggregatecontainersreleased();

    void setaggregatecontainersreleased(long aggregatecontainersreleased);

    @Column(name = AVAILABLE_MB)
    int getavailablemb();

    void setavailablemb(int availablemb);

    @Column(name = AVAILABLE_VCORES)
    int getavailablevcores();

    void setavailablevcores(int availablevcores);

    @Column(name = PENDING_MB)
    int getpendingmb();

    void setpendingmb(int pendingmb);

    @Column(name = PENDING_VCORES)
    int getpendingvcores();

    void setpendingvcores(int pendingvcores);

    @Column(name = PENDING_CONTAINERS)
    int getpendingContainers();

    void setpendingContainers(int pendingcontainers);

    @Column(name = RESERVED_MB)
    int getreservedmb();

    void setreservedmb(int reservedmb);

    @Column(name = RESERVED_VCORES)
    int getreservedvcores();

    void setreservedvcores(int reservedvcores);

    @Column(name = RESERVED_CONTAINERS)
    int getreservedcontainers();

    void setreservedcontainers(int reservedcontainers);

    @Column(name = ACTIVE_USERS)
    int getactiveusers();

    void setactiveusers(int activeusers);

    @Column(name = ACTIVE_APPLICATIONS)
    int getactiveapplications();

    void setactiveapplications(int activeapplications);

    @Column(name = PARENT_ID)
    int getparentid();

    void setparentid(int parentid);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public List<QueueMetrics> findAll() throws StorageException, IOException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<QueueMetricsClusterJ.QueueMetricsDTO> dobj = qb.
        createQueryDefinition(QueueMetricsClusterJ.QueueMetricsDTO.class);
    HopsQuery<QueueMetricsClusterJ.QueueMetricsDTO> query =
        session.createQuery(dobj);
    List<QueueMetricsClusterJ.QueueMetricsDTO> results = query.getResultList();
    session.flush();
    return createHopQueueMetricsList(results);

  }
  
  @Override
  public void addAll(Collection<QueueMetrics> toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<QueueMetricsDTO> toPersist = new ArrayList<QueueMetricsDTO>();
    for (QueueMetrics hop : toAdd) {
      QueueMetricsClusterJ.QueueMetricsDTO persistable =
          createPersistable(hop, session);
      toPersist.add(persistable);
    }
    session.savePersistentAll(toPersist);
  }

  private List<QueueMetrics> createHopQueueMetricsList(
      List<QueueMetricsClusterJ.QueueMetricsDTO> list) throws IOException {
    List<QueueMetrics> queueMetricsList = new ArrayList<QueueMetrics>();
    for (QueueMetricsClusterJ.QueueMetricsDTO persistable : list) {
      queueMetricsList.add(createHopQueueMetrics(persistable));
    }
    return queueMetricsList;
  }


  private QueueMetrics createHopQueueMetrics(QueueMetricsDTO queueMetricsDTO) {
    return new QueueMetrics(queueMetricsDTO.getqueuename(),
        queueMetricsDTO.getappssubmitted(), queueMetricsDTO.getappsrunning(),
        queueMetricsDTO.getappspending(), queueMetricsDTO.getappscompleted(),
        queueMetricsDTO.getappskilled(), queueMetricsDTO.getappsfailed(),
        queueMetricsDTO.getallocatedmb(), queueMetricsDTO.getallocatedvcores(),
        queueMetricsDTO.getallocatedcontainers(),
        queueMetricsDTO.getaggregatecontainersallocated(),
        queueMetricsDTO.getaggregatecontainersreleased(),
        queueMetricsDTO.getavailablemb(), queueMetricsDTO.getavailablevcores(),
        queueMetricsDTO.getpendingmb(), queueMetricsDTO.getpendingvcores(),
        queueMetricsDTO.getpendingContainers(), queueMetricsDTO.getreservedmb(),
        queueMetricsDTO.getreservedvcores(),
        queueMetricsDTO.getreservedcontainers(),
        queueMetricsDTO.getactiveusers(),
        queueMetricsDTO.getactiveapplications(), queueMetricsDTO.getparentid());
  }

  private QueueMetricsDTO createPersistable(QueueMetrics hop,
      HopsSession session) throws StorageException {
    QueueMetricsClusterJ.QueueMetricsDTO queueMetricsDTO =
        session.newInstance(QueueMetricsClusterJ.QueueMetricsDTO.class);

    queueMetricsDTO.setqueuename(hop.getQueuename());
    queueMetricsDTO.setactiveapplications(hop.getActiveapplications());
    queueMetricsDTO.setactiveusers(hop.getActiveusers());
    queueMetricsDTO.setaggregatecontainersallocated(hop.
        getAggregatecontainersallocated());
    queueMetricsDTO.setaggregatecontainersreleased(hop.
        getAggregatecontainersreleased());
    queueMetricsDTO.setallocatedcontainers(hop.getAllocatedcontainers());
    queueMetricsDTO.setallocatedmb(hop.getAllocatedmb());
    queueMetricsDTO.setallocatedvcores(hop.getAllocatedvcores());
    queueMetricsDTO.setappscompleted(hop.getAppscompleted());
    queueMetricsDTO.setappsfailed(hop.getAppsfailed());
    queueMetricsDTO.setappskilled(hop.getAppskilled());
    queueMetricsDTO.setappspending(hop.getAppspending());
    queueMetricsDTO.setappsrunning(hop.getAppsrunning());
    queueMetricsDTO.setappssubmitted(hop.getAppssubmitted());
    queueMetricsDTO.setavailablemb(hop.getAvailablemb());
    queueMetricsDTO.setavailablevcores(hop.getAvailablevcores());
    queueMetricsDTO.setparentid(hop.getParentid());
    queueMetricsDTO.setpendingmb(hop.getPendingmb());
    queueMetricsDTO.setpendingvcores(hop.getPendingvcores());
    queueMetricsDTO.setreservedcontainers(hop.getReservedcontainers());
    queueMetricsDTO.setreservedmb(hop.getReservedmb());
    queueMetricsDTO.setreservedvcores(hop.getReservedvcores());

    return queueMetricsDTO;
  }

}
