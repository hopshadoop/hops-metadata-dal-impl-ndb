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
package io.hops.metadata.ndb.dalimpl.yarn.capacity;

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
import io.hops.metadata.yarn.dal.capacity.CSPreemptedContainersDataAccess;
import io.hops.metadata.yarn.entity.capacity.CSPreemptedContainers;

import java.util.*;

public class CSPreemptedContainersClusterJ implements TablesDef.CSPreemptedContainersTableDef,
        CSPreemptedContainersDataAccess<CSPreemptedContainers> {

    @PersistenceCapable(table = TABLE_NAME)
    public interface CSPreemptedContainersDTO {

        @PrimaryKey
        @Column(name = RMCONTAINER_ID)
        String getrmcontainer_id();

        void setrmcontainer_id(String rmcontainer_id);

        @Column(name = PREEMPTION_TIME)
        long getpreemption_time();

        void setpreemption_time(long preemption_time);
    }

    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public CSPreemptedContainers findById(String rmContainerId) throws StorageException {
        HopsSession session = connector.obtainSession();

        CSPreemptedContainersDTO preemptedDTO = session.find(CSPreemptedContainersDTO.class,
                rmContainerId);

        if (preemptedDTO != null) {
            CSPreemptedContainers result = createHopContainer(preemptedDTO);
            session.release(preemptedDTO);
            return result;
        }

        return null;
    }

    @Override
    public Map<String, CSPreemptedContainers> getAll() throws StorageException {
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder queryBuilder = session.getQueryBuilder();
        HopsQueryDomainType<CSPreemptedContainersDTO> dto =
                queryBuilder.createQueryDefinition(CSPreemptedContainersDTO.class);
        HopsQuery<CSPreemptedContainersDTO> query =
                session.createQuery(dto);
        List<CSPreemptedContainersDTO> resultSet = query.getResultList();

        Map<String, CSPreemptedContainers> resultMap = createMap(resultSet);
        session.release(resultSet);

        return resultMap;
    }

    @Override
    public void addAll(Collection<CSPreemptedContainers> preemptedContainers)
            throws StorageException {
        HopsSession session = connector.obtainSession();

        if (preemptedContainers != null
                && !preemptedContainers.isEmpty()) {
            List<CSPreemptedContainersDTO> toPersist =
                    new ArrayList<CSPreemptedContainersDTO>();
            for (CSPreemptedContainers hop : preemptedContainers) {
                CSPreemptedContainersDTO persist = createPersistable(hop, session);
                toPersist.add(persist);
            }

            session.savePersistentAll(toPersist);
            session.release(toPersist);
        }
    }

    @Override
    public void removeAll(Collection<CSPreemptedContainers> killed)
            throws StorageException {
        HopsSession session = connector.obtainSession();
        List<CSPreemptedContainersDTO> toBeRemoved =
                new ArrayList<CSPreemptedContainersDTO>();
        for (CSPreemptedContainers cont : killed) {
            toBeRemoved.add(createPersistable(cont, session));
        }

        if (!toBeRemoved.isEmpty()) {
            session.deletePersistentAll(toBeRemoved);
            session.release(toBeRemoved);
        }
    }

    private Map<String, CSPreemptedContainers> createMap(List<CSPreemptedContainersDTO> resultSet) {
        Map<String, CSPreemptedContainers> resultMap = new HashMap<String, CSPreemptedContainers>();
        CSPreemptedContainers preemptedContainer;

        for (CSPreemptedContainersDTO dto : resultSet) {
            preemptedContainer = createHopContainer(dto);
            resultMap.put(dto.getrmcontainer_id(), preemptedContainer);
        }

        return resultMap;
    }

    private CSPreemptedContainers createHopContainer(CSPreemptedContainersDTO dto) {
        return new CSPreemptedContainers(dto.getrmcontainer_id(),
                dto.getpreemption_time());
    }

    private CSPreemptedContainersDTO createPersistable(CSPreemptedContainers hop,
                                                       HopsSession session)
            throws StorageException {
        CSPreemptedContainersDTO csPreemptedContainersDTO = session
                .newInstance(CSPreemptedContainersDTO.class);

        csPreemptedContainersDTO.setrmcontainer_id(hop.getRmContainerId());
        csPreemptedContainersDTO.setpreemption_time(hop.getPreemptionTime());

        return csPreemptedContainersDTO;
    }
}
