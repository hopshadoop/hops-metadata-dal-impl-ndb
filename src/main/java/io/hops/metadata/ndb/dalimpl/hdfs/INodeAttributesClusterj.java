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
package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.INodeAttributesDataAccess;
import io.hops.metadata.hdfs.entity.INodeAttributes;
import io.hops.metadata.hdfs.entity.INodeCandidatePrimaryKey;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.dalimpl.ClusterjDataAccess;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class INodeAttributesClusterj extends ClusterjDataAccess
    implements TablesDef.INodeAttributesTableDef, INodeAttributesDataAccess<INodeAttributes> {

  public INodeAttributesClusterj(ClusterjConnector connector) {
    super(connector);
  }

  @PersistenceCapable(table = TABLE_NAME)
  public interface INodeAttributesDTO {

    @PrimaryKey
    @Column(name = ID)
    int getId();

    void setId(int id);

    @Column(name = NSQUOTA)
    long getNSQuota();

    void setNSQuota(long nsquota);

    @Column(name = DSQUOTA)
    long getDSQuota();

    void setDSQuota(long dsquota);

    @Column(name = NSCOUNT)
    long getNSCount();

    void setNSCount(long nscount);

    @Column(name = DISKSPACE)
    long getDiskspace();

    void setDiskspace(long diskspace);
  }

  @Override
  public INodeAttributes findAttributesByPk(Integer inodeId)
      throws StorageException {
    HopsSession session = getConnector().obtainSession();
    INodeAttributesDTO dto = session.find(INodeAttributesDTO.class, inodeId);
    INodeAttributes iNodeAttributes = null;
    if (dto != null) {
      iNodeAttributes = makeINodeAttributes(dto);
      session.release(dto);
    }
    return iNodeAttributes;
  }

  @Override
  public Collection<INodeAttributes> findAttributesByPkList(
      List<INodeCandidatePrimaryKey> inodePks) throws StorageException {
    HopsSession session = getConnector().obtainSession();
    List<INodeAttributes> inodeAttributesBatchResponse =
        new ArrayList<INodeAttributes>();
    List<INodeAttributesDTO> inodeAttributesBatchRequest =
        new ArrayList<INodeAttributesDTO>();
    for (INodeCandidatePrimaryKey pk : inodePks) {
      INodeAttributesDTO dto = session.newInstance(INodeAttributesDTO.class);
      dto.setId(pk.getInodeId());
      inodeAttributesBatchRequest.add(dto);
      session.load(dto);
    }

    session.flush();

    for (int i = 0; i < inodeAttributesBatchRequest.size(); i++) {
      inodeAttributesBatchResponse
          .add(makeINodeAttributes(inodeAttributesBatchRequest.get(i)));
    }

    session.release(inodeAttributesBatchRequest);
    return inodeAttributesBatchResponse;
  }

  @Override
  public void prepare(Collection<INodeAttributes> modified,
                      Collection<INodeAttributes> removed) throws StorageException {
    HopsSession session = getConnector().obtainSession();
    List<INodeAttributesDTO> changes = new ArrayList<INodeAttributesDTO>();
    List<INodeAttributesDTO> deletions = new ArrayList<INodeAttributesDTO>();
    if (removed != null) {
      for (INodeAttributes attr : removed) {
        INodeAttributesDTO persistable =
            session.newInstance(INodeAttributesDTO.class, attr.getInodeId());
        deletions.add(persistable);
      }
    }
    if (modified != null) {
      for (INodeAttributes attr : modified) {
        INodeAttributesDTO persistable = createPersistable(attr, session);
        changes.add(persistable);
      }
    }
    session.deletePersistentAll(deletions);
    session.savePersistentAll(changes);
    session.release(deletions);
    session.release(changes);
  }

  private INodeAttributesDTO createPersistable(INodeAttributes attribute,
                                               HopsSession session) throws StorageException {
    INodeAttributesDTO dto = session.newInstance(INodeAttributesDTO.class);
    dto.setId(attribute.getInodeId());
    dto.setNSQuota(attribute.getNsQuota());
    dto.setNSCount(attribute.getNsCount());
    dto.setDSQuota(attribute.getDsQuota());
    dto.setDiskspace(attribute.getDiskspace());
    return dto;
  }

  private INodeAttributes makeINodeAttributes(INodeAttributesDTO dto) {
    if (dto == null) {
      return null;
    }
    INodeAttributes iNodeAttributes =
        new INodeAttributes(dto.getId(), dto.getNSQuota(), dto.getNSCount(),
            dto.getDSQuota(), dto.getDiskspace());
    return iNodeAttributes;
  }
}