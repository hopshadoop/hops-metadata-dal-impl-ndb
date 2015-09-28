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


import com.google.common.collect.Lists;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.GroupDataAccess;
import io.hops.metadata.hdfs.entity.Group;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.Collection;
import java.util.List;

public class GroupClusterj implements TablesDef.GroupsTableDef, GroupDataAccess<Group>{

  @PersistenceCapable(table = TABLE_NAME)
  public interface GroupDTO {

    @PrimaryKey
    @Column(name = ID)
    byte[] getId();

    void setId(byte[] id);

    @Column(name = Name)
    String getName();

    void setName(String name);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Group getGroup(byte[] id) throws StorageException {
    HopsSession session = connector.obtainSession();
    GroupDTO groupDTO = session.find(GroupDTO.class, id);
    Group group = new Group(groupDTO.getId(), groupDTO.getName());
    session.release(groupDTO);
    return group;
  }

  @Override
  public void addGroup(Group group) throws StorageException {
    HopsSession session = connector.obtainSession();
    GroupDTO groupDTO = session.newInstance(GroupDTO.class);
    groupDTO.setId(group.getId());
    groupDTO.setName(group.getName());
    session.savePersistent(groupDTO);
    session.release(groupDTO);
  }

  @Override
  public Collection<Group> getGroups(Collection<byte[]> ids)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    Collection<GroupDTO> groupDTOs = getGroups(session, ids);
    return convertAndRelease(session, groupDTOs);
  }

  static Collection<GroupDTO> getGroups(HopsSession session, Collection<byte[]>
      ids)
      throws StorageException {
    List<GroupDTO> groupDTOs = Lists.newArrayListWithCapacity(ids.size());
    for(byte[] id : ids){
      GroupClusterj.GroupDTO groupDTO = session.newInstance(GroupClusterj
          .GroupDTO.class, id);
      session.load(groupDTO);
      groupDTOs.add(groupDTO);
    }
    session.flush();
    return groupDTOs;
  }

  static List<Group> convertAndRelease(HopsSession session, Collection<GroupDTO>
      dtos)
      throws StorageException {
    List<Group> groups = Lists.newArrayListWithExpectedSize(dtos.size());
    for(GroupDTO dto : dtos){
      groups.add(new Group(dto.getId(), dto.getName()));
      session.release(dto);
    }
    return groups;
  }
}
