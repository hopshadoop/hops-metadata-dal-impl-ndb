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
import io.hops.metadata.hdfs.dal.UserDataAccess;
import io.hops.metadata.hdfs.entity.User;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.Collection;
import java.util.List;

public class UserClusterj implements TablesDef.UsersTableDef, UserDataAccess<User>{

  @PersistenceCapable(table = TABLE_NAME)
  public interface UserDTO {

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
  public User getUser(byte[] id) throws StorageException {
    HopsSession session = connector.obtainSession();
    UserDTO userDTO = session.find(UserDTO.class, id);
    User user  = new User(userDTO.getId(), userDTO.getName());
    session.release(userDTO);
    return user;
  }

  @Override
  public void addUser(User user) throws StorageException{
    HopsSession session = connector.obtainSession();
    UserDTO userDTO = session.newInstance(UserDTO.class);
    userDTO.setId(user.getId());
    userDTO.setName(user.getName());
    session.savePersistent(userDTO);
    session.release(userDTO);
  }

  @Override
  public Collection<User> getUsers(Collection<byte[]> ids) throws StorageException{
    HopsSession session = connector.obtainSession();
    Collection<UserDTO> userDTOs = getUsers(session, ids);
    List<User> users = Lists.newArrayListWithExpectedSize(userDTOs.size());
    for(UserDTO userDTO : userDTOs){
      users.add(new User(userDTO.getId(), userDTO.getName()));
      session.release(userDTO);
    }
    return users;
  }

  static Collection<UserDTO> getUsers(HopsSession session, Collection<byte[]>
      ids) throws StorageException{
    List<UserDTO> userDTOs = Lists.newArrayListWithExpectedSize(ids.size());

    for(byte[] id : ids) {
      UserDTO userDTO = session.newInstance(UserDTO.class, id);
      session.load(userDTO);
      userDTOs.add(userDTO);
    }

    session.flush();
    return userDTOs;
  }

}
