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
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.rmstatestore.RPCDataAccess;
import io.hops.metadata.yarn.entity.appmasterrpc.RPC;
import io.hops.util.CompressionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.DataFormatException;

public class RPCClusterJ implements TablesDef.RPCTableDef, RPCDataAccess<RPC> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface RPCDTO {

    @PrimaryKey
    @Column(name = ID)
    int getid();

    void setid(int id);

    @Column(name = TYPE)
    String gettype();

    void settype(String type);

    @Column(name = RPC)
    byte[] getrpc();

    void setrpc(byte[] rpc);

    @Column(name = USERID)
    String getuserid();

    void setuserid(String userid);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public boolean findByTypeAndUserId(String type, String userid)
      throws StorageException {
    //    StackTraceElement[] elements = Thread.currentThread().getStackTrace();
    //    StringBuilder sb = new StringBuilder();
    //    sb.append(", caller-");
    //    for (StackTraceElement elem : elements) {
    //      if (elem.getClassName().contains("RMUtilities")) {
    //        sb.append(elem.getClassName());
    //        sb.append("-");
    //        sb.append(elem.getMethodName());
    //      }
    //    }

    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<RPCDTO> dobj = qb.
        createQueryDefinition(RPCDTO.class);
    HopsPredicate pred1 = dobj.get(TYPE).equal(dobj.param(TYPE));
    HopsPredicate pred2 = dobj.get(USERID).equal(dobj.param(USERID));
    pred1 = pred1.and(pred2);
    dobj.where(pred1);
    HopsQuery<RPCDTO> query = session.
        createQuery(dobj);
    query.setParameter(TYPE, type);
    query.setParameter(USERID, userid);
    List<RPCDTO> results = query.getResultList();

    return !(results == null || results.isEmpty());
  }

  @Override
  public void add(io.hops.metadata.yarn.entity.appmasterrpc.RPC toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(toAdd, session));
  }

  @Override
  public void remove(io.hops.metadata.yarn.entity.appmasterrpc.RPC toRemove)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    session
        .deletePersistent(session.newInstance(RPCDTO.class, toRemove.getId()));
  }

  @Override
  public List<io.hops.metadata.yarn.entity.appmasterrpc.RPC> getAll()
      throws StorageException {
    try {
      HopsSession session = connector.obtainSession();
      HopsQueryBuilder qb = session.getQueryBuilder();
      HopsQueryDomainType<RPCClusterJ.RPCDTO> dobj = qb.
          createQueryDefinition(RPCClusterJ.RPCDTO.class);
      //Predicate pred1 = dobj.get("applicationid").equal(dobj.param("applicationid"));
      //dobj.where(pred1);
      HopsQuery<RPCClusterJ.RPCDTO> query = session.createQuery(dobj);
      //query.setParameter("applicationid", applicationid);
      List<RPCClusterJ.RPCDTO> results = query.getResultList();
      //            if (results != null && !results.isEmpty()) {
      return createHopRPCList(results);
      //            } else {

      //            }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  private io.hops.metadata.yarn.entity.appmasterrpc.RPC createHopRPC(
      RPCDTO appMasterRPCDTO) throws StorageException {
    try {
      return new RPC(appMasterRPCDTO.getid(),
          io.hops.metadata.yarn.entity.appmasterrpc.RPC.Type
              .valueOf(appMasterRPCDTO.gettype()),
          CompressionUtils.decompress(appMasterRPCDTO.getrpc()),
          appMasterRPCDTO.getuserid());
    } catch (IOException e) {
      throw new StorageException(e);
    } catch (DataFormatException e) {
      throw new StorageException(e);
    }
  }

  private List<io.hops.metadata.yarn.entity.appmasterrpc.RPC> createHopRPCList(
      List<RPCClusterJ.RPCDTO> list) throws StorageException {
    List<io.hops.metadata.yarn.entity.appmasterrpc.RPC> hopList =
        new ArrayList<io.hops.metadata.yarn.entity.appmasterrpc.RPC>();
    for (RPCClusterJ.RPCDTO dto : list) {
      hopList.add(createHopRPC(dto));
    }
    return hopList;

  }

  private RPCDTO createPersistable(
      io.hops.metadata.yarn.entity.appmasterrpc.RPC hop, HopsSession session)
      throws StorageException {
    RPCClusterJ.RPCDTO appMasterRPCDTO =
        session.newInstance(RPCClusterJ.RPCDTO.class);
    appMasterRPCDTO.setid(hop.getId());
    appMasterRPCDTO.settype(hop.getType().name());
    try {
      appMasterRPCDTO.setrpc(CompressionUtils.compress(hop.getRpc()));
    } catch (IOException e) {
      throw new StorageException(e);
    }
    appMasterRPCDTO.setuserid(hop.getUserId());

    return appMasterRPCDTO;
  }
}
