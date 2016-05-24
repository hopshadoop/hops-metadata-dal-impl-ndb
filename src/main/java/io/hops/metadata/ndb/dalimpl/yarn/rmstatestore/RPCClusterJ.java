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
    @Column(name = RPCID)
    int getrpcid();

    void setrpcid(int id);

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
    List<RPCDTO> queryResults = query.getResultList();
    
    boolean result = !(queryResults == null || queryResults.isEmpty());
    session.release(queryResults);
    return result;
  }

  @Override
  public void add(io.hops.metadata.yarn.entity.appmasterrpc.RPC toAdd)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    RPCDTO dto = createPersistable(toAdd, session);
    session.savePersistent(dto);
    session.release(dto);
  }

  @Override
  public void removeAll(List<io.hops.metadata.yarn.entity.appmasterrpc.RPC> toRemove)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    List<RPCDTO> dtoToRemove = new ArrayList<RPCDTO>();
    for(io.hops.metadata.yarn.entity.appmasterrpc.RPC rpc : toRemove){
      RPCDTO toRemoveDTO = session.newInstance(RPCDTO.class);
      toRemoveDTO.setrpcid(rpc.getRPCId());
      dtoToRemove.add(toRemoveDTO);
    }
    session
        .deletePersistentAll(dtoToRemove);
    session.release(dtoToRemove);
  }

  @Override
  public List<io.hops.metadata.yarn.entity.appmasterrpc.RPC> getAll()
          throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<RPCClusterJ.RPCDTO> dobj = qb.
            createQueryDefinition(RPCClusterJ.RPCDTO.class);
    HopsQuery<RPCClusterJ.RPCDTO> query = session.createQuery(dobj);
    List<RPCClusterJ.RPCDTO> queryResults = query.getResultList();
    List<RPC> result = createHopRPCList(queryResults);
    session.release(queryResults);
    return result;
  }

  private io.hops.metadata.yarn.entity.appmasterrpc.RPC createHopRPC(
      RPCDTO appMasterRPCDTO) throws StorageException {
    try {
      return new RPC(appMasterRPCDTO.getrpcid(),
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
    appMasterRPCDTO.setrpcid(hop.getRPCId());
    appMasterRPCDTO.settype(hop.getType().name());
    try {
      if (hop.getRpc() != null) {
        appMasterRPCDTO.setrpc(CompressionUtils.compress(hop.getRpc()));
      } else {
        appMasterRPCDTO.setrpc(new byte[0]);
      }
    } catch (IOException e) {
      throw new StorageException(e);
    }
    appMasterRPCDTO.setuserid(hop.getUserId());

    return appMasterRPCDTO;
  }
}
