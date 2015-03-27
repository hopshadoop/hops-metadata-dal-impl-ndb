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
import io.hops.metadata.yarn.dal.NodeDataAccess;
import io.hops.metadata.yarn.entity.Node;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodeClusterJ implements TablesDef.NodeTableDef, NodeDataAccess<Node> {

  private static final Log LOG = LogFactory.getLog(NodeClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface NodeDTO extends RMNodeComponentDTO {

    @PrimaryKey
    @Column(name = NODEID)
    String getnodeid();

    void setnodeid(String id);

    @Column(name = NAME)
    String getName();

    void setName(String host);

    @Column(name = LOCATION)
    String getLocation();

    void setLocation(String location);

    @Column(name = LEVEL)
    int getLevel();

    void setLevel(int level);

    @Column(name = PARENT)
    String getParent();

    void setParent(String parent);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Node findById(String id) throws StorageException {
    LOG.debug("HOP :: ClusterJ Node.findById - START:" + id);
    HopsSession session = connector.obtainSession();
    NodeDTO nodeDTO;
    if (session != null) {
      nodeDTO = session.find(NodeDTO.class, id);
      LOG.debug("HOP :: ClusterJ Node.findById - FINISH:" + id);
      if (nodeDTO != null) {
        return createHopNode(nodeDTO);
      }
    }
    return null;
  }

  @Override
  public Map<String, Node> getAll() throws StorageException {
    LOG.debug("HOP :: ClusterJ Node.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<NodeDTO> dobj = qb.createQueryDefinition(NodeDTO.class);
    HopsQuery<NodeDTO> query = session.createQuery(dobj);

    List<NodeDTO> results = query.getResultList();
    LOG.debug("HOP :: ClusterJ Node.getAll - FINISH");
    return createMap(results);
  }

  @Override
  public void addAll(Collection<Node> toAdd) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<NodeDTO> toPersist = new ArrayList<NodeDTO>();
    for (Node node : toAdd) {
      toPersist.add(createPersistable(node, session));
    }
    session.savePersistentAll(toPersist);
    session.flush();
  }

  @Override
  public void createNode(Node node) throws StorageException {
    HopsSession session = connector.obtainSession();
    session.savePersistent(createPersistable(node, session));
  }

  private NodeDTO createPersistable(Node hopNode, HopsSession session)
      throws StorageException {
    NodeDTO nodeDTO = session.newInstance(NodeDTO.class);
    //Set values to persist new rmnode
    nodeDTO.setnodeid(hopNode.getId());
    nodeDTO.setName(hopNode.getName());
    nodeDTO.setLocation(hopNode.getLocation());
    nodeDTO.setLevel(hopNode.getLevel());
    nodeDTO.setParent(hopNode.getParent());
    return nodeDTO;
  }

  /**
   * Transforms a DTO to Hop object.
   * <p/>
   * <p/>
   *
   * @param nodeDTO
   * @return HopRMNode
   */
  public static Node createHopNode(NodeDTO nodeDTO) {
    return new Node(nodeDTO.getnodeid(), nodeDTO.getName(), nodeDTO.
        getLocation(), nodeDTO.getLevel(), nodeDTO.getParent());
  }

  private Map<String, Node> createMap(List<NodeDTO> results) {
    Map<String, Node> map = new HashMap<String, Node>();
    for (NodeDTO persistable : results) {
      Node hop = createHopNode(persistable);
      map.put(hop.getId(), hop);
    }
    return map;
  }
}
