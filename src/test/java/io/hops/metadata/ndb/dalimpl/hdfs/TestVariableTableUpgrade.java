/*
 * Copyright (C) 2023 Hopsworks AB
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

import io.hops.DalDriver;
import io.hops.DalStorageFactory;
import io.hops.StorageConnector;
import io.hops.exception.StorageException;
import io.hops.metadata.common.entity.Variable;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.fail;


public class TestVariableTableUpgrade {

  String dropVariableTable = "drop table if exists " + TablesDef.VariableTableDef.TABLE_NAME;

  String truncateVariableTable = "truncate " + TablesDef.VariableTableDef.TABLE_NAME;

  String createTableNullable =
          "CREATE TABLE `" + TablesDef.VariableTableDef.TABLE_NAME + "`" +
                  " (`id` int NOT NULL,`value`" + " varbinary" + "(500) ,PRIMARY" + " KEY (`id`))" +
                  " ENGINE=ndbcluster " + "DEFAULT CHARSET=latin1 COLLATE=latin1_general_cs";

  String createTableNotNullable =
          "CREATE TABLE `" + TablesDef.VariableTableDef.TABLE_NAME + "` (`id` int NOT NULL,`value` "
                  + "varbinary" + "(500) " + "NOT NULL ,PRIMARY" + " KEY (`id`)) ENGINE=ndbcluster " +
                  "DEFAULT " + "CHARSET=latin1 COLLATE=latin1_general_cs";

  //from a running clusters
  String var_3_2_0_8 =
          "INSERT INTO `" + TablesDef.VariableTableDef.TABLE_NAME + "` VALUES (0,0x0000000000038270)," + "(3," +
                  "0x14000001DA1400000000140000000014000000001400000000),(6,NULL),(7,NULL)," + "(9,0x00" +
                  "000001),(10,0x2B45564943545F464C41473A66616C73653B54494D455F504552494F443A313030303B4D" +
                  "41585F49443A3233)," + "(15,0x0408011005),(16,0x0000000000000049),(17,0x170A150882EFD" +
                  "4A8FAFFFFFFFF0112083B44D1B6666115B8)," + "(18,NULL),(20,NULL),(23,NULL),(24,NULL),(" +
                  "26,NULL),(28,0x00000000),(29,NULL),(31,NULL),(34,0x000001E7)," + "(36,0xFFFFFFFF),(3" +
                  "7,0x00000185A5FA0EDD),(41,0x0000000063F76C3F),(1,0x0000000000038272),(2,0x00231860)," +
                  "" + "(4,0x14FFFFFFC9140000038F16284349442D35663235626462622D383133612D343331" + "30" +
                  "2D616336312D6238383762326338626430661500000185A5FA183B162442502D3636313131373036322" +
                  "D31302E302E3" + "22E31352D3136373335323639303930353316094E414D455F4E4F4445),(5,NULL)," +
                  "(8,NULL),(11,0x2745" + "564943545F464C41473A66616C73653B54494D455F504552494F443A303B4" +
                  "D41585F49443A30),(12,0x0000000000035B60)," + "(13,0xFFFFFFFFFFFFFFFF),(14,0x000000000" +
                  "0000001),(19,NULL),(21,NULL),(22,NULL),(27,0x0000000000231861),(" + "30,NULL),(32,0x0" +
                  "00001E8),(33,0x000001E7),(35,0x0001DA78),(38,0x0000000000000000),(39,0x00000000000000" +
                  "49)";

  String var_3_2_0_9_SNAPSHOT =
          "INSERT INTO `" + TablesDef.VariableTableDef.TABLE_NAME + "` VALUES (0,0x0000000000002710)," +
                  "(3," + "0x140000003E1400000000140000000014000000001400000000),(6,NULL),(7,NULL),(9,0x" +
                  "00000001)," + "(10,0x2A45564943545F464C41473A66616C73653B54494D455F504552494F443A3130" +
                  "30303B4D41585F49443A31)," + "(15,0x0408011005),(16,0x0000000000000001),(17,0x120A1008" +
                  "93C486D4071208DCF8679A5463C45C)," + "(18,NULL),(20,NULL),(23,NULL),(24,NULL),(26,NUL" +
                  "L),(28,0x00000000),(29,NULL),(31,NULL)," + "(34,0x00000000),(36,0xFFFFFFFF),(37,0x0" +
                  "00001867EB3B023),(41,0x0000000063F84F24),(1,0x0000000000002712)," + "(2,0x000186A0),(" +
                  "4,0x14FFFFFFC9140000038F16284349442D33656632383137362D363935622D346631622D61663963" +
                  "2D36323930393932633962316415000001867EB3B890162342502D33393436303838332D31302E302E322" +
                  "E31352D3136" + "373731363239353239303316094E414D455F4E4F4445),(5,NULL),(8,NULL),(11," +
                  "0x2745564943545F464C41473A66616C73653B54494D4" + "55F504552494F443A303B4D41585F49443A" +
                  "30),(12,0x0000000000000000),(13,0xFFFFFFFFFFFFFFFF),(14,0x0000000000000001),(19,NULL)," +
                  "(2" + "1,NULL),(22,NULL),(27,0x00000000000186A1),(30,NULL),(32,0x00000000),(33,0x0000" +
                  "0000),(35,0x000007" + "14),(38,0x0000000000000000),(39,0x0000000000000001)";

  // all columns are null
  String allNulls =
          "INSERT INTO `" + TablesDef.VariableTableDef.TABLE_NAME + "` VALUES (0,NULL),(3,NULL),(6,NULL),(7,NULL)," +
                  "(9," + "NULL)," + "(10,NULL),(15,NULL),(16,NULL),(17,NULL),(18,NULL),(20,NULL),(23,NULL)" +
                  ",(24,NULL),(26,NULL),(28,NULL)," + "(29,NULL),(31,NULL),(34,NULL),(36,NULL),(37,NULL)" +
                  ",(41,NULL),(1,NULL),(2,NULL),(4,NULL),(5,NULL),(8," + "NULL),(11,NULL),(12,NULL),(13," +
                  "NULL),(14,NULL),(19,NULL),(21,NULL),(22,NULL),(27,NULL),(30" + ",NULL),(32,NULL),(33" +
                  ",NULL),(35,NULL),(38,NULL),(39,NULL)";

  // this upstream without changes for hdfsvvairables
  String var_3_2_0_9_SNAPSHOT_before_fx =
          "INSERT INTO `" + TablesDef.VariableTableDef.TABLE_NAME + "` VALUES (0,0x0000000000000000)," +
                  "(3,0x14000000001400000000140000000014000000001400000000),(6,NULL),(7,NULL),(9,0x00000000)" +
                  ",(10,0x2745564943545F464C41473A66616C73653B54494D455F504552494F443A303B4D41585F49443A30)" +
                  ",(15,NULL),(16,NULL),(17,NULL),(18,NULL),(20,NULL),(23,NULL),(24,NULL),(26,NULL),(28," +
                  "0x00000000),(29,NULL),(31,NULL),(34,NULL),(36,0xFFFFFFFF),(37,0x00000186833C1544)," +
                  "(41,0x0000000000000000),(1,0x0000000000000002),(2,0x00000000),(4,0x14FFFFFFC9140" +
                  "000038F16284349442D31366230396261352D373831342D343339382D386336362D38643965623835" +
                  "61376365351500000186833C1D96162842502D3232343639383732372D3139322E3136382E312E31" +
                  "30322D3136373732333930303035303716094E414D455F4E4F4445),(5,NULL),(8,NULL),(11,0x2" +
                  "745564943545F464C41473A66616C73653B54494D455F504552494F443A303B4D41585F49443A30)," +
                  "(12,0x0000000000000000),(13,0xFFFFFFFF),(14,0x0000000000000001),(19,NULL),(21,NU" +
                  "LL),(22,NULL),(27,0x0000000000000001),(30,NULL),(32,NULL),(33,NULL),(35,0x000000" +
                  "00),(38,0x0000000000000000),(39,NULL)";

  String var_3_2_0_9_SNAPSHOT_after_fx =

          "INSERT INTO `" + TablesDef.VariableTableDef.TABLE_NAME + "` VALUES (0,0x000000000000000" +
                  "0),(3,0x14000000001400000000140000000014000000001400000000),(6,0x00),(7,0x00),(9" +
                  ",0x00000000),(10,0x2745564943545F464C41473A66616C73653B54494D455F504552494F443A30" +
                  "3B4D41585F49443A30),(15,0x00),(16,0x0000000000000000),(17,0x00),(18,0x00000000)," +
                  "(20,0x00000000),(23,0x00),(24,''),(26,0x00000000),(28,0x00000000),(29,0x00),(31," +
                  "0x0000000000000000),(34,0x00000000),(36,0xFFFFFFFF),(37,0x00000186833C1544),(41," +
                  "0x0000000000000000),(1,0x0000000000000002),(2,0x00000000),(4,0x14FFFFFFC91400000" +
                  "38F16284349442D31383862643734332D353932302D346330362D396633372D3265396634326638343" +
                  "836611500000186833F7720162942502D323032393031383339392D3139322E3136382E312E313032" +
                  "2D3136373732333932323030333716094E414D455F4E4F4445),(5,''),(8,0x00),(11,0x2745564" +
                  "943545F464C41473A66616C73653B54494D455F504552494F443A303B4D41585F49443A30),(12,0x" +
                  "0000000000000000),(13,0xFFFFFFFF),(14,0x0000000000000001),(19,0x000000000000000" +
                  "0),(21,0x0000000000000000),(22,0x00),(27,0x0000000000000001),(30,''),(32,0x0000000" +
                  "0),(33,0x00000000),(35,0x00000000),(38,0x0000000000000000),(39,0x0000000000000000" +
                  ")";

  public void initDB() throws IOException {
    DalStorageFactory dStorageFactory = DalDriver.load("io.hops.metadata.ndb.NdbStorageFactory");

    String configFile = "ndb-config.properties";
    Properties clusterConf = new Properties();
    InputStream inStream =
            StorageConnector.class.getClassLoader().getResourceAsStream(configFile);
    clusterConf.load(inStream);
    if (inStream == null) {
      throw new FileNotFoundException("Unable to load database configuration file");
    }

    dStorageFactory.setConfiguration(clusterConf);
  }

  @Test
  public void upgrateTest() throws IOException, SQLException {
    initDB();

    try {
      VariableClusterj da = new VariableClusterj();

      runSQLQuery(dropVariableTable);
      runSQLQuery(createTableNullable);
      runSQLQuery(var_3_2_0_9_SNAPSHOT_before_fx);
      ClusterjConnector.getInstance().obtainSession().unloadSchema(VariableClusterj.VariableDTO.class);

      Map<Variable.Finder, Variable> variables1 = new HashMap<>();
      if (!readAllVariables(da, variables1)) {
        fail("Failed to read all variables");
      }

      runSQLQuery(truncateVariableTable);
      runSQLQuery(var_3_2_0_9_SNAPSHOT_after_fx);
      ClusterjConnector.getInstance().obtainSession().unloadSchema(VariableClusterj.VariableDTO.class);
      Map<Variable.Finder, Variable> variables2 = new HashMap<>();
      if (!readAllVariables(da, variables2)) {
        fail("Failed to read all variables");
      }

      // compare the variables
      for (Variable.Finder vf : variables1.keySet()) {
        Variable var1 = variables1.get(vf);
        Variable var2 = variables2.get(vf);

        if (var1 == null || var2 == null) {
          fail("failed to read variable " + vf);
        }

        if (vf == Variable.Finder.StorageInfo) {
          //expected to not match due to random data
          continue;
        }

        if (!var1.equals(var2)) {
          fail("Variable do not match " + vf + "  var1: " + var1 + "  var2: " + var2);
        }
      }

      runSQLQuery(truncateVariableTable);
      runSQLQuery(allNulls);
      ClusterjConnector.getInstance().obtainSession().unloadSchema(VariableClusterj.VariableDTO.class);
      Map<Variable.Finder, Variable> variables = new HashMap<>();
      if (!readAllVariables(da, variables)) {
        fail("Failed to read all variables");
      }

      runSQLQuery(truncateVariableTable);
      runSQLQuery(var_3_2_0_8); //variable table copied from a running cluster
      ClusterjConnector.getInstance().obtainSession().unloadSchema(VariableClusterj.VariableDTO.class);
      variables.clear();
      if (!readAllVariables(da, variables)) {
        fail("Failed to read all variables");
      }

      runSQLQuery(truncateVariableTable);
      runSQLQuery(var_3_2_0_9_SNAPSHOT); //variable table copied from a running cluster
      ClusterjConnector.getInstance().obtainSession().unloadSchema(VariableClusterj.VariableDTO.class);
      variables.clear();
      if (!readAllVariables(da, variables)) {
        fail("Failed to read all variables");
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      //restore the table
      runSQLQuery(dropVariableTable);
      runSQLQuery(createTableNotNullable);
    }
  }

  private void runSQLQuery(String sql) throws StorageException, SQLException {
    Connection conn = MysqlServerConnector.getInstance().obtainSession();
    Statement statement = conn.createStatement();
    statement.execute(sql);
    statement.close();
  }

  private boolean readAllVariables(VariableClusterj vcj,
                                   Map<Variable.Finder, Variable> variables) {
    boolean pass = true;
    for (Variable.Finder varType : Variable.Finder.values()) {
      try {
        Variable v = vcj.getVariable(varType);
        variables.put(varType, v);
      } catch (Exception e) {
        System.err.println("Error in reading variable:  " + varType + " Error:" + e.getMessage());
        e.printStackTrace();
        pass = false;
      }
    }
    return pass;
  }
}
