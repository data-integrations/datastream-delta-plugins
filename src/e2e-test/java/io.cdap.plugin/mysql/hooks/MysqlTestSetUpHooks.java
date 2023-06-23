/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.mysql.hooks;

import com.google.cloud.bigquery.BigQueryException;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.mysql.actions.MysqlActions;
import io.cdap.plugin.mysql.utils.MysqlClient;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.sql.SQLException;


/**
 * MySQL test hooks.
 */
public class MysqlTestSetUpHooks {

  @Before(order = 1)
  public static void setTableName() {
    String randomString = RandomStringUtils.randomAlphabetic(10);
    String sourceTableName = String.format("SourceTable_%s", randomString);
    PluginPropertyUtils.addPluginProp("sourceMySqlTable", sourceTableName);
    System.out.println("Inside" + PluginPropertyUtils.pluginProp("sourceMySqlTable"));
}

  @Before(order = 1)
  public static void initializeDBProperties() {
    String username = System.getenv("mysqlUsername");
    if (username != null && !username.isEmpty()) {
      PluginPropertyUtils.addPluginProp("mysqlUsername", username);
    }
    String password = System.getenv("mysqlPassword");
    if (password != null && !password.isEmpty()) {
      PluginPropertyUtils.addPluginProp("mysqlPassword", password);
    }
    MysqlTestSetUpHooks.setTableName();
  }

//  public static void main(String[] args) throws SQLException, ClassNotFoundException {
//    setTableName();
////    initializeDBProperties();
//    createTable();
//  }

    @Before(order = 1, value = "@MYSQL_SOURCE")
    public static void createTable() throws SQLException, ClassNotFoundException {
      System.out.println("Error messaage" + PluginPropertyUtils.pluginProp("sourceMySqlTable"));
        MysqlClient.createTable(PluginPropertyUtils.pluginProp("sourceMySqlTable"),
                                PluginPropertyUtils.pluginProp("mysqlDatatypeColumns"));
      System.out.println(PluginPropertyUtils.pluginProp("sourceMySqlTable"));
//      MysqlClient.insertRow(PluginPropertyUtils.pluginProp("sourceMySqlTable"),
//                            PluginPropertyUtils.pluginProp("mysqlDatatypeValuesRow1"));
//      MysqlClient.insertRow(PluginPropertyUtils.pluginProp("sourceMySqlTable"),
//                            PluginPropertyUtils.pluginProp("mysqlDatatypeValuesRow2"));
//      MysqlActions.selectTable(PluginPropertyUtils.pluginProp("sourceMySqlTable"));
    }

    @Before(order = 2, value = "@MYSQL_SOURCE2")
    public static void insertRow() throws SQLException, ClassNotFoundException {
      System.out.println("Error messaage2" + PluginPropertyUtils.pluginProp("sourceMySqlTable"));
        MysqlClient.insertRow(PluginPropertyUtils.pluginProp("sourceMySqlTable"),
                              PluginPropertyUtils.pluginProp("mysqlDatatypeValuesRow1"));
        MysqlClient.insertRow(PluginPropertyUtils.pluginProp("sourceMySqlTable"),
                              PluginPropertyUtils.pluginProp("mysqlDatatypeValuesRow2"));
    }

  @After(order = 1, value = "@MYSQL_DELETE")
  public static void dropTable() throws SQLException, ClassNotFoundException {
        MysqlClient.deleteTable(new String[] {PluginPropertyUtils.pluginProp("sourceMySqlTable")});
  }

  @After(order = 1, value = "@BQ_SINK_TEST")
  public static void deleteTempTargetBQTable() throws IOException, InterruptedException {
    String bqTargetTableName = PluginPropertyUtils.pluginProp("sourceMySqlTable");
    try {
      BigQueryClient.dropBqQuery(bqTargetTableName);
      BeforeActions.scenario.write("BQ Target table - " + bqTargetTableName + " deleted successfully");
      PluginPropertyUtils.removePluginProp("sourceMySqlTable");
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Not found: Table")) {
        BeforeActions.scenario.write("BQ Target Table " + bqTargetTableName + " does not exist");
      } else {
        Assert.fail(e.getMessage());
      }
    }
  }
}
