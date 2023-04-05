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

package io.cdap.plugin.hooks;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.utils.OracleClient;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import stepsdesign.BeforeActions;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Oracle test hooks.
 */
public class TestSetUpHooks {
    public static List<Map<String, Object>> sourceOracleRecords = new ArrayList<>();
    public static String tableName = PluginPropertyUtils.pluginProp("sourceTable");
    public static String schemaName = PluginPropertyUtils.pluginProp("schema");
    public static String primaryKey = PluginPropertyUtils.pluginProp("primaryKey");
    public static String datatypeColumns = PluginPropertyUtils.pluginProp("datatypeColumns");
    public static String datatypeColumnNames = PluginPropertyUtils.pluginProp("datatypeColumnNames");
    public static String row1 = PluginPropertyUtils.pluginProp("datatypeValuesRow1");
    public static String row2= PluginPropertyUtils.pluginProp("datatypeValuesRow2");


    @Before(order = 1, value = "@ORACLE_SOURCE")
    public static void overridePropertiesFromEnvVarsIfProvided() {
        String username = System.getenv("ORACLE_USERNAME");
        if (username != null && !username.isEmpty()) {
            PluginPropertyUtils.addPluginProp("username", username);
        }
        String password = System.getenv("ORACLE_PASSWORD");
        if (password != null && !password.isEmpty()) {
            PluginPropertyUtils.addPluginProp("password", password);
        }
        String port = System.getenv("ORACLE_PORT");
        if (port!= null && !port.isEmpty()) {
            PluginPropertyUtils.addPluginProp("port", port);
        }
        String sapHost = System.getenv("ORACLE_HOST");
        if (sapHost != null && !sapHost.isEmpty()) {
            PluginPropertyUtils.addPluginProp("host", sapHost);
        }
    }

    @Before(order = 2, value = "@ORACLE_SOURCE")
    public static void createTable() throws SQLException, ClassNotFoundException {
        OracleClient.createTable(tableName,schemaName,datatypeColumns, primaryKey);
    }

    @Before(order = 3, value = "@ORACLE_SOURCE")
    public static void insertRow() throws SQLException, ClassNotFoundException {
        OracleClient.insertRow(tableName, schemaName, datatypeColumnNames, row1);
        OracleClient.insertRow(tableName, schemaName, datatypeColumnNames, row2);
        sourceOracleRecords = OracleClient.getOracleRecordsAsMap(tableName, schemaName);
    }

    @Before(order = 4, value = "@ORACLE_SOURCE")
    public static void getOracleRecordsAsMap() throws SQLException, ClassNotFoundException {
        sourceOracleRecords = OracleClient.getOracleRecordsAsMap(tableName, schemaName);
        BeforeActions.scenario.write("Expected Oracle records : " + sourceOracleRecords);
    }

  @After(order = 1, value = "@ORACLE_SOURCE_TEMP")
  public static void dropTables() throws SQLException, ClassNotFoundException {
    OracleClient.deleteTables(schemaName, tableName);
  }
}