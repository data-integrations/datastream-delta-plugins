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

package io.cdap.plugin.oracle.hooks;

import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.oracle.utils.OracleClient;
import io.cdap.plugin.utils.BigQuery;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.lang3.RandomStringUtils;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Oracle test hooks.
 */
public class OracleTestSetUpHooks {
    public static String tableName = PluginPropertyUtils.pluginProp("oracleSourceTable");
    public static String schemaName = PluginPropertyUtils.pluginProp("oracleSchema");
    public static String datatypeColumns = PluginPropertyUtils.pluginProp("oracleDatatypeColumns");

    @Before(order = 1)
    public static void setTableName() {
        String randomString = RandomStringUtils.randomAlphabetic(10).toUpperCase();
        String sourceTableName = String.format("SourceTable_%s", randomString);
        PluginPropertyUtils.addPluginProp("oracleSourceTable", sourceTableName);
    }

    @Before(order = 2, value = "@ORACLE_SOURCE")
    public static void createTable() throws SQLException, ClassNotFoundException {
        OracleClient.createTable(tableName, schemaName, datatypeColumns);
        BeforeActions.scenario.write("Oracle Source Table - " + tableName + " created successfully");
    }

  @After(order = 1, value = "@ORACLE_DELETE")
  public static void dropTable() throws SQLException, ClassNotFoundException {
        OracleClient.deleteTable(schemaName, tableName);
        BeforeActions.scenario.write("Oracle Source Table - " + tableName + " deleted successfully");
  }

    @After(order = 1, value = "@BIGQUERY_DELETE")
    public static void deleteTargetBQTable() throws IOException, InterruptedException {
        BigQuery.deleteTable(tableName);
    }
}
