/*
 * Copyright (c) 2023.
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

package io.cdap.plugin.actions;

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.utils.BigQuery;
import io.cdap.plugin.utils.OracleClient;
import io.cdap.plugin.utils.ValidationHelper;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Replication oracle Actions.
 */
public class ReplicationActions {

    private static final String projectId = PluginPropertyUtils.pluginProp("projectId");
    private static final String database = PluginPropertyUtils.pluginProp("dataset");
    public static String tableName = PluginPropertyUtils.pluginProp("sourceTable");
    public static String schemaName = PluginPropertyUtils.pluginProp("schema");
    public static String datatypeValues = PluginPropertyUtils.pluginProp("datatypeValuesForInsertOperation");
    public static String deleteCondition = PluginPropertyUtils.pluginProp("deleteRowCondition");
    public static String updateCondition = PluginPropertyUtils.pluginProp("updateRowCondition");
    public static String updatedValue = PluginPropertyUtils.pluginProp("updatedRow");

    public static void selectTable() {
        String table = schemaName + "." + PluginPropertyUtils.pluginProp("sourceTable");
        ElementHelper.clickOnElement(CdfPluginPropertiesLocators.selectReplicationTable(table), 180);
    }

    public static void waitTillPipelineIsRunningAndCheckForErrors() throws InterruptedException {
        //wait for datastream to startup
        int defaultTimeout = Integer.parseInt(PluginPropertyUtils.pluginProp("pipelineInitialization"));
        TimeUnit.SECONDS.sleep(defaultTimeout);
        BigQuery.waitForFlush();
    }

    public static void verifyTargetBigQueryRecordMatchesExpectedOracleRecord()
            throws IOException, InterruptedException, SQLException, ClassNotFoundException {
        List<Map<String, Object>> sourceOracleRecords = OracleClient.getOracleRecordsAsMap(
                PluginPropertyUtils.pluginProp("sourceTable"), schemaName);
        List<Map<String, Object>> targetBigQueryRecords =
                BigQuery.getBigQueryRecordsAsMap(projectId, database,
                PluginPropertyUtils.pluginProp("sourceTable"));
        ValidationHelper.validateRecords(sourceOracleRecords, targetBigQueryRecords);
    }

    public static void executeCdcEventsOnSourceTable()
            throws SQLException, ClassNotFoundException {
        OracleClient.insertRow(tableName, schemaName, datatypeValues);
        OracleClient.updateRow(tableName, schemaName, updateCondition, updatedValue);
        OracleClient.deleteRow(tableName, schemaName, deleteCondition);
        OracleClient.forceFlushCDC();
    }

    public static void waitForReplication() throws InterruptedException {
        BigQuery.waitForFlush();
    }
}
