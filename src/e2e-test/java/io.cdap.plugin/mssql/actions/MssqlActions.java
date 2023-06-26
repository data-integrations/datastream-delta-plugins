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

package io.cdap.plugin.mssql.actions;

import io.cdap.e2e.utils.AssertionHelper;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.WaitHelper;
import io.cdap.plugin.mssql.locators.MssqlLocators;
import io.cdap.plugin.utils.BigQuery;
import io.cdap.plugin.mssql.utils.MssqlClient;
import org.junit.Assert;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Replication Mssql Actions.
 */
public class MssqlActions {
    public static String tableName = PluginPropertyUtils.pluginProp("mssqlSourceTable");
    public static String schemaName = PluginPropertyUtils.pluginProp("mssqlSchema");
    public static String datatypeValues = PluginPropertyUtils.pluginProp("mssqlDatatypeValuesForInsertOperation");
    public static String deleteCondition = PluginPropertyUtils.pluginProp("mssqlDeleteRowCondition");
    public static String updateCondition = PluginPropertyUtils.pluginProp("mssqlUpdateRowCondition");
    public static String updatedValue = PluginPropertyUtils.pluginProp("mssqlUpdatedRow");

    static {
        SeleniumHelper.getPropertiesLocators(MssqlLocators.class);
    }

    public static void selectTable() {
        String table = tableName;
        WaitHelper.waitForElementToBeDisplayed(MssqlLocators.selectTable(table), 300);
        AssertionHelper.verifyElementDisplayed(MssqlLocators.selectTable(table));
        ElementHelper.clickOnElement(MssqlLocators.selectTable(table));
    }

    public static void waitTillPipelineIsRunningAndCheckForErrors() throws InterruptedException {
        //wait for datastream to startup
        int defaultTimeout = Integer.parseInt(PluginPropertyUtils.pluginProp("pipeline-initialization"));
        TimeUnit.SECONDS.sleep(defaultTimeout);
        BigQuery.waitForFlush();
        // Checking if an error message is displayed.
        Assert.assertFalse(ElementHelper.isElementDisplayed(MssqlLocators.error));
    }

    public static void executeCdcEventsOnSourceTable()
            throws SQLException, ClassNotFoundException {
        MssqlClient.insertRow(tableName, schemaName, datatypeValues);
        MssqlClient.updateRow(tableName, schemaName, updateCondition, updatedValue);
        MssqlClient.deleteRow(tableName, schemaName, deleteCondition);
    }

    public static void waitForReplication() throws InterruptedException {
        BigQuery.waitForFlush();
    }
}
