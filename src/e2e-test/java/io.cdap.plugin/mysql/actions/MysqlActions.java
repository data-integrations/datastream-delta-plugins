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

package io.cdap.plugin.mysql.actions;

import io.cdap.e2e.utils.AssertionHelper;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.WaitHelper;
import io.cdap.plugin.mysql.locators.MysqlLocators;
import io.cdap.plugin.mysql.utils.MysqlClient;
import io.cdap.plugin.utils.BigQuery;
import io.cdap.plugin.utils.ValidationHelper;
import org.junit.Assert;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Replication MySQL Actions.
 */
public class MysqlActions {
    private static final String projectId = PluginPropertyUtils.pluginProp("projectId");
    private static final String database = PluginPropertyUtils.pluginProp("dataset");
    public static String tableName = PluginPropertyUtils.pluginProp("sourceMySqlTable");
    public static String datatypeValues = PluginPropertyUtils.pluginProp("mysqlDatatypeValuesForInsertOperation");
    public static String deleteCondition = PluginPropertyUtils.pluginProp("deleteRowCondition");
    public static String updateCondition = PluginPropertyUtils.pluginProp("updateRowCondition");
    public static String updatedValue = PluginPropertyUtils.pluginProp("mysqlUpdatedRow");

    static {
        SeleniumHelper.getPropertiesLocators(MysqlLocators.class);
    }

    public static void selectTable() {
        String table = tableName;
        WaitHelper.waitForElementToBeDisplayed(MysqlLocators.selectTable(table), 300);
        AssertionHelper.verifyElementDisplayed(MysqlLocators.selectTable(table));
        ElementHelper.clickOnElement(MysqlLocators.selectTable(table));
    }

    public static void waitTillPipelineIsRunningAndCheckForErrors() throws InterruptedException {
        //wait for datastream to startup
        int defaultTimeout = Integer.parseInt(PluginPropertyUtils.pluginProp("pipeline-initialization"));
        TimeUnit.SECONDS.sleep(defaultTimeout);
        BigQuery.waitForFlush();
        // Checking if an error message is displayed.
        Assert.assertFalse(ElementHelper.isElementDisplayed(MysqlLocators.error));
    }

    public static void executeCdcEventsOnSourceTable()
            throws SQLException, ClassNotFoundException {
        MysqlClient.insertRow(tableName, datatypeValues);
        MysqlClient.updateRow(tableName, updateCondition, updatedValue);
        MysqlClient.deleteRow(tableName, deleteCondition);
    }

    public static void waitForReplication() throws InterruptedException {
        BigQuery.waitForFlush();
    }
}
