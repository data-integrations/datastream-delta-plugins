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

import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.locators.CdfPipelineRunLocators;
import io.cdap.e2e.utils.AssertionHelper;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PageHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.WaitHelper;
import io.cdap.plugin.mysql.locators.MysqlLocators;
import io.cdap.plugin.mysql.utils.BQClient;
import io.cdap.plugin.mysql.utils.Helper;
import io.cdap.plugin.mysql.utils.MysqlClient;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Replication MySQL Actions.
 */
public class MysqlActions {
    private static String parentWindow = StringUtils.EMPTY;
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
    public static void clickButton(String button) throws InterruptedException {
        TimeUnit.SECONDS.sleep(1);
        ElementHelper.clickOnElement(MysqlLocators.locateButton(button));
    }

    public static void selectSourcePlugin(String pluginName) {
        ElementHelper.clickOnElement(MysqlLocators.locateSourcePluginNameInList(pluginName));
    }

    public static void selectTable(String sourceTable) {
        sourceTable = PluginPropertyUtils.pluginProp("sourceMySqlTable");
        WaitHelper.waitForElementToBeDisplayed(MysqlLocators.selectTable(sourceTable), 1000);
        AssertionHelper.verifyElementDisplayed(MysqlLocators.selectTable(sourceTable));
        ElementHelper.clickOnElement(MysqlLocators.selectTable(sourceTable));
    }

    public static void verifyErrorMessage(String errorMessage) {
        String expectedErrorMessage = PluginPropertyUtils.errorProp(errorMessage);
        WaitHelper.waitForElementToBeDisplayed(MysqlLocators.rowError);
        AssertionHelper.verifyElementDisplayed(MysqlLocators.rowError);
        AssertionHelper.verifyElementContainsText(MysqlLocators.rowError,expectedErrorMessage);
    }

    public static void deployPipeline() {
        ElementHelper.clickOnElement(MysqlLocators.deployPipeline);
    }

    public static void startPipeline() {
        ElementHelper.clickIfDisplayed(MysqlLocators.start, ConstantsUtil.DEFAULT_TIMEOUT_SECONDS);
    }

    public static void runThePipeline() {
        startPipeline();
        WaitHelper.waitForElementToBeDisplayed(MysqlLocators.running);
    }

    public static void openAdvanceLogs() {
        MysqlLocators.logs.click();
        parentWindow = SeleniumDriver.getDriver().getWindowHandle();
        ArrayList<String> tabs = new ArrayList(SeleniumDriver.getDriver().getWindowHandles());
        SeleniumDriver.getDriver().switchTo().window(tabs.get(tabs.indexOf(parentWindow) + 1));
        MysqlLocators.advancedLogs.click();
    }

    public static void captureRawLog() {
        try {
            String rawLogs = getRawLogs();
            String logsSeparatorMessage = ConstantsUtil.LOGS_SEPARATOR_MESSAGE
                    .replace("MESSAGE", "DEPLOYED PIPELINE RUNTIME LOGS");
            BeforeActions.scenario.write(rawLogs);
            CdfPipelineRunAction.writeRawLogsToFile(BeforeActions.file, logsSeparatorMessage, rawLogs);
        } catch (Exception e) {
            BeforeActions.scenario.write("Exception in capturing logs : " + e);
        }
    }

    public static String getRawLogs() {
        CdfPipelineRunAction.viewRawLogs();
        ArrayList<String> tabs = new ArrayList(SeleniumDriver.getDriver().getWindowHandles());
        PageHelper.switchToWindow(tabs.indexOf(parentWindow) + 2);
        String logs = CdfPipelineRunLocators.logsTextbox.getText();
        Assert.assertNotNull(logs);
        PageHelper.closeCurrentWindow();
        return logs;
    }

    public static void waitTillPipelineIsRunningAndCheckForErrors() throws InterruptedException {
        //wait for datastream to startup
        int defaultTimeout = Integer.parseInt(PluginPropertyUtils.pluginProp("pipeline-initialization"));
        TimeUnit.SECONDS.sleep(defaultTimeout);
        BQClient.waitForFlush();
        // Checking if an error message is displayed.
        Assert.assertFalse(ElementHelper.isElementDisplayed(MysqlLocators.error));
    }

    public static void closeTheLogsAndClickOnStopButton() {
        SeleniumDriver.getDriver().switchTo().window(parentWindow);
        //Stopping the pipeline
        ElementHelper.clickOnElement(MysqlLocators.stop);
    }

    public static void executeCdcEventsOnSourceTable()
            throws SQLException, ClassNotFoundException {
        MysqlClient.insertRow(tableName, datatypeValues);
        MysqlClient.updateRow(tableName, updateCondition, updatedValue);
        MysqlClient.deleteRow(tableName, deleteCondition);
//        MysqlClient.forceFlushCDC();
    }

    public static void waitForReplication() throws InterruptedException {
        BQClient.waitForFlush();
    }

    public static void enterSearchTableValue(String value) throws IOException {
        ElementHelper.sendKeys(MysqlLocators.searchTable, PluginPropertyUtils.pluginProp("sourceMySqlTable"));
        ElementHelper.clickOnElement(MysqlLocators.selectTable(value));
    }
}
