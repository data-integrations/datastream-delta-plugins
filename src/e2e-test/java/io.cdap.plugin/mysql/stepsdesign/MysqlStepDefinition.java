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

package io.cdap.plugin.mysql.stepsdesign;

import io.cdap.e2e.pages.actions.CdfPluginPropertiesActions;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.plugin.mysql.actions.MysqlActions;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Contains MySQL replication test scenarios step definitions.
 */
public class MysqlStepDefinition implements CdfHelper {
    @Given("Open DataFusion Project with replication to configure pipeline")
    public void openDataFusionProjectWithReplicationToConfigurePipeline() throws IOException, InterruptedException {
        openCdf();
        SeleniumDriver.getDriver().get(SeleniumDriver.getDriver().getCurrentUrl().replace(
                PluginPropertyUtils.pluginProp("cdfUrl"), PluginPropertyUtils.pluginProp("replication.url")));
    }
    @And("Click on the button {string}")
    public void clickOnTheButton(String button) throws InterruptedException {
        MysqlActions.clickButton(button);
    }

    @When("Select Source plugin: {string} from the replication plugins list")
    public void selectSourcePlugin(String pluginName) {
        MysqlActions.selectSourcePlugin(pluginName);
    }

    @Then("Verify that the Plugin is displaying an error message: {string}")
    public void verifyErrorMessageIsDisplayed (String errorMessage) {
        MysqlActions.verifyErrorMessage(errorMessage);
    }

    @Then("Validate MySQL Source table is available and select it")
    public void selectSourceTable(String sourceTable) {
        MysqlActions.selectTable(sourceTable);
    }

    @Then("Enter source table in search panel to select")
    public void enterSourceTableToSelect() throws IOException {
        MysqlActions.enterSearchTableValue(PluginPropertyUtils.pluginProp("sourceMySqlTable"));
    }

    @Then("Deploy the replication pipeline")
    public void deployPipeline() {
        MysqlActions.deployPipeline();
    }

    @Then("Run the replication Pipeline")
    public void runPipelineInRuntime() {
        MysqlActions.runThePipeline();
    }

    @Then("Open the logs")
    public void openLogsOfSltPipeline() {
        MysqlActions.openAdvanceLogs();
    }

    @Then("Wait till pipeline is in running state and check if no errors occurred")
    public void waitTillPipelineIsInRunningState() throws InterruptedException {
        MysqlActions.waitTillPipelineIsRunningAndCheckForErrors();
    }

    @Then("Close the pipeline logs and stop the pipeline")
    public void closeLogsAndStopThePipeline() {
        MysqlActions.closeTheLogsAndClickOnStopButton();
    }

    @And("Capture raw logs")
    public void captureRawLogs() {
        MysqlActions.captureRawLog();
    }

    @And("Run insert, update and delete CDC events on source table")
    public void executeCdcEvents() throws SQLException, ClassNotFoundException {
        MysqlActions.executeCdcEventsOnSourceTable();
    }

    @And("Wait till CDC events are reflected in BQ")
    public void waitForReplicationToFlushEvents() throws InterruptedException {
        MysqlActions.waitForReplication();
    }

//    @Then("Verify expected MySQL records in target BigQuery table")
//    public void verifyExpectedOracleRecordsInTargetBigQueryTable() throws
//            IOException, InterruptedException, SQLException, ClassNotFoundException {
//        MysqlActions.verifyTargetBigQueryRecordMatchesExpectedOracleRecord();
//    }

}
