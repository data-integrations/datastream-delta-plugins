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

package io.cdap.plugin.stepsdesign;

import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.plugin.actions.ReplicationActions;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;

import java.io.IOException;
import java.sql.SQLException;

public class StepDefinition implements CdfHelper {
    @Given("Open DataFusion Project with replication to configure pipeline")
    public void openDataFusionProjectWithReplicationToConfigurePipeline() throws IOException, InterruptedException {
        openCdf();
        SeleniumDriver.getDriver().get(SeleniumDriver.getDriver().getCurrentUrl().replace(
                PluginPropertyUtils.pluginProp("cdfUrl"), PluginPropertyUtils.pluginProp("replication.url")));
    }
    @And("Click on the Next button")
    public void clickOnTheNextButton() throws InterruptedException {
        ReplicationActions.clickNextButton();
    }
    @And("Select Oracle as Source")
    public void selectSourceAsOracle() {
        ReplicationActions.clickOnOraclePlugin();
    }

    @Then("Validate Source table is available and select it")
    public void selectTable() {
        ReplicationActions.selectTable();
    }
    @Then("Deploy the replication pipeline")
    public void deployPipeline() {
        ReplicationActions.deployPipeline();
    }

    @Then("Run the replication Pipeline")
    public void runPipelineInRuntime() {
        ReplicationActions.runThePipeline();
    }

    @Then("Open the logs")
    public void openLogsOfSltPipeline() {
        ReplicationActions.openAdvanceLogs();
    }

    @Then("Wait till pipeline is in running state and check if no errors occurred")
    public void waitTillSltPipelineIsInRunningState() throws InterruptedException {
        ReplicationActions.waitTillPipelineIsRunningAndCheckForErrors();
    }

    @Then("Close the pipeline logs and stop the pipeline")
    public void closeLogsAndStopThePipeline() {
        ReplicationActions.closeTheLogsAndClickOnStopButton();
    }

    @And("Capture raw logs")
    public void captureRawLogs() {
        ReplicationActions.captureRawLog();
    }

    @And("Insert a record in the source table and wait for replication")
    public void triggerInsertCdcEvent() throws IOException, InterruptedException, SQLException, ClassNotFoundException {
        ReplicationActions.insertRecordAndWait(); //JCoException,
    }

    @And("Delete a record in the source table and wait for replication")
    public void triggerDeleteCdcEvent() throws IOException, InterruptedException, SQLException, ClassNotFoundException {
        ReplicationActions.deleteRecordAndWait(); //JCoException,
    }

    @And("Update a record in the source table and wait for replication")
    public void triggerUpdateCdcEvent() throws IOException, InterruptedException, SQLException, ClassNotFoundException {
        ReplicationActions.updateRecordAndWait(); //JCoException,
    }
    @Then("Verify expected Oracle records in target BigQuery table")
    public void verifyExpectedOracleRecordsInTargetBigQueryTable() throws IOException, InterruptedException, SQLException, ClassNotFoundException {
        ReplicationActions.verifyTargetBigQueryRecordMatchesExpectedOracleRecord();
    }

}
