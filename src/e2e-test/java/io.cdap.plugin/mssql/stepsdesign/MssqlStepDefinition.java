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

package io.cdap.plugin.mssql.stepsdesign;

import io.cdap.e2e.utils.CdfHelper;
import io.cdap.plugin.mssql.actions.MssqlActions;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Contains Mssql replication test scenarios step definitions.
 */
public class MssqlStepDefinition implements CdfHelper {

  @Then("Validate Source table is available and select it")
  public void selectTable() {
    MssqlActions.selectTable();
  }

  @Then("Wait till pipeline is in running state and check if no errors occurred")
  public void waitTillPipelineIsInRunningState() throws InterruptedException {
    MssqlActions.waitTillPipelineIsRunningAndCheckForErrors();
  }

  @And("Run insert, update and delete CDC events on source table")
  public void executeCdcEvents() throws SQLException, ClassNotFoundException {
    MssqlActions.executeCdcEventsOnSourceTable();
  }

  @And("Wait till CDC events are reflected in BQ")
  public void waitForReplicationToFlushEvents() throws InterruptedException {
    MssqlActions.waitForReplication();
  }

  @Then("Verify expected MsSQL records in target BigQuery table")
  public void verifyExpectedMsSQLRecordsInTargetBigQueryTable() throws
    IOException, InterruptedException, SQLException, ClassNotFoundException {
//    MssqlActions.verifyTargetBigQueryRecordMatchesExpectedMssqlRecord();
  }
}
