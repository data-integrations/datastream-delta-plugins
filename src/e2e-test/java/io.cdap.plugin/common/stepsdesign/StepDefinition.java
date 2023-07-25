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

package io.cdap.plugin.common.stepsdesign;

import io.cdap.e2e.utils.CdfHelper;
import io.cdap.plugin.common.actions.CommonReplicationActions;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;

/**
 * Contains common replication test scenarios step definitions.
 */
public class StepDefinition implements CdfHelper {

  @Then("Wait till pipeline is in running state and check if no errors occurred")
  public void waitTillPipelineIsInRunningState() throws InterruptedException {
    CommonReplicationActions.waitTillPipelineIsRunningAndCheckForErrors();
  }

  @And("Wait till CDC events are reflected in BQ")
  public void waitForReplicationToFlushEvents() throws InterruptedException {
    CommonReplicationActions.waitForReplication();
  }
}
