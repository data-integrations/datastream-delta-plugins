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

package io.cdap.plugin.common.actions;

import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.common.locators.Locators;
import io.cdap.plugin.utils.BigQuery;
import org.junit.Assert;

import java.util.concurrent.TimeUnit;

/**
 * Replication Common Actions.
 */
public class CommonReplicationActions {

  static {
    SeleniumHelper.getPropertiesLocators(Locators.class);
  }

  public static void waitTillPipelineIsRunningAndCheckForErrors() throws InterruptedException {
    //wait for datastream to startup
    int defaultTimeout = Integer.parseInt(PluginPropertyUtils.pluginProp("pipeline-initialization"));
    TimeUnit.SECONDS.sleep(defaultTimeout);
    BigQuery.waitForFlush();
    // Checking if an error message is displayed.
    Assert.assertFalse(ElementHelper.isElementDisplayed(Locators.error));
  }

  public static void waitForReplication() throws InterruptedException {
     BigQuery.waitForFlush();
  }
}
