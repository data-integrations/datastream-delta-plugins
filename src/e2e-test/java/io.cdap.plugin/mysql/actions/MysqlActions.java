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
import io.cdap.plugin.common.locators.Locators;
import io.cdap.plugin.mysql.utils.MysqlClient;

import java.sql.SQLException;

/**
 * Replication MySQL Actions.
 */
public class MysqlActions {
  public static String tableName = PluginPropertyUtils.pluginProp("sourceMySqlTable");
  public static String datatypeValues = PluginPropertyUtils.pluginProp("mysqlValuesForInsertOperation");
  public static String deleteCondition = PluginPropertyUtils.pluginProp("deleteRowCondition");
  public static String updateCondition = PluginPropertyUtils.pluginProp("updateRowCondition");
  public static String updatedValue = PluginPropertyUtils.pluginProp("mysqlUpdatedRow");

  static {
    SeleniumHelper.getPropertiesLocators(Locators.class);
  }

  public static void selectTable() {
    String table = tableName;
    WaitHelper.waitForElementToBeDisplayed(Locators.selectTable(table), 300);
    AssertionHelper.verifyElementDisplayed(Locators.selectTable(table));
    ElementHelper.clickOnElement(Locators.selectTable(table));
  }

  public static void executeCdcEventsOnSourceTable() throws SQLException, ClassNotFoundException {
     MysqlClient.insertRow(tableName, datatypeValues);
     MysqlClient.updateRow(tableName, updateCondition, updatedValue);
     MysqlClient.deleteRow(tableName, deleteCondition);
    }
}
