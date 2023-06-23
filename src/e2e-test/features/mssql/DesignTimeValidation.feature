#
# Copyright © 2023 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

@Mssql
Feature: Mssql - Verify SqlServer source plugin design time validation scenarios

  Scenario: To verify validation message when user provides invalid Host
    Given Open DataFusion Project with replication to configure pipeline
    When Enter input plugin property: "name" with value: "pipelineName"
    And Click on the button "Next"
    And Select Source plugin: "Microsoft SQLServer" from the replication plugins list
    Then Replace input plugin property: "host" with value: "mssqlInvalidHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "mssqlPort" for Credentials and Authorization related fields
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "mssqlDriverName"
    Then Replace input plugin property: "database" with value: "mssqlDatabaseName"
    Then Replace input plugin property: "user" with value: "mssqlUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "mssqlPassword" for Credentials and Authorization related fields
    And Click on the button "Next"
    Then Replace input plugin property: "project" with value: "bqProjectId"
    Then Enter input plugin property: "datasetName" with value: "bqDataset"
    And Click on the button "Next"
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidHost"

  Scenario: To verify validation message when user provides invalid Port
    Given Open DataFusion Project with replication to configure pipeline
    When Enter input plugin property: "name" with value: "pipelineName"
    And Click on the button "Next"
    And Select Source plugin: "Microsoft SQLServer" from the replication plugins list
    Then Replace input plugin property: "host" with value: "mssqlHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "mssqlInvalidPort" for Credentials and Authorization related fields
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "mssqlDriverName"
    Then Replace input plugin property: "database" with value: "mssqlDatabaseName"
    Then Replace input plugin property: "user" with value: "mssqlUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "mssqlPassword" for Credentials and Authorization related fields
    And Click on the button "Next"
    Then Replace input plugin property: "project" with value: "bqProjectId"
    Then Enter input plugin property: "datasetName" with value: "bqDataset"
    And Click on the button "Next"
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidPort"

  Scenario: To verify validation message when user provides invalid Database Name
    Given Open DataFusion Project with replication to configure pipeline
    When Enter input plugin property: "name" with value: "pipelineName"
    And Click on the button "Next"
    And Select Source plugin: "Microsoft SQLServer" from the replication plugins list
    Then Replace input plugin property: "host" with value: "mssqlHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "mssqlPort" for Credentials and Authorization related fields
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "mssqlDriverName"
    Then Replace input plugin property: "database" with value: "mssqlInvalidDatabaseName"
    Then Replace input plugin property: "user" with value: "mssqlUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "mssqlPassword" for Credentials and Authorization related fields
    And Click on the button "Next"
    Then Replace input plugin property: "project" with value: "bqProjectId"
    Then Enter input plugin property: "datasetName" with value: "bqDataset"
    And Click on the button "Next"
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidDatabase"

  Scenario: To verify validation message when user provides invalid user
    Given Open DataFusion Project with replication to configure pipeline
    When Enter input plugin property: "name" with value: "pipelineName"
    And Click on the button "Next"
    And Select Source plugin: "Microsoft SQLServer" from the replication plugins list
    Then Replace input plugin property: "host" with value: "mssqlHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "mssqlPort" for Credentials and Authorization related fields
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "mssqlDriverName"
    Then Replace input plugin property: "database" with value: "mssqlDatabaseName"
    Then Replace input plugin property: "user" with value: "mssqlInvalidUser" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "mssqlPassword" for Credentials and Authorization related fields
    And Click on the button "Next"
    Then Replace input plugin property: "project" with value: "bqProjectId"
    Then Enter input plugin property: "datasetName" with value: "bqDataset"
    And Click on the button "Next"
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidUser"

  Scenario: To verify validation message when user provides invalid password
    Given Open DataFusion Project with replication to configure pipeline
    When Enter input plugin property: "name" with value: "pipelineName"
    And Click on the button "Next"
    And Select Source plugin: "Microsoft SQLServer" from the replication plugins list
    Then Replace input plugin property: "host" with value: "mssqlHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "mssqlPort" for Credentials and Authorization related fields
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "mssqlDriverName"
    Then Replace input plugin property: "database" with value: "mssqlDatabaseName"
    Then Replace input plugin property: "user" with value: "mssqlInvalidUser" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "mssqlInvalidPassword" for Credentials and Authorization related fields
    And Click on the button "Next"
    Then Replace input plugin property: "project" with value: "bqProjectId"
    Then Enter input plugin property: "datasetName" with value: "bqDataset"
    And Click on the button "Next"
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidPassword"
