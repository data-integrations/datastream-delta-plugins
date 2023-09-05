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

@Oracle
Feature: Oracle - Verify Oracle source plugin design time validation scenarios

  @REQUIRED
  Scenario: To verify validation message when user provides invalid Host
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Oracle" from the replication plugins list
    Then Select radio button plugin property: "connectivityMethod" with value: "private-connectivity"
    Then Enter input plugin property: "privateConnectionName" with value: "privateConnectionName"
    Then Replace input plugin property: "host" with value: "oracleInvalidHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "oraclePort" for Credentials and Authorization related fields
    Then Replace input plugin property: "sid" with value: "oracleDatabaseName"
    Then Replace input plugin property: "user" with value: "oracleUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "oraclePassword" for Credentials and Authorization related fields
    And Click on the "Next" button in replication to navigate
    Then Enter input plugin property: "datasetName" with value: "dataset"
    Then Replace input plugin property: "project" with value: "projectId"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidHost"

  Scenario: To verify validation message when user provides invalid Port
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Oracle" from the replication plugins list
    Then Select radio button plugin property: "connectivityMethod" with value: "private-connectivity"
    Then Enter input plugin property: "privateConnectionName" with value: "privateConnectionName"
    Then Replace input plugin property: "host" with value: "oracleHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "oracleInvalidPort" for Credentials and Authorization related fields
    Then Replace input plugin property: "sid" with value: "oracleDatabaseName"
    Then Replace input plugin property: "user" with value: "oracleUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "oraclePassword" for Credentials and Authorization related fields
    And Click on the "Next" button in replication to navigate
    Then Enter input plugin property: "datasetName" with value: "dataset"
    Then Replace input plugin property: "project" with value: "projectId"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidPort"

  @REQUIRED
  Scenario: To verify validation message when user provides invalid Database Name
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Oracle" from the replication plugins list
    Then Select radio button plugin property: "connectivityMethod" with value: "private-connectivity"
    Then Enter input plugin property: "privateConnectionName" with value: "privateConnectionName"
    Then Replace input plugin property: "host" with value: "oracleHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "oraclePort" for Credentials and Authorization related fields
    Then Replace input plugin property: "sid" with value: "oracleInvalidDatabaseName"
    Then Replace input plugin property: "user" with value: "oracleUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "oraclePassword" for Credentials and Authorization related fields
    And Click on the "Next" button in replication to navigate
    Then Enter input plugin property: "datasetName" with value: "dataset"
    Then Replace input plugin property: "project" with value: "projectId"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidDatabase"

  @REQUIRED
  Scenario: To verify validation message when user provides invalid user
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Oracle" from the replication plugins list
    Then Select radio button plugin property: "connectivityMethod" with value: "private-connectivity"
    Then Enter input plugin property: "privateConnectionName" with value: "privateConnectionName"
    Then Replace input plugin property: "host" with value: "oracleHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "oraclePort" for Credentials and Authorization related fields
    Then Replace input plugin property: "sid" with value: "oracleDatabaseName"
    Then Replace input plugin property: "user" with value: "oracleInvalidUser" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "oraclePassword" for Credentials and Authorization related fields
    And Click on the "Next" button in replication to navigate
    Then Enter input plugin property: "datasetName" with value: "dataset"
    Then Replace input plugin property: "project" with value: "projectId"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidUser"

  Scenario: To verify validation message when user provides invalid password
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Oracle" from the replication plugins list
    Then Select radio button plugin property: "connectivityMethod" with value: "private-connectivity"
    Then Enter input plugin property: "privateConnectionName" with value: "privateConnectionName"
    Then Replace input plugin property: "host" with value: "oracleHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "oraclePort" for Credentials and Authorization related fields
    Then Replace input plugin property: "sid" with value: "oracleDatabaseName"
    Then Replace input plugin property: "user" with value: "oracleInvalidUser" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "oracleInvalidPassword" for Credentials and Authorization related fields
    And Click on the "Next" button in replication to navigate
    Then Enter input plugin property: "datasetName" with value: "dataset"
    Then Replace input plugin property: "project" with value: "projectId"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageInvalidPassword"

  Scenario: To verify validation message when macro enabled for password field
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Oracle" from the replication plugins list
    Then Select radio button plugin property: "connectivityMethod" with value: "private-connectivity"
    Then Enter input plugin property: "privateConnectionName" with value: "privateConnectionName"
    Then Replace input plugin property: "host" with value: "oracleHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "oraclePort" for Credentials and Authorization related fields
    Then Replace input plugin property: "sid" with value: "oracleDatabaseName"
    Then Replace input plugin property: "user" with value: "oracleUsername" for Credentials and Authorization related fields
    Then Click on the Macro button of Property: "password" and set the value to: "oraclePassword"
    And Click on the "Next" button in replication to navigate
    Then Enter input plugin property: "datasetName" with value: "dataset"
    Then Replace input plugin property: "project" with value: "projectId"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageMacroPassword"

  Scenario: To verify validation message when macro enabled for service account key field
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Oracle" from the replication plugins list
    Then Select radio button plugin property: "connectivityMethod" with value: "private-connectivity"
    Then Enter input plugin property: "privateConnectionName" with value: "privateConnectionName"
    Then Replace input plugin property: "host" with value: "oracleHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "oraclePort" for Credentials and Authorization related fields
    Then Replace input plugin property: "sid" with value: "oracleDatabaseName"
    Then Replace input plugin property: "user" with value: "oracleUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "oraclePassword" for Credentials and Authorization related fields
    And Click on the "Next" button in replication to navigate
    Then Enter input plugin property: "datasetName" with value: "dataset"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Click on the Macro button of Property: "serviceAccountKey" and set the value to: "ServiceAccountKey"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageMacroServiceAccountKey"

  @REQUIRED
  Scenario: To verify validation message when user provides blank password
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Oracle" from the replication plugins list
    Then Select radio button plugin property: "connectivityMethod" with value: "private-connectivity"
    Then Enter input plugin property: "privateConnectionName" with value: "privateConnectionName"
    Then Replace input plugin property: "host" with value: "oracleHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "oraclePort" for Credentials and Authorization related fields
    Then Replace input plugin property: "sid" with value: "oracleDatabaseName"
    Then Replace input plugin property: "user" with value: "oracleUsername" for Credentials and Authorization related fields
    And Click on the "Next" button in replication to navigate
    Then Enter input plugin property: "datasetName" with value: "dataset"
    Then Replace input plugin property: "project" with value: "projectId"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageBlankPassword"

  Scenario: To verify validation message when user provides blank username
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Oracle" from the replication plugins list
    Then Select radio button plugin property: "connectivityMethod" with value: "private-connectivity"
    Then Enter input plugin property: "privateConnectionName" with value: "privateConnectionName"
    Then Replace input plugin property: "host" with value: "oracleHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "oraclePort" for Credentials and Authorization related fields
    Then Replace input plugin property: "sid" with value: "oracleDatabaseName"
    Then Replace input plugin property: "password" with value: "oraclePassword" for Credentials and Authorization related fields
    And Click on the "Next" button in replication to navigate
    Then Enter input plugin property: "datasetName" with value: "dataset"
    Then Replace input plugin property: "project" with value: "projectId"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageBlankUserName"

  Scenario: To verify validation message when user provides blank host
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Oracle" from the replication plugins list
    Then Select radio button plugin property: "connectivityMethod" with value: "private-connectivity"
    Then Enter input plugin property: "privateConnectionName" with value: "privateConnectionName"
    Then Replace input plugin property: "port" with value: "oraclePort" for Credentials and Authorization related fields
    Then Replace input plugin property: "sid" with value: "oracleDatabaseName"
    Then Replace input plugin property: "user" with value: "oracleUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "oraclePassword" for Credentials and Authorization related fields
    And Click on the "Next" button in replication to navigate
    Then Enter input plugin property: "datasetName" with value: "dataset"
    Then Replace input plugin property: "project" with value: "projectId"
    And Click on the "Next" button in replication to navigate
    Then Verify that the Plugin is displaying an error message: "errorMessageBlankHost"
