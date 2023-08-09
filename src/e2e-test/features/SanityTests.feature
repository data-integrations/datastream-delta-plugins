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
Feature: Oracle - Verify Oracle source data transfer to Big Query
  @ORACLE_SOURCE_TEST @ORACLE_DELETE_TEST @BIGQUERY_DELETE @REQUIRED
  Scenario: To verify replication of snapshot and cdc data from Oracle to Big Query successfully with Sanity test
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Oracle" from the replication plugins list
    Then Select radio button plugin property: "connectivityMethod" with value: "private-connectivity"
    Then Enter input plugin property: "privateConnectionName" with value: "privateConnectionName"
    Then Replace input plugin property: "host" with value: "oracleHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "oraclePort" for Credentials and Authorization related fields
    Then Click plugin property: "region"
    Then Click plugin property: "regionOption"
    Then Replace input plugin property: "user" with value: "oracleUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "oraclePassword" for Credentials and Authorization related fields
    Then Replace input plugin property: "sid" with value: "oracleDatabaseName"
    Then Replace input plugin property: "project" with value: "oracleProjectId" for Credentials and Authorization related fields
    Then Click on the "Next" button in replication to navigate
    Then Replace input plugin property: "loadInterval" with value: "loadInterval"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetName" with value: "dataset"
    Then Click on the "Next" button in replication to navigate
    Then Validate Source table is available and select it
    And Click on the "Next" button in replication to navigate
    Then Wait till the Configure Advanced Properties page is loaded in replication
    And Click on the "Next" button in replication to navigate
    Then Wait till the Review Assessment page is loaded in replication
    And Click on the "Next" button in replication to navigate
    Then Deploy the replication pipeline
    And Run the replication Pipeline
    Then Open the Advanced logs
    And Wait till pipeline is in running state and check if no errors occurred
    Then Verify expected Oracle records in target BigQuery table
    And Run insert, update and delete CDC events on source table
    And Wait till CDC events are reflected in BQ
    Then Verify expected Oracle records in target BigQuery table
    And Open and capture logs
    Then Close the replication pipeline logs and stop the pipeline

  @ORACLE_SOURCE_TEST @ORACLE_DELETE_TEST @BIGQUERY_DELETE @REQUIRED
  Scenario: To verify replication of snapshot from Oracle to BigQuery successfully
    Given Open DataFusion Project with replication to configure replication job
    When Enter input plugin property: "name" with pipelineName
    And Click on the "Next" button in replication to navigate
    And Select Source plugin: "Oracle" from the replication plugins list
    Then Select radio button plugin property: "connectivityMethod" with value: "private-connectivity"
    Then Enter input plugin property: "privateConnectionName" with value: "privateConnectionName"
    Then Replace input plugin property: "host" with value: "oracleHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "oraclePort" for Credentials and Authorization related fields
    Then Click plugin property: "region"
    Then Click plugin property: "regionOption"
    Then Replace input plugin property: "user" with value: "oracleUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "oraclePassword" for Credentials and Authorization related fields
    Then Replace input plugin property: "sid" with value: "oracleDatabaseName"
    Then Replace input plugin property: "project" with value: "oracleProjectId" for Credentials and Authorization related fields
    And Click on the "Next" button in replication to navigate
    Then Replace input plugin property: "loadInterval" with value: "loadInterval"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetName" with value: "dataset"
    And Click on the "Next" button in replication to navigate
    Then Validate Source table is available and select it
    Then Click on the "Next" button in replication to navigate
    Then Wait till the Configure Advanced Properties page is loaded in replication
    Then Click on the "Next" button in replication to navigate
    Then Wait till the Review Assessment page is loaded in replication
    Then Click on the "Next" button in replication to navigate
    Then Deploy the replication pipeline
    And Run the replication Pipeline
    Then Open the Advanced logs
    And Wait till pipeline is in running state and check if no errors occurred
    Then Verify expected Oracle records in target BigQuery table
    And Open and capture logs
    Then Close the replication pipeline logs and stop the pipeline
