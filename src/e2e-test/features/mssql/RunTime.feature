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
Feature: Mssql - Verify Mssql source data transfer to Big Query

  @MSSQL_SOURCE @MSSQL_DELETE @BQ_SINK_TEST
  Scenario: To verify replication of snapshot data from Mssql to BigQuery successfully
    Given Open DataFusion Project with replication to configure pipeline
    When Enter input plugin property: "name" with pipelineName
    And Click on the Next button in Replication mode "Next"
    And Select Source plugin: "Microsoft SQLServer" from the replication plugins list
    Then Replace input plugin property: "host" with value: "mssqlHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "mssqlPort" for Credentials and Authorization related fields
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "mssqlDriverName"
    Then Replace input plugin property: "database" with value: "mssqlDatabaseName"
    Then Replace input plugin property: "user" with value: "mssqlUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "mssqlPassword" for Credentials and Authorization related fields
    And Click on the Next button in Replication mode "Next"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetName" with value: "dataset"
    And Click on the Next button in Replication mode "Next"
    Then Validate Source table is available and select it
    And Click on the Next button in Replication mode "Next"
    And Click on the Next button in Replication mode "Next"
    Then Wait till the Review Assessment page is loaded in replication
    And Click on the Next button in Replication mode "Next"
    Then Deploy the replication pipeline
    And Run the replication Pipeline
    Then Open the Advanced logs
    And Wait till pipeline is in running state and check if no errors occurred
#    Then Verify expected MsSQL records in target BigQuery table
    And Open and capture logs
    Then Close the replication pipeline logs and stop the pipeline

  @MSSQL_SOURCE @MSSQL_DELETE @BQ_SINK_TEST
  Scenario: To verify replication of snapshot and cdc data from MsSQL to BigQuery successfully
    Given Open DataFusion Project with replication to configure pipeline
    When Enter input plugin property: "name" with pipelineName
    And Click on the Next button in Replication mode "Next"
    And Select Source plugin: "Microsoft SQLServer" from the replication plugins list
    Then Replace input plugin property: "host" with value: "mssqlHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "mssqlPort" for Credentials and Authorization related fields
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "mssqlDriverName"
    Then Replace input plugin property: "database" with value: "mssqlDatabaseName"
    Then Replace input plugin property: "user" with value: "mssqlUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "mssqlPassword" for Credentials and Authorization related fields
    And Click on the Next button in Replication mode "Next"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetName" with value: "dataset"
    And Click on the Next button in Replication mode "Next"
    Then Validate Source table is available and select it
    And Click on the Next button in Replication mode "Next"
    And Click on the Next button in Replication mode "Next"
    Then Wait till the Review Assessment page is loaded in replication
    And Click on the Next button in Replication mode "Next"
    Then Deploy the replication pipeline
    And Run the replication Pipeline
    Then Open the Advanced logs
    And Wait till pipeline is in running state and check if no errors occurred
#    Then Verify expected MsSQL records in target BigQuery table
    And Run insert, update and delete CDC events on source table
    And Wait till CDC events are reflected in BQ
#    Then Verify expected MsSQL records in target BigQuery table
    And Open and capture logs
    Then Close the replication pipeline logs and stop the pipeline
