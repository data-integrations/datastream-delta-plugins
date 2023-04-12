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
  @ENV_VARIABLES @ORACLE_SOURCE @ORACLE_DELETE @BIGQUERY_DELETE
  Scenario: To verify replication of snapshot and cdc data from Oracle to Big Query successfully with Sanity test
    Given Open DataFusion Project with replication to configure pipeline
    When Enter input plugin property: "name" with value: "pipelineName"
    And Click on the Next button
    And Select Oracle as Source
    Then Replace input plugin property: "host" with value: "host" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "port" for Credentials and Authorization related fields
    Then Click plugin property: "region"
    Then Click plugin property: "regionOption"
    Then Replace input plugin property: "user" with value: "username" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Replace input plugin property: "sid" with value: "dataset" for Credentials and Authorization related fields
    Then Click on the Next button
    Then Replace input plugin property: "loadInterval" with value: "loadInterval"
    Then Click on the Next button
    Then Validate Source table is available and select it
    And Click on the Next button
    And Click on the Next button
    And Click on the Next button
    Then Deploy the replication pipeline
    And Run the replication Pipeline
    Then Open the logs
    And Wait till pipeline is in running state and check if no errors occurred
    Then Verify expected Oracle records in target BigQuery table
    And Insert a record in the source table and wait for replication
    Then Verify expected Oracle records in target BigQuery table
    And Delete a record in the source table and wait for replication
    Then Verify expected Oracle records in target BigQuery table
    And Update a record in the source table and wait for replication
    Then Verify expected Oracle records in target BigQuery table
    And Capture raw logs
    Then Close the pipeline logs and stop the pipeline