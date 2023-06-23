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

@Mysql
Feature: Mysql - Verify Mysql source data transfer to Big Query

  @MYSQL_SOURCE
  Scenario: To verify replication of snapshot and cdc data from MySQL to Big Query successfully
    Given Open DataFusion Project with replication to configure pipeline
    When Enter input plugin property: "name" with value: "pipelineName"
    And Click on the button "Next"
    And Select Source plugin: "MySQL" from the replication plugins list
    Then Replace input plugin property: "host" with value: "mysqlHost" for Credentials and Authorization related fields
    Then Replace input plugin property: "port" with value: "mysqlPort" for Credentials and Authorization related fields
    Then Select dropdown plugin property: "select-jdbcPluginName" with option value: "mysqlDriverName"
    Then Replace input plugin property: "database" with value: "mysqlDatabaseName"
    Then Replace input plugin property: "user" with value: "mysqlUsername" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "mysqlPassword" for Credentials and Authorization related fields
    And Click on the button "Next"
    Then Replace input plugin property: "project" with value: "bqProjectId"
    Then Enter input plugin property: "datasetName" with value: "bqDataset"
    And Click on the button "Next"
    Then Enter source table in search panel to select
#    Then Validate MySQL Source table is available and select it
    And Click on the button "Next"
    And Click on the button "Next"
    And Click on the button "Next"
    Then Deploy the replication pipeline
    And Run the replication Pipeline
    Then Open the logs
    And Wait till pipeline is in running state and check if no errors occurred
##    Then Verify expected MySQL records in target BigQuery table
#    And Run insert, update and delete CDC events on source table
#    And Wait till CDC events are reflected in BQ
##    Then Verify expected Oracle records in target BigQuery table
    And Capture raw logs
    Then Close the pipeline logs and stop the pipeline
