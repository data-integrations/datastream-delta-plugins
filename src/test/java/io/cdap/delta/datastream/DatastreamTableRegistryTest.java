/*
 *
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.delta.datastream;

import com.google.auth.oauth2.GoogleCredentials;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.api.assessment.TableSummary;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class DatastreamTableRegistryTest {

  private static String serviceLocation;
  private static String oracleHost;
  private static String oracleUser;
  private static String oraclePassword;
  private static String oracleDb;
  private static int oraclePort;
  private static GoogleCredentials credentials;

  @BeforeAll
  public static void setupTestClass() throws Exception {
    // Certain properties need to be configured otherwise the whole tests will be skipped.
    // Check README for how to configure the properties below.

    String messageTemplate = "%s is not configured, please refer to README for details.";

    String project = System.getProperty("project.id");
    if (project == null) {
      project = System.getProperty("GOOGLE_CLOUD_PROJECT");
    }
    if (project == null) {
      project = System.getProperty("GCLOUD_PROJECT");
    }
    assumeFalse(project == null, String.format(messageTemplate, "project id"));

    String serviceAccountFilePath = System.getProperty("service.account.file");
    assumeFalse(serviceAccountFilePath == null, String.format(messageTemplate, "service account key file"));

    serviceLocation = System.getProperty("service.location");
    if (serviceLocation == null) {
      serviceLocation = "us-central1";
    }

    String port = System.getProperty("oracle.port");
    if (port == null) {
      oraclePort = 1521;
    } else {
      oraclePort = Integer.parseInt(port);
    }

    oracleHost = System.getProperty("oracle.host");
    assumeFalse(oracleHost == null);

    oracleUser = System.getProperty("oracle.user");
    assumeFalse(oracleUser == null);

    oraclePassword = System.getProperty("oracle.password");
    assumeFalse(oraclePassword == null);

    oracleDb = System.getProperty("oracle.database");
    assumeFalse(oracleDb == null);

    File serviceAccountFile = new File(serviceAccountFilePath);
    try (InputStream is = new FileInputStream(serviceAccountFile)) {
      credentials = GoogleCredentials.fromStream(is).createScoped("https://www.googleapis.com/auth/cloud-platform");
    }

  }

  @Test
  public void testListDescribeTable_IPallowList() throws IOException, TableNotFoundException {
    TableList tableList = null;
    try (DatastreamTableRegistry registry = new DatastreamTableRegistry(
      new DatastreamConfig(oracleHost, oraclePort, oracleUser, oraclePassword, oracleDb, serviceLocation,
        DatastreamConfig.CONNECTIVITY_METHOD_IP_ALLOWLISTING, null, null, null, null, null, null, null, null, null),
      credentials)) {
      tableList = registry.listTables();
    }
    assertNotNull(tableList);
    List<TableSummary> tables = tableList.getTables();
    assertNotNull(tables);
    assertFalse(tables.isEmpty());
    System.out.println("table counts:" + tables.size());
    for (TableSummary table : tables) {
      assertNotNull(table);
      assertEquals(oracleDb, table.getDatabase());
      String schema = table.getSchema();
      assertNotNull(schema);
      String tableName = table.getTable();
      assertNotNull(tableName);
      TableDetail tableDetail = null;
      try (DatastreamTableRegistry registry = new DatastreamTableRegistry(
        new DatastreamConfig(oracleHost, oraclePort, oracleUser, oraclePassword, oracleDb, serviceLocation,
          DatastreamConfig.CONNECTIVITY_METHOD_IP_ALLOWLISTING, null, null, null, null, null, null, null, null, null),
        credentials)) {
        tableDetail = registry.describeTable(oracleDb, tableName, schema);
      }
      assertNotNull(tableDetail);
      List<ColumnDetail> columns = tableDetail.getColumns();
      assertNotNull(columns);
      assertFalse(columns.isEmpty());
      for (ColumnDetail column : columns) {
        assertNotNull(column);
        assertNotNull(column.getName());
        Map<String, String> properties = column.getProperties();
        assertNotNull(properties);
        assertNotNull(properties.get(DatastreamTableAssessor.PRECISION));
        assertNotNull(properties.get(DatastreamTableAssessor.SCALE));
        assertNotNull(column.getType());
      }
      List<String> primaryKeys = tableDetail.getPrimaryKey();
      assertNotNull(primaryKeys);
      for (String key : primaryKeys) {
        assertNotNull(key);
        assertFalse(key.isEmpty());
      }
    }
  }

}
