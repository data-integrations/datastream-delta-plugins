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

package com.google.cloud.datastream.v1alpha1;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.datastream.v1alpha1.DataStream;
import com.google.api.services.datastream.v1alpha1.model.AvroFileFormat;
import com.google.api.services.datastream.v1alpha1.model.ConnectionProfile;
import com.google.api.services.datastream.v1alpha1.model.DestinationConfig;
import com.google.api.services.datastream.v1alpha1.model.DiscoverConnectionProfileRequest;
import com.google.api.services.datastream.v1alpha1.model.DiscoverConnectionProfileResponse;
import com.google.api.services.datastream.v1alpha1.model.GcsDestinationConfig;
import com.google.api.services.datastream.v1alpha1.model.GcsProfile;
import com.google.api.services.datastream.v1alpha1.model.NoConnectivitySettings;
import com.google.api.services.datastream.v1alpha1.model.Operation;
import com.google.api.services.datastream.v1alpha1.model.OracleColumn;
import com.google.api.services.datastream.v1alpha1.model.OracleProfile;
import com.google.api.services.datastream.v1alpha1.model.OracleRdbms;
import com.google.api.services.datastream.v1alpha1.model.OracleSchema;
import com.google.api.services.datastream.v1alpha1.model.OracleSourceConfig;
import com.google.api.services.datastream.v1alpha1.model.OracleTable;
import com.google.api.services.datastream.v1alpha1.model.SourceConfig;
import com.google.api.services.datastream.v1alpha1.model.StaticServiceIpConnectivity;
import com.google.api.services.datastream.v1alpha1.model.Stream;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;


public class DatastreamClientTest {

  private static String parent;
  private static String oracleHost;
  private static int oraclePort;
  private static String serviceLocation;
  private static String oracleUser;
  private static String oraclePassword;
  private static String oracleDb;
  private static String gcsBucket;
  private static DataStream datastream;

  @BeforeAll
  public static void setupTestClass() throws Exception {
    // Certain properties need to be configured otherwise the whole tests will be skipped.
    // Check README for how to configure the properties below.
    String project = System.getProperty("project.id");
    if (project == null) {
      project = System.getProperty("GOOGLE_CLOUD_PROJECT");
    }
    if (project == null) {
      project = System.getProperty("GCLOUD_PROJECT");
    }
    assumeFalse(project == null);

    String serviceLocation = System.getProperty("service.location");
    if (serviceLocation == null) {
      serviceLocation = "us-central1";
    }

    parent = String.format("projects/%s/locations/%s", project, serviceLocation);

    String serviceAccountFilePath = System.getProperty("service.account.file");
    assumeFalse(serviceAccountFilePath == null);


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

    gcsBucket = System.getProperty("gcs.bucket");
    assumeFalse(gcsBucket == null);

    Credentials credentials;
    File serviceAccountFile = new File(serviceAccountFilePath);
    try (InputStream is = new FileInputStream(serviceAccountFile)) {
      credentials = GoogleCredentials.fromStream(is)
        .createScoped("https://www.googleapis.com/auth/cloud-platform");
    }

    datastream = new DataStream(new NetHttpTransport(), new JacksonFactory(),
      new HttpCredentialsAdapter(credentials));
  }

  @Test
  public void testDiscoverConnectionProfiles() throws IOException {

    DiscoverConnectionProfileRequest request =
      new DiscoverConnectionProfileRequest().setRecursive(true)
        .setConnectionProfile(buildOracleConnectionProfile("discover-test"));

    DiscoverConnectionProfileResponse response =
      datastream.projects().locations().connectionProfiles().discover(parent, request).execute();
    OracleRdbms rdbms = response.getOracleRdbms();
    assertNotNull(rdbms);
    for (OracleSchema schema : rdbms.getOracleSchemas()) {
      assertNotNull(schema.getSchemaName());
      if (schema.getOracleTables() == null) {
        continue;
      }
      for (OracleTable table : schema.getOracleTables()) {
        assertNotNull(table.getTableName());
        for (OracleColumn column : table.getOracleColumns()) {
          assertNotNull(column.getColumnName());
          assertNotNull(column.getDataType());
        }
      }
    }
  }

  @Test
  public void testStreams() throws IOException, ExecutionException, InterruptedException {

    String sourceName = "Datafusion-Oracle-" + UUID.randomUUID();
    Operation sourceProfileCreationOperation = createOracleConnectionProfile(sourceName);

    String destinationName = "Datafusion-GCS-" + UUID.randomUUID();
    Operation destinationProfileCreationOperation = createGcsConnectionProfile(destinationName);

    // TODO check with Datastream whether there is better way to query the status
    sourceProfileCreationOperation = waitUntilComplete(sourceProfileCreationOperation);
    assertNull(sourceProfileCreationOperation.getError());
    assertNotNull(datastream.projects().locations().connectionProfiles().get(buildConnectionProfilePath(sourceName)));

    destinationProfileCreationOperation = waitUntilComplete(destinationProfileCreationOperation);
    assertNull(destinationProfileCreationOperation.getError());
    assertNotNull(
      datastream.projects().locations().connectionProfiles().get(buildConnectionProfilePath(destinationName)));

    String streamName = "Datafusion-DS-" + UUID.randomUUID();
    Operation streamCreationOperation =
      datastream.projects().locations().streams().create(parent,
        new Stream().setDisplayName(streamName).setDestinationConfig(
        new DestinationConfig()
          .setDestinationConnectionProfileName(buildConnectionProfilePath(destinationName))
          .setGcsDestinationConfig(
            new GcsDestinationConfig().setAvroFileFormat(new AvroFileFormat())
              .setFileRotationMb(5)
              .setFileRotationInterval("15s"))).setSourceConfig(
        new SourceConfig()
          .setSourceConnectionProfileName(buildConnectionProfilePath(sourceName))
          .setOracleSourceConfig(
            new OracleSourceConfig().setAllowlist(new OracleRdbms())
              .setRejectlist(new OracleRdbms())))).setStreamId(streamName).execute();

    streamCreationOperation = waitUntilComplete(streamCreationOperation);
    assertNull(streamCreationOperation.getError());

    assertNotNull(datastream.projects().locations().streams().get(buildStreamPath(streamName)).execute());

    Operation streamDeletionOperation =
      datastream.projects().locations().streams().delete(buildStreamPath(streamName)).execute();

    streamDeletionOperation = waitUntilComplete(streamDeletionOperation);
    assertNull(streamDeletionOperation.getError());
    GoogleJsonResponseException exception = assertThrows(GoogleJsonResponseException.class,
      () -> datastream.projects().locations().streams().get(buildStreamPath(streamName)).execute());
    assertEquals(404, exception.getStatusCode());

    Operation sourceProfileDeletionOperation =
      datastream.projects().locations().connectionProfiles().delete(buildConnectionProfilePath(sourceName)).execute();

    Operation destinationProfileDeletionOperation =
      datastream.projects().locations().connectionProfiles().delete(buildConnectionProfilePath(destinationName))
        .execute();


    sourceProfileDeletionOperation = waitUntilComplete(sourceProfileDeletionOperation);
    assertNull(sourceProfileDeletionOperation.getError());

    destinationProfileDeletionOperation = waitUntilComplete(destinationProfileDeletionOperation);
    assertNull(destinationProfileDeletionOperation.getError());

    exception = assertThrows(GoogleJsonResponseException.class, () -> {
      datastream.projects().locations().connectionProfiles().get(buildConnectionProfilePath(sourceName)).execute();
    });
    assertEquals(404, exception.getStatusCode());

    exception = assertThrows(GoogleJsonResponseException.class,
      () -> datastream.projects().locations().connectionProfiles().get(buildConnectionProfilePath(destinationName))
        .execute());
    assertEquals(404, exception.getStatusCode());
  }

  private Operation waitUntilComplete(Operation operation) throws InterruptedException, IOException {
    while (!operation.getDone()) {
      TimeUnit.MILLISECONDS.sleep(200L);
      operation = datastream.projects().locations().operations().get(operation.getName()).execute();
    }
    return operation;
  }

  private Operation createGcsConnectionProfile(String name) throws IOException {
    return datastream.projects().locations().connectionProfiles().create(parent, buildGCSConnectionProfile(name))
      .setConnectionProfileId(name).execute();
  }

  private Operation createOracleConnectionProfile(String name) throws IOException {
    return datastream.projects().locations().connectionProfiles().create(parent, buildOracleConnectionProfile(name))
      .setConnectionProfileId(name).execute();
  }

  private String buildConnectionProfilePath(String resourceName) {
    return parent + "/connectionProfiles/" + resourceName;
  }

  private String buildStreamPath(String resourceName) {
    return parent + "/streams/" + resourceName;
  }

  private ConnectionProfile buildOracleConnectionProfile(String name) {
    return new ConnectionProfile().setDisplayName(name)
      .setStaticServiceIpConnectivity(new StaticServiceIpConnectivity()).setOracleProfile(
        new OracleProfile().setHostname(oracleHost).setUsername(oracleUser).setPassword(oraclePassword)
          .setDatabaseService(oracleDb).setPort(oraclePort));
  }

  private ConnectionProfile buildGCSConnectionProfile(String name) {
    return new ConnectionProfile().setDisplayName(name).setNoConnectivity(new NoConnectivitySettings())
      .setGcsProfile(new GcsProfile().setBucketName(gcsBucket).setRootPath("/" + name));
  }

}

