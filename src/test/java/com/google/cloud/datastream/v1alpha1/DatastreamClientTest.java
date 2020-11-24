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

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.protobuf.Duration;
import com.google.protobuf.Empty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
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
  private static DatastreamClient datastream;

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

    datastream = DatastreamClient
      .create(DatastreamSettings.newBuilder().setCredentialsProvider(new CredentialsProvider() {
        @Override
        public Credentials getCredentials() throws IOException {
          return credentials;
        }
      }).build());


  }

  @Test
  public void testDiscoverConnectionProfiles() throws IOException {

    DiscoverConnectionProfileRequest request =
      DiscoverConnectionProfileRequest.newBuilder().setParent(parent).setRecursive(true)
        .setConnectionProfile(buildOracleConnectionProfile("discover-test")).build();

    DiscoverConnectionProfileResponse response = datastream.discoverConnectionProfile(request);
    OracleRdbms rdbms = response.getOracleRdbms();
    assertNotNull(rdbms);
    for (OracleSchema schema : rdbms.getOracleSchemasList()) {
      assertNotNull(schema.getSchemaName());
      for (OracleTable table : schema.getOracleTablesList()) {
        assertNotNull(table.getTableName());
        for (OracleColumn column : table.getOracleColumnsList()) {
          assertNotNull(column.getColumnName());
          assertNotNull(column.getDataType());
        }
      }
    }
  }

  @Test
  public void testStreams() throws IOException, ExecutionException, InterruptedException {

    String sourceName = "Datafusion-Oracle-" + UUID.randomUUID();
    OperationFuture<ConnectionProfile, OperationMetadata> sourceProfileResponse =
      createOracleConnectionProfile(sourceName);

    String destinationName = "Datafusion-GCS-" + UUID.randomUUID();
    OperationFuture<ConnectionProfile, OperationMetadata> destinationProfileResponse =
      createGcsConnectionProfile(destinationName);

    // TODO Replace below with sourceProfileResponse.get() and destinationProfileResponse.get() to
    //  block until source and destination connection profiles are successfully created. So far this
    //  method call (getting the long running operation) has some issue for java client.
    waitUntilComplete(sourceProfileResponse);
    waitUntilComplete(destinationProfileResponse);

    String streamName = "Datafusion-DS-" + UUID.randomUUID();
    CreateStreamRequest createStreamRequest =
      CreateStreamRequest.newBuilder().setParent(parent).setStreamId(streamName).setStream(
        Stream.newBuilder().setDisplayName(streamName).setDestinationConfig(
          DestinationConfig.newBuilder()
            .setDestinationConnectionProfileName(buildConnectionProfilePath(destinationName))
            .setGcsDestinationConfig(
              GcsDestinationConfig.newBuilder().setGcsFileFormat(GcsFileFormat.AVRO)
                .setFileRotationMb(5)
                .setFileRotationInterval(Duration.newBuilder().setSeconds(15)))).setSourceConfig(
          SourceConfig.newBuilder()
            .setSourceConnectionProfileName(buildConnectionProfilePath(sourceName))
            .setOracleSourceConfig(
              OracleSourceConfig.newBuilder().setAllowlist(OracleRdbms.getDefaultInstance())
                .setRejectlist(OracleRdbms.getDefaultInstance())))).build();
    OperationFuture<Stream, OperationMetadata> createStreamResponse =
      datastream.createStreamAsync(createStreamRequest);

    // TODO Replace below with createStreamResponse.get() to block until stream is successfully
    //  created. So far this method call (getting the long running operation) has some issue for
    //  java client.
    waitUntilComplete(createStreamResponse);

    Stream stream = datastream.getStream(buildStreamPath(streamName));
    assertNotNull(stream);

    // TODO check whether the source and destination in the stream matches the one from
    //  sourceProfileResponse.get() and destinationProfileResponse.get() So far this method call
    //  (getting the long running operation) has some issue for java client.

    OperationFuture<Empty, OperationMetadata> deleteStreamResponse =
      datastream.deleteStreamAsync(buildStreamPath(streamName));

    //TODO Replace below with deleteStreamResponse.get() to block until stream is successfully
    // deleted. So far this method call (getting the long running operation) has some issue for java
    // client.
    waitUntilComplete(deleteStreamResponse);
    assertThrows(NotFoundException.class, () -> datastream.getStream(buildStreamPath(streamName)));

    OperationFuture<Empty, OperationMetadata> deleteSourceResponse =
      datastream.deleteConnectionProfileAsync(buildConnectionProfilePath(sourceName));

    OperationFuture<Empty, OperationMetadata> deleteDestinationResponse =
      datastream.deleteConnectionProfileAsync(buildConnectionProfilePath(destinationName));

    //TODO Replace below with deleteSourceResponse.get() and deleteDestinationResponse.get()
    // to block until source and  destination connection profiles are successfully deleted. So far
    // this method call (getting the long running operation) has some issue for java client.
    waitUntilComplete(deleteSourceResponse);
    waitUntilComplete(deleteDestinationResponse);
    assertThrows(NotFoundException.class,
                 () -> datastream.getConnectionProfile(buildConnectionProfilePath(sourceName)));
    assertThrows(NotFoundException.class, () -> datastream
      .getConnectionProfile(buildConnectionProfilePath(destinationName)));
  }

  private void waitUntilComplete(Future<?> future) throws InterruptedException {
    while (!future.isDone() && !future.isCancelled()) {
      TimeUnit.MILLISECONDS.sleep(200L);
    }
  }

  private OperationFuture<ConnectionProfile, OperationMetadata> createGcsConnectionProfile(String destinationName) {
    CreateConnectionProfileRequest destinationProfileRequest =
      CreateConnectionProfileRequest.newBuilder().setParent(parent)
        .setConnectionProfileId(destinationName)
        .setConnectionProfile(buildGCSConnectionProfile(destinationName)).build();
    return datastream.createConnectionProfileAsync(destinationProfileRequest);
  }

  private OperationFuture<ConnectionProfile, OperationMetadata> createOracleConnectionProfile(String name) {
    CreateConnectionProfileRequest sourceProfileRequest =
      CreateConnectionProfileRequest.newBuilder().setParent(parent).setConnectionProfileId(name)
        .setConnectionProfile(buildOracleConnectionProfile(name)).build();
    return datastream.createConnectionProfileAsync(sourceProfileRequest);
  }

  private String buildConnectionProfilePath(String resourceName) {
    return parent + "/connectionProfiles/" + resourceName;
  }

  private String buildStreamPath(String resourceName) {
    return parent + "/streams/" + resourceName;
  }

  private ConnectionProfile.Builder buildOracleConnectionProfile(String name) {
    return ConnectionProfile.newBuilder().setDisplayName(name)
      .setStaticServiceIpConnectivity(StaticServiceIpConnectivity.getDefaultInstance())
      .setOracleProfile(OracleProfile.newBuilder().setHostname(oracleHost).setUsername(oracleUser)
                          .setPassword(oraclePassword).setDatabaseService(oracleDb)
                          .setPort(oraclePort));
  }

  private ConnectionProfile.Builder buildGCSConnectionProfile(String name) {
    return ConnectionProfile.newBuilder().setDisplayName(name)
      .setNoConnectivity(NoConnectivitySettings.getDefaultInstance())
      .setGcsProfile(GcsProfile.newBuilder().setBucketName(gcsBucket).setRootPath("/" + name));
  }

}

