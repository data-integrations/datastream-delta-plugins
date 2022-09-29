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

package com.google.cloud.datastream.v1;


import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.FailedPreconditionException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datastream.v1alpha1.AvroFileFormat;
import com.google.cloud.datastream.v1alpha1.ConnectionProfile;
import com.google.cloud.datastream.v1alpha1.CreateConnectionProfileRequest;
import com.google.cloud.datastream.v1alpha1.CreateStreamRequest;
import com.google.cloud.datastream.v1alpha1.DatastreamClient;
import com.google.cloud.datastream.v1alpha1.DatastreamSettings;
import com.google.cloud.datastream.v1alpha1.DestinationConfig;
import com.google.cloud.datastream.v1alpha1.DiscoverConnectionProfileRequest;
import com.google.cloud.datastream.v1alpha1.DiscoverConnectionProfileResponse;
import com.google.cloud.datastream.v1alpha1.FetchErrorsRequest;
import com.google.cloud.datastream.v1alpha1.FetchErrorsResponse;
import com.google.cloud.datastream.v1alpha1.GcsDestinationConfig;
import com.google.cloud.datastream.v1alpha1.GcsProfile;
import com.google.cloud.datastream.v1alpha1.NoConnectivitySettings;
import com.google.cloud.datastream.v1alpha1.OperationMetadata;
import com.google.cloud.datastream.v1alpha1.OracleColumn;
import com.google.cloud.datastream.v1alpha1.OracleProfile;
import com.google.cloud.datastream.v1alpha1.OracleRdbms;
import com.google.cloud.datastream.v1alpha1.OracleSchema;
import com.google.cloud.datastream.v1alpha1.OracleSourceConfig;
import com.google.cloud.datastream.v1alpha1.OracleTable;
import com.google.cloud.datastream.v1alpha1.SourceConfig;
import com.google.cloud.datastream.v1alpha1.StaticServiceIpConnectivity;
import com.google.cloud.datastream.v1alpha1.Stream;
import com.google.cloud.datastream.v1alpha1.Validation;
import com.google.cloud.datastream.v1alpha1.Validation.Status;
import com.google.cloud.datastream.v1alpha1.ValidationMessage;
import com.google.cloud.datastream.v1alpha1.ValidationResult;
import com.google.protobuf.Duration;
import com.google.protobuf.Empty;
import com.google.protobuf.FieldMask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;


public class DatastreamClientTest {

  private static final String FIELD_STATE = "state";
  private static String parent;
  private static String oracleHost;
  private static int oraclePort;
  private static String oracleUser;
  private static String oraclePassword;
  private static String oracleDb;
  private static String gcsBucket;
  private static com.google.cloud.datastream.v1alpha1.DatastreamClient datastream;

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
      credentials = GoogleCredentials.fromStream(is).createScoped("https://www.googleapis.com/auth/cloud-platform");
    }

    datastream =
      DatastreamClient.create(DatastreamSettings.newBuilder().setCredentialsProvider(new CredentialsProvider() {
        @Override
        public Credentials getCredentials() throws IOException {
          return credentials;
        }
      }).build());
  }

  @Test
  public void testDiscoverConnectionProfiles() throws IOException {
    com.google.cloud.datastream.v1alpha1.DiscoverConnectionProfileRequest request =
      DiscoverConnectionProfileRequest.newBuilder().setParent(parent).setRecursive(true)
        .setConnectionProfile(buildOracleConnectionProfile("discover-test")).build();
    com.google.cloud.datastream.v1alpha1.DiscoverConnectionProfileResponse response = datastream.discoverConnectionProfile(request);
    checkDiscoverResponse(response);
  }

  private void checkDiscoverResponse(DiscoverConnectionProfileResponse response) {
    com.google.cloud.datastream.v1alpha1.OracleRdbms rdbms = response.getOracleRdbms();
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
    OperationFuture<com.google.cloud.datastream.v1alpha1.ConnectionProfile, com.google.cloud.datastream.v1alpha1.OperationMetadata> sourceProfileCreationOperation =
      createOracleConnectionProfile(sourceName);

    String destinationName = "Datafusion-GCS-" + UUID.randomUUID();
    OperationFuture<com.google.cloud.datastream.v1alpha1.ConnectionProfile, com.google.cloud.datastream.v1alpha1.OperationMetadata> destinationProfileCreationOperation =
      createGcsConnectionProfile(destinationName);


    sourceProfileCreationOperation.get();
    assertNotNull(datastream.getConnectionProfile(buildConnectionProfilePath(sourceName)));

    destinationProfileCreationOperation.get();
    assertNotNull(datastream.getConnectionProfile(buildConnectionProfilePath(destinationName)));

    String streamName = "Datafusion-DS-" + UUID.randomUUID();
    com.google.cloud.datastream.v1alpha1.Stream.Builder streamBuilder = com.google.cloud.datastream.v1alpha1.Stream.newBuilder().setDisplayName(streamName).setDestinationConfig(
      com.google.cloud.datastream.v1alpha1.DestinationConfig.newBuilder().setDestinationConnectionProfileName(buildConnectionProfilePath(destinationName))
        .setGcsDestinationConfig(
          com.google.cloud.datastream.v1alpha1.GcsDestinationConfig.newBuilder().setAvroFileFormat(
                  com.google.cloud.datastream.v1alpha1.AvroFileFormat.getDefaultInstance()).setFileRotationMb(5)
            .setFileRotationInterval(Duration.newBuilder().setSeconds(15).build()))).setSourceConfig(
      com.google.cloud.datastream.v1alpha1.SourceConfig.newBuilder().setSourceConnectionProfileName(buildConnectionProfilePath(sourceName))
        .setOracleSourceConfig(com.google.cloud.datastream.v1alpha1.OracleSourceConfig.newBuilder().setAllowlist(
                com.google.cloud.datastream.v1alpha1.OracleRdbms.getDefaultInstance())
          .setRejectlist(com.google.cloud.datastream.v1alpha1.OracleRdbms.getDefaultInstance())))
      .setBackfillAll(com.google.cloud.datastream.v1alpha1.Stream.BackfillAllStrategy.getDefaultInstance());
    OperationFuture<com.google.cloud.datastream.v1alpha1.Stream, com.google.cloud.datastream.v1alpha1.OperationMetadata> streamCreationOperation = datastream.createStreamAsync(
      com.google.cloud.datastream.v1alpha1.CreateStreamRequest.newBuilder().setParent(parent).setStream(streamBuilder).setStreamId(streamName).build());

    Assertions.assertEquals(com.google.cloud.datastream.v1alpha1.Stream.State.CREATED, streamCreationOperation.get().getState());

    String streamPath = buildStreamPath(streamName);
    Assertions.assertEquals(
        com.google.cloud.datastream.v1alpha1.Stream.State.CREATED, datastream.getStream(streamPath).getState());


    OperationFuture<com.google.cloud.datastream.v1alpha1.Stream, com.google.cloud.datastream.v1alpha1.OperationMetadata> streamStartOperation = datastream
      .updateStreamAsync(
          com.google.cloud.datastream.v1alpha1.Stream.newBuilder().setName(streamPath).setState(
              com.google.cloud.datastream.v1alpha1.Stream.State.RUNNING).build(),
        FieldMask.newBuilder().addPaths(FIELD_STATE).build());

    assertEquals(
        com.google.cloud.datastream.v1alpha1.Stream.State.RUNNING, streamStartOperation.get().getState());
    assertEquals(
        com.google.cloud.datastream.v1alpha1.Stream.State.RUNNING, datastream.getStream(streamPath).getState());

    OperationFuture<FetchErrorsResponse, com.google.cloud.datastream.v1alpha1.OperationMetadata> fetchErrorOperation =
      datastream.fetchErrorsAsync(FetchErrorsRequest.newBuilder().setStream(streamPath).build());
    FetchErrorsResponse fetchErrorsResponse = fetchErrorOperation.get();
    assertTrue(fetchErrorsResponse.getErrorsList().isEmpty());

    OperationFuture<com.google.cloud.datastream.v1alpha1.Stream, com.google.cloud.datastream.v1alpha1.OperationMetadata> streamPauseOperation = datastream
      .updateStreamAsync(
          com.google.cloud.datastream.v1alpha1.Stream.newBuilder().setName(streamPath).setState(
              com.google.cloud.datastream.v1alpha1.Stream.State.PAUSED).build(),
        FieldMask.newBuilder().addPaths(FIELD_STATE).build());

    assertEquals(
        com.google.cloud.datastream.v1alpha1.Stream.State.PAUSED, streamPauseOperation.get().getState());
    assertEquals(
        com.google.cloud.datastream.v1alpha1.Stream.State.PAUSED, datastream.getStream(streamPath).getState());

    OperationFuture<Empty, com.google.cloud.datastream.v1alpha1.OperationMetadata> streamDeletionOperation = datastream.deleteStreamAsync(streamPath);

    streamDeletionOperation.get();
    assertThrows(NotFoundException.class, () -> datastream.getStream(buildStreamPath(streamName)));

    OperationFuture<Empty, com.google.cloud.datastream.v1alpha1.OperationMetadata> sourceProfileDeletionOperation =
      datastream.deleteConnectionProfileAsync(buildConnectionProfilePath(sourceName));

    OperationFuture<Empty, com.google.cloud.datastream.v1alpha1.OperationMetadata> destinationProfileDeletionOperation =
      datastream.deleteConnectionProfileAsync(buildConnectionProfilePath(destinationName));


    sourceProfileDeletionOperation.get();
    destinationProfileDeletionOperation.get();

    assertThrows(NotFoundException.class,
      () -> datastream.getConnectionProfile(buildConnectionProfilePath(sourceName)));

    assertThrows(NotFoundException.class,
      () -> datastream.getConnectionProfile(buildConnectionProfilePath(destinationName)));
  }

  @Test
  public void testValidateStreams() throws IOException, InterruptedException, ExecutionException {

    String sourceName = "Datafusion-Oracle-" + UUID.randomUUID();
    OperationFuture<com.google.cloud.datastream.v1alpha1.ConnectionProfile, com.google.cloud.datastream.v1alpha1.OperationMetadata> sourceProfileCreationOperation =
      createOracleConnectionProfile(sourceName);

    String destinationName = "Datafusion-GCS-" + UUID.randomUUID();
    String originalBucket = gcsBucket;
    gcsBucket = "non-existing";
    OperationFuture<com.google.cloud.datastream.v1alpha1.ConnectionProfile, com.google.cloud.datastream.v1alpha1.OperationMetadata> destinationProfileCreationOperation =
      createGcsConnectionProfile(destinationName);
    gcsBucket = originalBucket;

    sourceProfileCreationOperation.get();
    assertNotNull(datastream.getConnectionProfile(buildConnectionProfilePath(sourceName)));

    destinationProfileCreationOperation.get();
    assertNotNull(datastream.getConnectionProfile(buildConnectionProfilePath(destinationName)));

    String streamName = "Datafusion-DS-" + UUID.randomUUID();
    com.google.cloud.datastream.v1alpha1.Stream.Builder streamBuilder = com.google.cloud.datastream.v1alpha1.Stream.newBuilder().setDisplayName(streamName).setDestinationConfig(
      DestinationConfig.newBuilder().setDestinationConnectionProfileName(buildConnectionProfilePath(destinationName))
        .setGcsDestinationConfig(
          GcsDestinationConfig.newBuilder().setAvroFileFormat(AvroFileFormat.getDefaultInstance()).setFileRotationMb(5)
            .setFileRotationInterval(Duration.newBuilder().setSeconds(15).build()))).setSourceConfig(
      SourceConfig.newBuilder().setSourceConnectionProfileName(buildConnectionProfilePath(sourceName))
        .setOracleSourceConfig(OracleSourceConfig.newBuilder().setAllowlist(
                com.google.cloud.datastream.v1alpha1.OracleRdbms.getDefaultInstance())
          .setRejectlist(OracleRdbms.getDefaultInstance())))
      .setBackfillAll(com.google.cloud.datastream.v1alpha1.Stream.BackfillAllStrategy.getDefaultInstance());

    OperationFuture<Stream, com.google.cloud.datastream.v1alpha1.OperationMetadata> streamValidationOperation = datastream.createStreamAsync(
      CreateStreamRequest.newBuilder().setParent(parent)
              .setStream(streamBuilder)
              .setStreamId(streamName)
              .setValidateOnly(true)
              .build());
    ExecutionException exception = assertThrows(ExecutionException.class, () -> streamValidationOperation.get());
    assertTrue(exception.getCause() instanceof FailedPreconditionException);
    ValidationResult validationResult = streamValidationOperation.getMetadata().get().getValidationResult();
    assertFalse(validationResult.getValidationsList().isEmpty());
    for (com.google.cloud.datastream.v1alpha1.Validation validation : validationResult.getValidationsList()) {
      String code = validation.getCode();
      assertFalse(code.isEmpty());
      Status status = validation.getStatus();
      String description = validation.getDescription();
      assertFalse(description.isEmpty());

      if (code.equals("GCS_VALIDATE_PERMISSIONS")) {
        Assertions.assertEquals(com.google.cloud.datastream.v1alpha1.Validation.Status.FAILED, status);
        List<ValidationMessage> messages = validation.getMessageList();
        assertFalse(messages.get(0).getCode().isEmpty());
        assertFalse(messages.get(0).getMessage().isEmpty());
      } else {
        Assertions.assertEquals(Validation.Status.PASSED, status);
      }
    }

    // no need to delete stream, because validation failed without stream created
    datastream.deleteConnectionProfileAsync(buildConnectionProfilePath(sourceName)).get();
    datastream.deleteConnectionProfileAsync(buildConnectionProfilePath(destinationName)).get();
  }

  private OperationFuture<com.google.cloud.datastream.v1alpha1.ConnectionProfile, com.google.cloud.datastream.v1alpha1.OperationMetadata> createGcsConnectionProfile(String name)
    throws IOException {
    return datastream.createConnectionProfileAsync(
        com.google.cloud.datastream.v1alpha1.CreateConnectionProfileRequest.newBuilder().setParent(parent)
      .setConnectionProfile(buildGCSConnectionProfile(name)).setConnectionProfileId(name).build());
  }

  private OperationFuture<com.google.cloud.datastream.v1alpha1.ConnectionProfile, OperationMetadata> createOracleConnectionProfile(String name)
    throws IOException {
    return datastream.createConnectionProfileAsync(CreateConnectionProfileRequest.newBuilder().setParent(parent)
      .setConnectionProfile(buildOracleConnectionProfile(name)).setConnectionProfileId(name).build());
  }

  private String buildConnectionProfilePath(String resourceName) {
    return parent + "/connectionProfiles/" + resourceName;
  }

  private String buildStreamPath(String resourceName) {
    return parent + "/streams/" + resourceName;
  }

  private com.google.cloud.datastream.v1alpha1.ConnectionProfile buildOracleConnectionProfile(String name) {

    return com.google.cloud.datastream.v1alpha1.ConnectionProfile.newBuilder().setDisplayName(name)
      .setStaticServiceIpConnectivity(StaticServiceIpConnectivity.getDefaultInstance()).setOracleProfile(
        OracleProfile.newBuilder().setHostname(oracleHost).setUsername(oracleUser).setPassword(oraclePassword)
          .setDatabaseService(oracleDb).setPort(oraclePort)).build();
  }

  private com.google.cloud.datastream.v1alpha1.ConnectionProfile buildGCSConnectionProfile(String name) {
    return ConnectionProfile.newBuilder().setDisplayName(name)
      .setNoConnectivity(NoConnectivitySettings.getDefaultInstance())
      .setGcsProfile(GcsProfile.newBuilder().setBucketName(gcsBucket).setRootPath("/" + name)).build();
  }
}

