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
import com.google.api.gax.rpc.FailedPreconditionException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.protobuf.Duration;
import com.google.protobuf.Empty;
import com.google.protobuf.FieldMask;
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
    DiscoverConnectionProfileRequest request =
      DiscoverConnectionProfileRequest.newBuilder().setParent(parent).setRecursive(true)
        .setConnectionProfile(buildOracleConnectionProfile("discover-test")).build();
    DiscoverConnectionProfileResponse response = datastream.discoverConnectionProfile(request);
    checkDiscoverResponse(response);
  }

  private void checkDiscoverResponse(DiscoverConnectionProfileResponse response) {
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
    OperationFuture<ConnectionProfile, OperationMetadata> sourceProfileCreationOperation =
      createOracleConnectionProfile(sourceName);

    String destinationName = "Datafusion-GCS-" + UUID.randomUUID();
    OperationFuture<ConnectionProfile, OperationMetadata> destinationProfileCreationOperation =
      createGcsConnectionProfile(destinationName);


    sourceProfileCreationOperation.get();
    assertNotNull(datastream.getConnectionProfile(buildConnectionProfilePath(sourceName)));

    destinationProfileCreationOperation.get();
    assertNotNull(datastream.getConnectionProfile(buildConnectionProfilePath(destinationName)));

    String streamName = "Datafusion-DS-" + UUID.randomUUID();
    Stream.Builder streamBuilder = Stream.newBuilder().setDisplayName(streamName).setDestinationConfig(
      DestinationConfig.newBuilder().setDestinationConnectionProfileName(buildConnectionProfilePath(destinationName))
        .setGcsDestinationConfig(
          GcsDestinationConfig.newBuilder().setAvroFileFormat(AvroFileFormat.getDefaultInstance()).setFileRotationMb(5)
            .setFileRotationInterval(Duration.newBuilder().setSeconds(15).build()))).setSourceConfig(
      SourceConfig.newBuilder().setSourceConnectionProfileName(buildConnectionProfilePath(sourceName))
        .setOracleSourceConfig(OracleSourceConfig.newBuilder().setAllowlist(OracleRdbms.getDefaultInstance())
          .setRejectlist(OracleRdbms.getDefaultInstance())))
      .setBackfillAll(Stream.BackfillAllStrategy.getDefaultInstance());
    OperationFuture<Stream, OperationMetadata> streamCreationOperation = datastream.createStreamAsync(
      CreateStreamRequest.newBuilder().setParent(parent).setStream(streamBuilder).setStreamId(streamName).build());

    assertEquals(Stream.State.CREATED, streamCreationOperation.get().getState());

    String streamPath = buildStreamPath(streamName);
    assertEquals(Stream.State.CREATED, datastream.getStream(streamPath).getState());


    OperationFuture<Stream, OperationMetadata> streamStartOperation = datastream
      .updateStreamAsync(Stream.newBuilder().setName(streamPath).setState(Stream.State.RUNNING).build(),
        FieldMask.newBuilder().addPaths(FIELD_STATE).build());

    assertEquals(Stream.State.RUNNING, streamStartOperation.get().getState());
    assertEquals(Stream.State.RUNNING, datastream.getStream(streamPath).getState());

    OperationFuture<FetchErrorsResponse, OperationMetadata> fetchErrorOperation =
      datastream.fetchErrorsAsync(FetchErrorsRequest.newBuilder().setStream(streamPath).build());
    FetchErrorsResponse fetchErrorsResponse = fetchErrorOperation.get();
    assertTrue(fetchErrorsResponse.getErrorsList().isEmpty());

    OperationFuture<Stream, OperationMetadata> streamPauseOperation = datastream
      .updateStreamAsync(Stream.newBuilder().setName(streamPath).setState(Stream.State.PAUSED).build(),
        FieldMask.newBuilder().addPaths(FIELD_STATE).build());

    assertEquals(Stream.State.PAUSED, streamPauseOperation.get().getState());
    assertEquals(Stream.State.PAUSED, datastream.getStream(streamPath).getState());

    OperationFuture<Empty, OperationMetadata> streamDeletionOperation = datastream.deleteStreamAsync(streamPath);

    streamDeletionOperation.get();
    assertThrows(NotFoundException.class, () -> datastream.getStream(buildStreamPath(streamName)));

    OperationFuture<Empty, OperationMetadata> sourceProfileDeletionOperation =
      datastream.deleteConnectionProfileAsync(buildConnectionProfilePath(sourceName));

    OperationFuture<Empty, OperationMetadata> destinationProfileDeletionOperation =
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
    OperationFuture<ConnectionProfile, OperationMetadata> sourceProfileCreationOperation =
      createOracleConnectionProfile(sourceName);

    String destinationName = "Datafusion-GCS-" + UUID.randomUUID();
    String originalBucket = gcsBucket;
    gcsBucket = "non-existing";
    OperationFuture<ConnectionProfile, OperationMetadata> destinationProfileCreationOperation =
      createGcsConnectionProfile(destinationName);
    gcsBucket = originalBucket;

    sourceProfileCreationOperation.get();
    assertNotNull(datastream.getConnectionProfile(buildConnectionProfilePath(sourceName)));

    destinationProfileCreationOperation.get();
    assertNotNull(datastream.getConnectionProfile(buildConnectionProfilePath(destinationName)));

    String streamName = "Datafusion-DS-" + UUID.randomUUID();
    Stream.Builder streamBuilder = Stream.newBuilder().setDisplayName(streamName).setDestinationConfig(
      DestinationConfig.newBuilder().setDestinationConnectionProfileName(buildConnectionProfilePath(destinationName))
        .setGcsDestinationConfig(
          GcsDestinationConfig.newBuilder().setAvroFileFormat(AvroFileFormat.getDefaultInstance()).setFileRotationMb(5)
            .setFileRotationInterval(Duration.newBuilder().setSeconds(15).build()))).setSourceConfig(
      SourceConfig.newBuilder().setSourceConnectionProfileName(buildConnectionProfilePath(sourceName))
        .setOracleSourceConfig(OracleSourceConfig.newBuilder().setAllowlist(OracleRdbms.getDefaultInstance())
          .setRejectlist(OracleRdbms.getDefaultInstance())))
      .setBackfillAll(Stream.BackfillAllStrategy.getDefaultInstance());

    //TODO set validate_only to true when it works
    OperationFuture<Stream, OperationMetadata> streamValidationOperation = datastream.createStreamAsync(
      CreateStreamRequest.newBuilder().setParent(parent).setStream(streamBuilder).setStreamId(streamName).build());
    ExecutionException exception = assertThrows(ExecutionException.class, () -> streamValidationOperation.get());
    assertTrue(exception.getCause() instanceof FailedPreconditionException);
    ValidationResult validationResult = streamValidationOperation.getMetadata().get().getValidationResult();
    assertFalse(validationResult.getValidationsList().isEmpty());
    for (Validation validation : validationResult.getValidationsList()) {
      String code = validation.getCode();
      assertFalse(code.isEmpty());
      Validation.Status status = validation.getStatus();
      String description = validation.getDescription();
      assertFalse(description.isEmpty());

      if (code.equals("GCS_VALIDATE_PERMISSIONS")) {
        assertEquals(Validation.Status.FAILED, status);
        List<ValidationMessage> messages = validation.getMessageList();
        assertFalse(messages.get(0).getCode().isEmpty());
        assertFalse(messages.get(0).getMessage().isEmpty());
      } else {
        assertEquals(Validation.Status.PASSED, status);
      }
    }

    // no need to delete stream, because validation failed without stream created
    datastream.deleteConnectionProfileAsync(buildConnectionProfilePath(sourceName)).get();
    datastream.deleteConnectionProfileAsync(buildConnectionProfilePath(destinationName)).get();
  }

  private OperationFuture<ConnectionProfile, OperationMetadata> createGcsConnectionProfile(String name)
    throws IOException {
    return datastream.createConnectionProfileAsync(CreateConnectionProfileRequest.newBuilder().setParent(parent)
      .setConnectionProfile(buildGCSConnectionProfile(name)).setConnectionProfileId(name).build());
  }

  private OperationFuture<ConnectionProfile, OperationMetadata> createOracleConnectionProfile(String name)
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

  private ConnectionProfile buildOracleConnectionProfile(String name) {

    return ConnectionProfile.newBuilder().setDisplayName(name)
      .setStaticServiceIpConnectivity(StaticServiceIpConnectivity.getDefaultInstance()).setOracleProfile(
        OracleProfile.newBuilder().setHostname(oracleHost).setUsername(oracleUser).setPassword(oraclePassword)
          .setDatabaseService(oracleDb).setPort(oraclePort)).build();
  }

  private ConnectionProfile buildGCSConnectionProfile(String name) {
    return ConnectionProfile.newBuilder().setDisplayName(name)
      .setNoConnectivity(NoConnectivitySettings.getDefaultInstance())
      .setGcsProfile(GcsProfile.newBuilder().setBucketName(gcsBucket).setRootPath("/" + name)).build();
  }
}

