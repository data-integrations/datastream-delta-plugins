/*
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

import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.ServiceOptions;
import com.google.cloud.datastream.v1alpha1.ConnectionProfile;
import com.google.cloud.datastream.v1alpha1.CreateConnectionProfileRequest;
import com.google.cloud.datastream.v1alpha1.CreateStreamRequest;
import com.google.cloud.datastream.v1alpha1.DatastreamClient;
import com.google.cloud.datastream.v1alpha1.DestinationConfig;
import com.google.cloud.datastream.v1alpha1.GcsDestinationConfig;
import com.google.cloud.datastream.v1alpha1.GcsFileFormat;
import com.google.cloud.datastream.v1alpha1.GcsProfile;
import com.google.cloud.datastream.v1alpha1.NoConnectivitySettings;
import com.google.cloud.datastream.v1alpha1.OperationMetadata;
import com.google.cloud.datastream.v1alpha1.OracleRdbms;
import com.google.cloud.datastream.v1alpha1.OracleSchema;
import com.google.cloud.datastream.v1alpha1.OracleSourceConfig;
import com.google.cloud.datastream.v1alpha1.OracleTable;
import com.google.cloud.datastream.v1alpha1.SourceConfig;
import com.google.cloud.datastream.v1alpha1.Stream;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Duration;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.Offset;
import io.cdap.delta.datastream.util.DatastreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Reads events from DataStream
 */
public class DatastreamEventReader implements EventReader {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamTableRegistry.class);

  private final DatastreamClient datastreamClient;
  private final DatastreamConfig config;
  private final DeltaSourceContext context;
  private final EventReaderDefinition definition;
  private final EventEmitter emitter;
  private final String parentPath;
  private final Storage storage;

  public DatastreamEventReader(DatastreamConfig config, EventReaderDefinition definition, DeltaSourceContext context,
    EventEmitter emitter, DatastreamClient datastreamClient, Storage storage) {
    this.context = context;
    this.config = config;
    this.definition = definition;
    this.emitter = emitter;
    this.datastreamClient = datastreamClient;
    this.storage = storage;
    this.parentPath =
      String.format("projects/%s/locations/%s", ServiceOptions.getDefaultProjectId(), config.getRegion());
  }

  @Override
  public void start(Offset offset) {
    Map<String, String> state = offset.get();
    if (state.isEmpty()) {
      createStreamIfNotExisted();
    }

  }

  @VisibleForTesting
  void createStreamIfNotExisted() {
    String streamName = buildStreamName();
    String streamPath = buildStreamPath(streamName);
    try {
      datastreamClient.getStream(streamPath);
    } catch (NotFoundException e) {
      String sourceProfileName = buildSourceProfileName();
      String sourceProfilePath = buildConnectionProfilePath(sourceProfileName);
      try {
        datastreamClient.getConnectionProfile(sourceProfilePath);
      } catch (NotFoundException es) {
        OperationFuture<ConnectionProfile, OperationMetadata> response =
          datastreamClient.createConnectionProfileAsync(buildSourceProfileCreationRequest(sourceProfileName));
        // TODO call response.get()
        // Currently Datastream java client doesn't support long running task well
        waitUntilComplete(response);
      }
      String targetProfileName = buildTargetProfileName();
      String targetProfilePath = buildConnectionProfilePath(targetProfileName);
      try {
        datastreamClient.getConnectionProfile(targetProfilePath);
      } catch (NotFoundException es) {
        // check GCS Bucket first
        Bucket bucket = storage.get(config.getGcsBucket());
        if (bucket == null) {
          storage.create(BucketInfo.newBuilder(config.getGcsBucket()).build());
        }
        OperationFuture<ConnectionProfile, OperationMetadata> response =
          datastreamClient.createConnectionProfileAsync(buildTargetProfileCreationRequest(targetProfileName));
        // TODO call response.get()
        // Currently Datastream java client doesn't support long running task well
        waitUntilComplete(response);
      }
      OperationFuture<Stream, OperationMetadata> response = datastreamClient
        .createStreamAsync(buildStreamCreationRequest(streamName, sourceProfilePath, targetProfilePath));
      // TODO call response.get()
      // Currently Datastream java client doesn't support long running task well
      waitUntilComplete(response);
    }
  }

  private CreateStreamRequest buildStreamCreationRequest(String name, String sourcePath, String targetPath) {
    return CreateStreamRequest.newBuilder().setParent(parentPath).setStreamId(name).setStream(
      Stream.newBuilder().setDisplayName(name).setDestinationConfig(
        DestinationConfig.newBuilder().setDestinationConnectionProfileName(targetPath).setGcsDestinationConfig(
          GcsDestinationConfig.newBuilder().setGcsFileFormat(GcsFileFormat.AVRO).setPath("/" + name)
            .setFileRotationMb(5).setFileRotationInterval(Duration.newBuilder().setSeconds(15)))).setSourceConfig(
        SourceConfig.newBuilder().setSourceConnectionProfileName(sourcePath).setOracleSourceConfig(
          OracleSourceConfig.newBuilder().setAllowlist(buildAllowlist())))).build();
  }

  private OracleRdbms buildAllowlist() {
    OracleRdbms.Builder rdbms = OracleRdbms.newBuilder();
    Map<String, OracleSchema.Builder> schemaToTables = new HashMap<>();
    definition.getAllTables().forEach(
      table -> schemaToTables.computeIfAbsent(table.getSchema(), name -> OracleSchema.newBuilder().setSchemaName(name))
        .addOracleTables(OracleTable.newBuilder().setTableName(table.getTable())));
    schemaToTables.values().forEach(rdbms::addOracleSchemas);
    return rdbms.build();
  }

  private void waitUntilComplete(Future<?> future) {
    while (!future.isDone() && !future.isCancelled()) {
      try {
        TimeUnit.MILLISECONDS.sleep(200L);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private CreateConnectionProfileRequest buildTargetProfileCreationRequest(String name) {
    return CreateConnectionProfileRequest.newBuilder().setParent(parentPath).setConnectionProfileId(name)
      .setConnectionProfile(ConnectionProfile.newBuilder().setDisplayName(name)
        .setNoConnectivity(NoConnectivitySettings.getDefaultInstance()).setGcsProfile(
          GcsProfile.newBuilder().setBucketName(config.getGcsBucket()).setRootPath(config.getGcsPathPrefix()))).build();
  }

  private CreateConnectionProfileRequest buildSourceProfileCreationRequest(String name) {
    return CreateConnectionProfileRequest.newBuilder().setParent(parentPath).setConnectionProfileId(name)
      .setConnectionProfile(DatastreamUtils.buildOracleConnectionProfile(config).setDisplayName(name)).build();
  }

  private String buildConnectionProfilePath(String name) {
    return String.format("%s/connectionProfiles/%s", parentPath, name);
  }

  private String buildSourceProfileName() {
    return "CDF-Src-" + buildReplicatorId();
  }

  private String buildTargetProfileName() {
    return "CDF-Tgt-" + buildReplicatorId();
  }

  private String buildStreamPath(String name) {
    return String.format("%s/streams/%s", parentPath, name);
  }

  private String buildStreamName() {
    return "CDF-Stream-" + buildReplicatorId();
  }

  private String buildReplicatorId() {
    DeltaPipelineId pipelineId = context.getPipelineId();
    return pipelineId.getNamespace() + "-" + pipelineId.getApp() + "-" + pipelineId.getGeneration();
  }

  public void stop() throws InterruptedException {
  }
}
