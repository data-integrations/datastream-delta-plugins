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

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.datastream.v1.CreateConnectionProfileRequest;
import com.google.cloud.datastream.v1.CreateStreamRequest;
import com.google.cloud.datastream.v1.DatastreamClient;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.SourceConfigurer;
import io.cdap.delta.api.SourceProperties;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.datastream.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static io.cdap.delta.datastream.util.Utils.buildOracleConnectionProfile;

/**
 * Datastream origin.
 */
@Plugin(type = DeltaSource.PLUGIN_TYPE)
@Name(DatastreamDeltaSource.NAME)
@Description("Delta source for Datastream.")
public class DatastreamDeltaSource implements DeltaSource {

  public static final String NAME = "OracleDatastream";
  public static final String BUCKET_CREATED_BY_CDF = "bucketCreatedByCDF";
  private static final Logger LOGGER = LoggerFactory.getLogger(DatastreamDeltaSource.class);
  private static final Gson GSON = new Gson();
  private final DatastreamConfig config;
  private Storage storage;
  private DatastreamClient datastream;
  private String parentPath;


  public DatastreamDeltaSource(DatastreamConfig config) {
    config.validate();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Instantiate Datstream delta source with config {}", GSON.toJson(config));
    }
    this.config = config;
  }

  @Override
  public void initialize(DeltaSourceContext context) throws Exception {
    storage =
      StorageOptions.newBuilder().setCredentials(config.getGcsCredentials()).setProjectId(config.getProject()).build()
        .getService();
    datastream = Utils.getDataStreamClient(config.getDatastreamCredentials());
    parentPath = Utils.buildParentPath(config.getProject(), config.getRegion());

    if (config.isUsingExistingStream()) {
      // for reusing an existing stream, it's possible we add more tables to replicate
      updateStream(context);
    } else {
      createStreamIfNotExisted(context);
    }
  }

  private void updateStream(DeltaSourceContext context) throws IOException {
    String streamPath = Utils.buildStreamPath(parentPath, config.getStreamId());
    Stream.Builder streamBuilder = Utils.getStream(datastream, streamPath, LOGGER).toBuilder();
    Utils.addToAllowList(streamBuilder, context.getAllTables());
    Utils.updateAllowlist(datastream, streamBuilder.build(), false, LOGGER);
  }

  @Override
  public void configure(SourceConfigurer configurer) {
    configurer.setProperties(
      new SourceProperties.Builder().setOrdering(SourceProperties.Ordering.UN_ORDERED).build());
  }

  @Override
  public DatastreamEventReader createReader(EventReaderDefinition definition, DeltaSourceContext context,
    EventEmitter eventEmitter) throws Exception {
    return new DatastreamEventReader(config, definition, context, eventEmitter, datastream, storage);
  }

  @Override
  public TableRegistry createTableRegistry(Configurer configurer) throws Exception {
    return new DatastreamTableRegistry(config, Utils.getDataStreamClient(config.getDatastreamCredentials()));
  }

  @Override
  public TableAssessor<TableDetail> createTableAssessor(Configurer configurer) throws Exception {
    return createTableAssessor(configurer, Collections.emptyList());
  }

  @Override
  public TableAssessor<TableDetail> createTableAssessor(Configurer configurer, List<SourceTable> tables)
    throws Exception {
    return new DatastreamTableAssessor(config, Utils.getDataStreamClient(config.getDatastreamCredentials()),
      StorageOptions.newBuilder().setCredentials(config.getGcsCredentials()).setProjectId(config.getProject()).build()
        .getService(), tables);
  }

  private void createStreamIfNotExisted(DeltaSourceContext context) throws IOException {

    String replicatorId = Utils.buildReplicatorId(context);
    String streamName = Utils.buildStreamName(replicatorId);
    String streamPath = Utils.buildStreamPath(parentPath, streamName);
    try {
      // try to see whether the stream was already created
      Utils.getStream(datastream, streamPath, LOGGER);
    } catch (NotFoundException e) {
      // stream does not exist
      String oracleProfileName = Utils.buildOracleProfileName(replicatorId);
      String oracleProfilePath = Utils.buildConnectionProfilePath(parentPath, oracleProfileName);

      // crete the oracle connection profile if not existing
      CreateConnectionProfileRequest createConnectionProfileRequest =
              CreateConnectionProfileRequest.newBuilder().setParent(parentPath)
                      .setConnectionProfile(buildOracleConnectionProfile(parentPath, oracleProfileName, config))
                      .setConnectionProfileId(oracleProfileName).build();
      Utils.createConnectionProfileIfNotExisting(datastream, createConnectionProfileRequest, LOGGER);

      String gcsProfileName = Utils.buildGcsProfileName(replicatorId);
      String gcsProfilePath = Utils.buildConnectionProfilePath(parentPath, gcsProfileName);
      // check whether GCS Bucket exists first
      String bucketName = config.getGcsBucket();
      // If user doesn't provide bucketName, we assign one based on run id
      if (bucketName == null || bucketName.trim().isEmpty()) {
        bucketName = Utils.buildBucketName(context.getRunId());
      }
      boolean bucketCreated = Utils.createBucketIfNotExisting(storage, bucketName, config.getGcsBucketLocation());
      context.putState(BUCKET_CREATED_BY_CDF, Bytes.toBytes(bucketCreated));
      // create the gcs connection profile
      createConnectionProfileRequest =
        CreateConnectionProfileRequest.newBuilder().setParent(parentPath).setConnectionProfile(
          Utils.buildGcsConnectionProfile(parentPath, gcsProfileName, bucketName, config.getGcsPathPrefix()))
          .setConnectionProfileId(gcsProfileName).build();
      Utils.createConnectionProfileIfNotExisting(datastream, createConnectionProfileRequest, LOGGER);

      // Create the stream
      Stream stream = Utils
              .buildStreamConfig(parentPath, streamName, oracleProfilePath, gcsProfilePath, context.getAllTables(),
                      config.shouldReplicateExistingData());
      CreateStreamRequest createStreamRequest =
              CreateStreamRequest.newBuilder().setParent(parentPath).setStream(stream).setStreamId(streamName).build();
      Utils.createStreamIfNotExisting(datastream, createStreamRequest, LOGGER);
    }
  }
}
