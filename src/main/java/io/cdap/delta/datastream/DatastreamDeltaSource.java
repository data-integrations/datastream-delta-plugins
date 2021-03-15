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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.datastream.v1alpha1.DataStream;
import com.google.api.services.datastream.v1alpha1.model.Operation;
import com.google.api.services.datastream.v1alpha1.model.Stream;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.SourceConfigurer;
import io.cdap.delta.api.SourceProperties;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.datastream.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static io.cdap.delta.datastream.util.Utils.buildOracleConnectionProfile;
import static io.cdap.delta.datastream.util.Utils.waitUntilComplete;

/**
 * Datastream origin.
 */
@Plugin(type = DeltaSource.PLUGIN_TYPE)
@Name(DatastreamDeltaSource.NAME)
@Description("Delta source for Datastream.")
public class DatastreamDeltaSource implements DeltaSource {

  public static final String NAME = "datastream";
  private static final Logger LOGGER = LoggerFactory.getLogger(DatastreamDeltaSource.class);
  private static final String GCS_BUCKET_NAME_PREFIX = "df-rds-";
  private final DatastreamConfig config;
  private Storage storage;
  private DataStream datastream;
  private String parentPath;


  public DatastreamDeltaSource(DatastreamConfig config) {
    config.validate();
    this.config = config;
  }

  @Override
  public void initialize(DeltaSourceContext context) throws Exception {
    storage = StorageOptions.newBuilder().setCredentials(config.getGcsCredentials())
      .setProjectId(config.getProject()).build().getService();
    datastream = createDatastreamClient();
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
    Stream stream =
      datastream.projects().locations().streams().get(streamPath)
        .execute();
    Utils.addToAllowList(stream, context.getAllTables());
    //TODO add update mask when new client is ready for the new API
    Operation operation = datastream.projects().locations().streams().patch(streamPath, stream).execute();
    Utils.waitUntilComplete(datastream, operation, LOGGER);
  }

  private DataStream createDatastreamClient() throws IOException {
    HttpRequestInitializer httpRequestInitializer = Utils.setAdditionalHttpRequestHeaders(
      new HttpCredentialsAdapter(config.getDatastreamCredentials()));
    return new DataStream(new NetHttpTransport(), new JacksonFactory(), httpRequestInitializer);
  }

  @Override
  public void configure(SourceConfigurer configurer) {
    configurer.setProperties(new SourceProperties.Builder().setRowIdSupported(true).setOrdering(
      SourceProperties.Ordering.UN_ORDERED).build());
  }

  @Override
  public DatastreamEventReader createReader(EventReaderDefinition definition, DeltaSourceContext context,
    EventEmitter eventEmitter) throws Exception {
    return new DatastreamEventReader(config, definition, context, eventEmitter, datastream, storage);
  }

  @Override
  public TableRegistry createTableRegistry(Configurer configurer) throws Exception {
    return new DatastreamTableRegistry(config, createDatastreamClient());
  }

  @Override
  public TableAssessor<TableDetail> createTableAssessor(Configurer configurer) throws Exception {
    return new DatastreamTableAssessor(config);
  }

  private void createStreamIfNotExisted(DeltaSourceContext context) throws IOException {

    String replicatorId = Utils.buildReplicatorId(context);
    String streamName = Utils.buildStreamName(replicatorId);
    String streamPath = Utils.buildStreamPath(parentPath, streamName);
    try {
      // try to see whether the stream was already created
      datastream.projects().locations().streams().get(streamPath).execute();
    } catch (GoogleJsonResponseException e) {
      if (404 != e.getStatusCode()) {
        throw e;
      }
      // stream does not exist
      String oracleProfileName = Utils.buildOracleProfileName(replicatorId);
      String oracleProfilePath = Utils.buildConnectionProfilePath(parentPath, oracleProfileName);
      try {
        // try to check whether the oracle connection profile was already created
        datastream.projects().locations().connectionProfiles().get(oracleProfilePath).execute();
      } catch (GoogleJsonResponseException ex) {
        if (404 != ex.getStatusCode()) {
          throw ex;
        }
        // oracle connection profile does not exist
        // crete the oracle connection profile
        Operation operation = datastream.projects().locations().connectionProfiles()
          .create(parentPath, buildOracleConnectionProfile(oracleProfileName, config))
          .setConnectionProfileId(oracleProfileName).execute();
        waitUntilComplete(datastream, operation, LOGGER);
      }


      String gcsProfileName = Utils.buildGcsProfileName(replicatorId);
      String gcsProfilePath = Utils.buildConnectionProfilePath(parentPath, gcsProfileName);
      try {
        // try to check whether the gcs connection profile was already created
        datastream.projects().locations().connectionProfiles().get(gcsProfilePath).execute();
      } catch (GoogleJsonResponseException ex) {
        if (404 != ex.getStatusCode()) {
          throw ex;
        }
        // gcs connection profile does not exist
        // check whether GCS Bucket exists first
        String bucketName = config.getGcsBucket();
        // If user doesn't provide bucketName, we assign one based on run id
        if (bucketName == null) {
          bucketName = GCS_BUCKET_NAME_PREFIX + context.getRunId();
        }
        Bucket bucket = storage.get(bucketName);
        if (bucket == null) {
          // create corresponding GCS bucket
          storage.create(BucketInfo.newBuilder(bucketName).build());
        }

        // crete the gcs connection profile
        Operation operation = datastream.projects().locations().connectionProfiles().create(parentPath,
          Utils.buildGcsConnectionProfile(parentPath, gcsProfileName, bucketName, config.getGcsPathPrefix()))
          .setConnectionProfileId(gcsProfileName).execute();

        waitUntilComplete(datastream, operation, LOGGER);
      }

      // Create the stream
      Operation operation = datastream.projects().locations().streams().create(parentPath,
        Utils.buildStreamConfig(parentPath, streamName, oracleProfilePath, gcsProfilePath, context.getAllTables()))
        .setStreamId(streamName).execute();
      waitUntilComplete(datastream, operation, LOGGER);
    }
  }


}
