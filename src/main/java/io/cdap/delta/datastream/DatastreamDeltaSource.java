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
import com.google.cloud.datastream.v1alpha1.DatastreamClient;
import com.google.cloud.datastream.v1alpha1.OperationMetadata;
import com.google.cloud.datastream.v1alpha1.Stream;
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
import io.cdap.delta.datastream.util.DatastreamUtils;

import static io.cdap.delta.datastream.util.DatastreamUtils.buildOracleConnectionProfile;
import static io.cdap.delta.datastream.util.DatastreamUtils.waitUntilComplete;

/**
 * Datastream origin.
 */
@Plugin(type = DeltaSource.PLUGIN_TYPE)
@Name(DatastreamDeltaSource.NAME)
@Description("Delta source for Datastream.")
public class DatastreamDeltaSource implements DeltaSource {

  public static final String NAME = "datastream";


  private final DatastreamConfig config;
  private Storage storage;
  private DatastreamClient datastreamClient;
  private String parentPath;


  public DatastreamDeltaSource(DatastreamConfig config) {
    config.validate();
    this.config = config;
  }

  @Override
  public void initialize(DeltaSourceContext context) throws Exception {
    storage = StorageOptions.newBuilder().setCredentials(config.getGcsCredentials())
      .setProjectId(ServiceOptions.getDefaultProjectId()).build().getService();
    datastreamClient = DatastreamClient.create();
    createStreamIfNotExisted(context);
  }

  @Override
  public void configure(SourceConfigurer configurer) {
    configurer.setProperties(new SourceProperties.Builder().setRowIdSupported(true).build());
  }

  @Override
  public DatastreamEventReader createReader(EventReaderDefinition definition, DeltaSourceContext context,
    EventEmitter eventEmitter) {
    return new DatastreamEventReader(config, definition, context, eventEmitter, datastreamClient, storage);
  }

  @Override
  public TableRegistry createTableRegistry(Configurer configurer) throws Exception {
    return new DatastreamTableRegistry(config, DatastreamClient.create());
  }

  @Override
  public TableAssessor<TableDetail> createTableAssessor(Configurer configurer) throws Exception {
    return new DatastreamTableAssessor(config);
  }

  private void createStreamIfNotExisted(DeltaSourceContext context) {
    String parentPath = DatastreamUtils.buildParentPath(config.getRegion());
    String replicatorId = DatastreamUtils.buildReplicatorId(context);
    String streamName = DatastreamUtils.buildStreamName(replicatorId);
    String streamPath = DatastreamUtils.buildStreamPath(parentPath, streamName);
    try {
      // try to see whether the stream was already created
      datastreamClient.getStream(streamPath);
    } catch (NotFoundException e) {
      // stream does not exist
      String sourceProfileName = DatastreamUtils.buildSourceProfileName(replicatorId);
      String sourceProfilePath = DatastreamUtils.buildConnectionProfilePath(parentPath, sourceProfileName);
      try {
        // try to check whether the source connection profile was already created
        datastreamClient.getConnectionProfile(sourceProfilePath);
      } catch (NotFoundException es) {
        // source connection profile does not exist
        // crete the source connection profile
        OperationFuture<ConnectionProfile, OperationMetadata> response = datastreamClient.createConnectionProfileAsync(
          DatastreamUtils.buildSourceProfileCreationRequest(parentPath, sourceProfileName,
            buildOracleConnectionProfile(config)));
        // TODO call response.get() and handle errors once Datastream supports long running jobs
        // Currently Datastream java client has issue with it
        waitUntilComplete(response);
      }


      String targetProfileName = DatastreamUtils.buildTargetProfileName(replicatorId);
      String targetProfilePath = DatastreamUtils.buildConnectionProfilePath(parentPath, targetProfileName);
      try {
        // try to check whether the target connection profile was already created
        datastreamClient.getConnectionProfile(targetProfilePath);
      } catch (NotFoundException es) {
        // target connection profile does not exist
        // check whether GCS Bucket exists first
        Bucket bucket = storage.get(config.getGcsBucket());
        if (bucket == null) {
          // create corresponding GCS bucket
          storage.create(BucketInfo.newBuilder(config.getGcsBucket()).build());
        }

        // crete the target connection profile
        OperationFuture<ConnectionProfile, OperationMetadata> response = datastreamClient.createConnectionProfileAsync(
          DatastreamUtils.buildTargetProfileCreationRequest(parentPath, targetProfileName, config.getGcsBucket(),
            config.getGcsPathPrefix()));

        // TODO call response.get() and handle errors once Datastream supports long running jobs
        // Currently Datastream java client has issue with it
        waitUntilComplete(response);
      }

      // Create the stream
      OperationFuture<Stream, OperationMetadata> response = datastreamClient.createStreamAsync(
        DatastreamUtils.buildStreamCreationRequest(parentPath, streamName, sourceProfilePath, targetProfilePath,
          context.getAllTables()));
      // TODO call response.get() and handle errors once Datastream supports long running jobs
      // Currently Datastream java client has issue with it
      waitUntilComplete(response);
    }
  }


}
