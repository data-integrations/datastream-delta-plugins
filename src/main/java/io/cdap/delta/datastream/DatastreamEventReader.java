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

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.datastream.v1alpha1.DatastreamClient;
import com.google.cloud.datastream.v1alpha1.OperationMetadata;
import com.google.cloud.datastream.v1alpha1.ResumeStreamRequest;
import com.google.cloud.datastream.v1alpha1.StartStreamRequest;
import com.google.cloud.datastream.v1alpha1.Stream;
import com.google.cloud.storage.Storage;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.Offset;
import io.cdap.delta.datastream.util.Utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Reads events from DataStream
 */
public class DatastreamEventReader implements EventReader {

  private final DatastreamClient datastreamClient;
  private final DatastreamConfig config;
  private final DeltaSourceContext context;
  private final EventReaderDefinition definition;
  private final EventEmitter emitter;
  private final Storage storage;
  private final ScheduledExecutorService executorService;

  public DatastreamEventReader(DatastreamConfig config, EventReaderDefinition definition, DeltaSourceContext context,
    EventEmitter emitter, DatastreamClient datastreamClient, Storage storage) {
    this.context = context;
    this.config = config;
    this.definition = definition;
    this.emitter = emitter;
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.datastreamClient = datastreamClient;
    this.storage = storage;
  }

  @Override
  public void start(Offset offset) {

    startStreamIfNot();
  }

  public void stop() throws InterruptedException {
  }

  private void startStreamIfNot() {
    String parentPath = Utils.buildParentPath(config.getRegion());
    String replicatorId = Utils.buildReplicatorId(context);
    //TODO if the stream is resued, the stream name should be from config
    String streamName = Utils.buildStreamName(replicatorId);
    String streamPath = Utils.buildStreamPath(parentPath, streamName);
    Stream stream = datastreamClient.getStream(streamPath);

    OperationFuture<Stream, OperationMetadata> response = null;
    if (stream.getState() == Stream.State.PAUSED) {
      response = datastreamClient.resumeStreamAsync(ResumeStreamRequest.newBuilder().setName(streamPath).build());
    } else if (stream.getState() == Stream.State.CREATED) {
      response = datastreamClient.startStreamAsync(StartStreamRequest.newBuilder().setName(streamPath).build());
    }
    if (response != null) {
      // TODO call response.get() and handle errors once Datastream supports long running jobs
      // Currently Datastream java client has issue with it
      Utils.waitUntilComplete(response);
    }
  }
}
