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

import com.google.api.services.datastream.v1alpha1.DataStream;
import com.google.api.services.datastream.v1alpha1.model.Operation;
import com.google.api.services.datastream.v1alpha1.model.ResumeStreamRequest;
import com.google.api.services.datastream.v1alpha1.model.StartStreamRequest;
import com.google.api.services.datastream.v1alpha1.model.Stream;
import com.google.cloud.storage.Storage;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.Offset;
import io.cdap.delta.datastream.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Reads events from DataStream
 */
public class DatastreamEventReader implements EventReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatastreamEventReader.class);
  private static final String STREAM_STATE_PAUSED = "PAUSED";
  private static final String STREAM_STATE_CREATED = "CREATED";
  private final DataStream datastream;
  private final DatastreamConfig config;
  private final DeltaSourceContext context;
  private final EventReaderDefinition definition;
  private final EventEmitter emitter;
  private final Storage storage;
  private final ScheduledExecutorService executorService;

  public DatastreamEventReader(DatastreamConfig config, EventReaderDefinition definition, DeltaSourceContext context,
    EventEmitter emitter, DataStream datastream, Storage storage) {
    this.context = context;
    this.config = config;
    this.definition = definition;
    this.emitter = emitter;
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.datastream = datastream;
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
    Stream stream = null;
    try {
      stream = datastream.projects().locations().streams().get(streamPath).execute();
    } catch (IOException e) {
      throw Utils.handleError(LOGGER, context, String.format("Failed to get stream: %s", streamPath), e);
    }

    Operation operation = null;
    if (STREAM_STATE_PAUSED.equals(stream.getState())) {
      try {
        operation = datastream.projects().locations().streams().resume(streamPath, new ResumeStreamRequest()).execute();
      } catch (IOException e) {
        throw Utils.handleError(LOGGER, context, String.format("Failed to resume stream: %s", streamPath), e);
      }
    } else if (STREAM_STATE_CREATED.equals(stream.getState())) {
      try {
        operation = datastream.projects().locations().streams().start(streamPath, new StartStreamRequest()).execute();
      } catch (IOException e) {
        throw Utils.handleError(LOGGER, context, String.format("Failed to start stream: %s", streamPath), e);
      }
    }

    Utils.waitUntilComplete(datastream, operation, LOGGER);
  }
}
