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
import com.google.api.gax.paging.Page;
import com.google.cloud.datastream.v1alpha1.DatastreamClient;
import com.google.cloud.datastream.v1alpha1.OperationMetadata;
import com.google.cloud.datastream.v1alpha1.ResumeStreamRequest;
import com.google.cloud.datastream.v1alpha1.StartStreamRequest;
import com.google.cloud.datastream.v1alpha1.Stream;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import io.cdap.delta.api.ChangeEvent;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.ReplicationError;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.datastream.util.DatastreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Reads events from DataStream
 */
public class DatastreamEventReader implements EventReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatastreamTableRegistry.class);
  private static final String DB_CREATED_STATE_KEY = "db.created";
  public static final String TIME_CREATED_STATE_KEY_SUFFIX = ".creation.time";
  public static final String SOURCE_TIME_STATE_KEY_SUFFIX = ".source.time";
  public static final String POSITION_STATE_KEY_SUFFIX = ".pos";
  public static final String PATH_STATE_KEY_SUFFIX = ".path";
  private static final String DUMP_STATE_KEY_SUFFIX = ".dumped";

  private static final int DATASTREAM_SLA_IN_MINUTES = 5;
  private static final int SCAN_INTERVAL_IN_SECONDS = 30;

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
    executorService.scheduleWithFixedDelay(new ScanTask(offset), 0, SCAN_INTERVAL_IN_SECONDS, TimeUnit.SECONDS);

  }

  public void stop() throws InterruptedException {
    executorService.shutdownNow();
    if (!executorService.awaitTermination(2, TimeUnit.MINUTES)) {
      LOGGER.warn("Unable to cleanly shutdown reader within the timeout.");
    }
  }

  private void startStreamIfNot() {
    String parentPath = DatastreamUtils.buildParentPath(config.getRegion());
    String replicatorId = DatastreamUtils.buildReplicatorId(context);
    String streamName = DatastreamUtils.buildStreamName(replicatorId);
    String streamPath = DatastreamUtils.buildStreamPath(parentPath, streamName);
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
      DatastreamUtils.waitUntilComplete(response);
    }
  }

  class ScanTask implements Runnable {
    private final HashMap<String, String> state;

    ScanTask(Offset offset) {
      this.state = new HashMap<>(offset.get());
    }
    @Override
    public void run() {
      /**

       Offset Structure:

       db.created                        → whether the db is created
       ${table_name}.dumped              →  whether the table is dumped
       ${table_name}.creation.time  → creation time of last detected cdc/dump file
       ${table_name}.path                → the path of last scanned events file of the table
       ${table_name}.pos             → the pos of last scanned event in the events file
       ${table_name}.source.time    →  the biggest source timestamp of the events scanned so far

       Algorithm:


       // Create DB
       If db.created is false :
         Generate DDL for create DB
       Offset : db.created = true

       For each table do :
       If  table.path is not in the state :
         Scan table folder to get first dump file path
         Set Offset:   table.path = current path of the dump file

       // If snapshot is not done, scan only dump file
       If table.dumped == false :
       scan all files under current folder order by time created starting from the path file
       If it’s event file skip
       If table.pos doesn’t exist:
       Generate DDL for create table
       pos = -1;
       Read events from file from pos + 1
       Generate Insert DML event , set Offset: table.pos = current pos, table.path = current path and table.timestamp
       = time created of current file

       If table.dumped is false and dump is completed
       Set Offset: table.dumped = true;

       If table.dumped is true
       scan all files starting from the table.source.timestamp or beginning :
       Start from current path
       If dump file skip
       Read events from file from pos + 1
       Generate Insert DML event , Offset: Table.pos = current pos, table.path = current path,

       *
       */

      boolean dbCreated = Boolean.parseBoolean(state.getOrDefault(DB_CREATED_STATE_KEY, "false"));
      // Generate DDL for DB creation if DB not created yet
      if (!dbCreated) {
        emitEvent(DDLEvent.builder().setDatabaseName(config.getSid()).setOperation(DDLOperation.Type.CREATE_DATABASE)
          .setSourceTimestamp(0).setSnapshot(true).build());
        state.put(DB_CREATED_STATE_KEY, "true");
      }

      // construct the map table name -> source table definition,
      // table name is prefixed with schema name if schema name is not null
      Map<String, SourceTable> tables = new HashMap<>();
      for (SourceTable table : definition.getTables()) {
        tables.put(table.getSchema() == null ? table.getTable() : table.getSchema() + "_" + table.getTable(), table);
      }

      // get the target gcs bucket
      Bucket bucket = storage.get(config.getGcsBucket());
      String replicatorPathPrefix = buildReplicatorPathPrefix();


      // Scan table by table
      for (Map.Entry<String, SourceTable> table : tables.entrySet()) {
        String tableName = table.getKey();
        SourceTable srcTable = table.getValue();

        String prefix = replicatorPathPrefix + tableName.toUpperCase() + "/";
        boolean dumped = getDumped(tableName);
        if (!dumped) {

          String path = getPath(tableName);
          long timeCreated = getTimeCreated(tableName);
          if (path == null) {
            Blob blob = getFirstFile(bucket, prefix, srcTable, true);
            if (blob == null) {
              //no dump file has been found yet
              continue;
            }
            path = blob.getName();
            savePath(tableName, path);
            saveTimeCreated(tableName, blob.getCreateTime());
            emitCreateTableDDL(srcTable);
          }
          //Scan all dump files of that table starting from the path only in current folder
          //TODO optimize the BlobListOptions to flilter only needed fields
          Page<Blob> blobs = bucket.list(Storage.BlobListOption.prefix(path.substring(0, path.lastIndexOf("/") + 1)));
          scanEvents(blobs, tableName, srcTable, true);
          // TODO use Datastream API to determine whether dump is done, below is just a workaround
          // new dump file found or no dump found was found at all, take it as dump not completed
          if (getTimeCreated(tableName) == 0 || getTimeCreated(tableName) > timeCreated) {
            continue;
          }
          // dump is finished
          saveDumped(tableName);
          removePath(tableName);
          removeTimeCreated(tableName);
          removePosition(tableName);

        }
        String sourceTime = getStartingSourceTime(getSourceTime(tableName));
        Storage.BlobListOption[] options;
        //TODO optimize the BlobListOptions to flilter only needed fields
        if (sourceTime == null) {
          options = new Storage.BlobListOption[]{Storage.BlobListOption.prefix(prefix)};
        } else {
          options = new Storage.BlobListOption[]{Storage.BlobListOption.prefix(prefix),
            Storage.BlobListOption.startOffset(prefix + sourceTime)};
        }
        scanEvents(bucket.list(options), tableName, srcTable, false);
      }
    }

    private String getStartingSourceTime(String sourceTime) {
      if (sourceTime == null) {
        return null;
      }
      SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd/HH/mm");
      formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

      try {
        return formatter.format(formatter.parse(sourceTime).getTime() - TimeUnit.MINUTES
          .toMillis(DATASTREAM_SLA_IN_MINUTES));
      } catch (ParseException e) {
        throw DatastreamUtils.handleError(
          LOGGER, context, String.format("Failed to parse date from path : %s", sourceTime), e);
      }
    }

    private void emitCreateTableDDL(SourceTable table) {
      DatastreamTableRegistry tableRegistry = new DatastreamTableRegistry(config, datastreamClient);
      TableDetail tableDetail;
      try {
         tableDetail = tableRegistry.describeTable(table.getDatabase(), table.getSchema(), table.getTable());
      } catch (TableNotFoundException e) {
        throw DatastreamUtils.handleError(LOGGER, context,
          String.format("Cannot find the table: database: %s, schema: %s, table: %s", table.getDatabase(),
            table.getSchema(), table.getTable()), e);
      } catch (IOException e) {
        throw DatastreamUtils.handleError(LOGGER, context,
          String.format("Failed to describe the table: database: %s, schema: %s, table: %s", table.getDatabase(),
            table.getSchema(), table.getTable()), e);
      }
      StandardizedTableDetail standardizedTableDetail = tableRegistry.standardize(tableDetail);
      emitEvent(DDLEvent.builder().setDatabaseName(table.getDatabase()).setTableName(table.getTable())
        .setSchemaName(table.getSchema()).setOperation(DDLOperation.Type.CREATE_TABLE)
        .setPrimaryKey(standardizedTableDetail.getPrimaryKey()).setSourceTimestamp(0L)
        .setSchema(standardizedTableDetail.getSchema()).setSnapshot(true).setOffset(new Offset(state)).build());
    }

    private void scanEvents(Page<Blob> allBlobs, String tableName, SourceTable srcTable, boolean snapshot) {
      List<BlobWrapper> blobs = new ArrayList<>();

      for (Blob blob : allBlobs.iterateAll()) {
        blobs.add(new BlobWrapper(blob));
      }
      // sort the blobs based on creation time
      blobs.sort(Comparator.<BlobWrapper>naturalOrder());
      String path = getPath(tableName);
      long timeCreated = getTimeCreated(tableName);
      long position = getPosition(tableName) + 1;
      int start = timeCreated > 0 ? Collections.binarySearch(blobs, new BlobWrapper(timeCreated, path)) : 0;

      for (int i = start; i < blobs.size(); i++) {
        BlobWrapper blob = blobs.get(i);
        // each blob can be a folder or a file
        // TODO check blob.isDirectory() before get contents
        // Currently isDirectory() doesn't work
        if (blob.getBlob().getSize() > 0) {

          path = blob.getName();
          timeCreated = blob.getTimeCreated();
          savePath(tableName, path);
          saveTimeCreated(tableName, timeCreated);

          if (!snapshot) {
            updateSourceTime(tableName,
              path.substring(path.lastIndexOf(tableName.toUpperCase()) + tableName.length() + 1,
              path.lastIndexOf(
              "/")));
          }

          DatastreamEventConsumer consumer =
            new DatastreamEventConsumer(blob.getBlob().getContent(), context, path, srcTable, position, state);
          if (consumer.isSnapshot() != snapshot) {
            continue;
          }
          while (consumer.hasNext()) {
            position = savePosition(tableName, position);

            DMLEvent event = consumer.next();
            if (!definition.getDmlBlacklist().contains(event.getOperation().getType())) {
              emitEvent(event);
            }
          }
          position = 0;
        }
      }
    }

    private long savePosition(String tableName, long position) {
      state.put(tableName + POSITION_STATE_KEY_SUFFIX, String.valueOf(position++));
      return position;
    }

    private void removePosition(String tableName) {
      state.remove(tableName + POSITION_STATE_KEY_SUFFIX);
    }

    private long getPosition(String tableName) {
      return Long.parseLong(state.getOrDefault(tableName + POSITION_STATE_KEY_SUFFIX, "-1"));
    }

    private void updateSourceTime(String tableName, String sourceTime) {
      String originalSourceTime = getSourceTime(tableName);
      if (originalSourceTime == null || originalSourceTime.compareTo(sourceTime) < 0) {
        state.put(tableName + SOURCE_TIME_STATE_KEY_SUFFIX, sourceTime);
      }
    }

    private boolean getDumped(String tableName) {
      return Boolean.parseBoolean(state.getOrDefault(tableName + DUMP_STATE_KEY_SUFFIX, "false"));
    }

    private void saveDumped(String tableName) {
      state.put(tableName + DUMP_STATE_KEY_SUFFIX, "true");
    }

    private long getTimeCreated(String tableName) {
      return Long.parseLong(state.getOrDefault(tableName + TIME_CREATED_STATE_KEY_SUFFIX, "0"));
    }
    private void saveTimeCreated(String tableName, long timeCreated) {
      state.put(tableName + TIME_CREATED_STATE_KEY_SUFFIX, String.valueOf(timeCreated));
    }

    private void removeTimeCreated(String tableName) {
      state.remove(tableName + TIME_CREATED_STATE_KEY_SUFFIX);
    }

    private void removePath(String tableName) {
      state.remove(tableName + PATH_STATE_KEY_SUFFIX);
    }

    private void savePath(String tableName, String path) {
      state.put(tableName + PATH_STATE_KEY_SUFFIX, path);
    }

    private String getPath(String tableName) {
      return state.get(tableName + PATH_STATE_KEY_SUFFIX);
    }

    private String getSourceTime(String tableName) {
      return state.getOrDefault(tableName + SOURCE_TIME_STATE_KEY_SUFFIX, null);
    }



    private Blob getFirstFile(Bucket bucket, String prefix, SourceTable table, boolean isSnapshot) {
      Page<Blob> blobs = bucket.list(Storage.BlobListOption.prefix(prefix));
      for (Blob blob : blobs.iterateAll()) {
        if (blob.getSize() > 0) {
          DatastreamEventConsumer consumer =
            new DatastreamEventConsumer(blob.getContent(), context, blob.getName(), table, 0, state);
          if (consumer.isSnapshot() == isSnapshot) {
            return blob;
          }
        }
      }
      return null;
    }

    private String buildReplicatorPathPrefix() {
      // construct the path prefix
      String streamName = DatastreamUtils.buildStreamName(DatastreamUtils.buildReplicatorId(context));
      String gcsPathPrefix = config.getGcsPathPrefix();
      // path prefix does not start with "/"
      gcsPathPrefix = gcsPathPrefix.startsWith("/") ? gcsPathPrefix.substring(1) : gcsPathPrefix;
      if (gcsPathPrefix.isEmpty()) {
        return streamName + "/";
      }
      return String.format("%s/%s/", gcsPathPrefix, streamName);
    }

    private void emitEvent(ChangeEvent event) {
      try {
        if (event instanceof DMLEvent) {
          emitter.emit((DMLEvent) event);
        } else {
          emitter.emit((DDLEvent) event);
        }
      } catch (InterruptedException e) {
        LOGGER.error("Failed to emit event : " + event.toString(), e);
        try {
          context.setError(new ReplicationError(e));
        } catch (IOException ioException) {
          LOGGER.error("Failed to set error!");
          throw new RuntimeException((ioException));
        }
        throw new RuntimeException(e);
      }
    }
  }

  // wrapper class used for sorting and binary search
  private static class BlobWrapper implements Comparable<BlobWrapper> {

    private final Blob blob;
    private final long timeCreated;
    private final String name;

    private BlobWrapper(Blob blob) {
      this.blob = blob;
      this.timeCreated = blob.getCreateTime();
      this.name = blob.getName();
    }

    public long getTimeCreated() {
      return timeCreated;
    }

    public String getName() {
      return name;
    }

    private BlobWrapper(long timeCreated, String name) {
      this.timeCreated = timeCreated;
      this.name = name;
      this.blob = null;
    }

    public Blob getBlob() {
      return blob;
    }

    @Override
    public int compareTo(BlobWrapper other) {
      if (other == null) {
        return 1;
      }

      int result = Long.compare(timeCreated, other.timeCreated);
      if (result != 0) {
        return result;
      }
      return name.compareTo(other.name);
    }
  }
}
