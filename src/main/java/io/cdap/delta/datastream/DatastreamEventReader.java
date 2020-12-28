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

import com.google.api.gax.paging.Page;
import com.google.api.services.datastream.v1alpha1.DataStream;
import com.google.api.services.datastream.v1alpha1.model.GcsProfile;
import com.google.api.services.datastream.v1alpha1.model.Operation;
import com.google.api.services.datastream.v1alpha1.model.ResumeStreamRequest;
import com.google.api.services.datastream.v1alpha1.model.StartStreamRequest;
import com.google.api.services.datastream.v1alpha1.model.Stream;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.gson.Gson;
import io.cdap.delta.api.ChangeEvent;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DDLOperation;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReader;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.datastream.util.Utils;
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

import static io.cdap.delta.datastream.DatastreamEventConsumer.POSITION_STATE_KEY_SUFFIX;

/**
 * Reads events from DataStream
 */
public class DatastreamEventReader implements EventReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatastreamEventReader.class);
  private static final Gson GSON = new Gson();
  private static final String STREAM_STATE_PAUSED = "PAUSED";
  private static final String STREAM_STATE_CREATED = "CREATED";
  private static final String DB_CREATED_STATE_KEY = "db.created";
  public static final String PROCESSED_TIME_STATE_KEY_SUFFIX = ".processed.time";
  public static final String SOURCE_TIME_STATE_KEY_SUFFIX = ".source.time";
  public static final String PATH_STATE_KEY_SUFFIX = ".path";
  private static final String DUMP_STATE_KEY_SUFFIX = ".dumped";
  private static final String SCAN_DONE_STATE_KEY_SUFFIX = ".last.done";

  // Datastream suggested 3 days scanning window
  private static final int DATASTREAM_SLA_IN_MINUTES = 60 * 24 * 3;
  private static final int SCAN_INTERVAL_IN_SECONDS = 30;

  private final DataStream datastream;
  private final DatastreamConfig config;
  private final DeltaSourceContext context;
  private final EventReaderDefinition definition;
  private final EventEmitter emitter;
  private final Storage storage;
  private final ScheduledExecutorService executorService;
  private final String streamPath;
  // The GCS bucket in which datastream result is written to
  private String bucketName;
  // The root GCS path under which datastream result is written to
  private String gcsRootPath;
  //Datastream allow each stream to specify a path prefix under which datastream result is written to
  private String streamGcsPathPrefix;

  public DatastreamEventReader(DatastreamConfig config, EventReaderDefinition definition, DeltaSourceContext context,
    EventEmitter emitter, DataStream datastream, Storage storage) {
    this.context = context;
    this.config = config;
    this.definition = definition;
    this.emitter = emitter;
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.datastream = datastream;
    this.storage = storage;
    String streamId = config.getStreamId();

    if (streamId == null || streamId.isEmpty()) {
      streamId = Utils.buildStreamName(Utils.buildReplicatorId(context));
    }
    this.streamPath = Utils.buildStreamPath(Utils.buildParentPath(config.getRegion()), streamId);

    Stream stream;
    try {
      stream = datastream.projects().locations().streams().get(streamPath).execute();
    } catch (IOException e) {
      throw Utils.handleError(LOGGER, context, "Failed to get stream : " + streamPath, e);
    }
    this.streamGcsPathPrefix = stream.getDestinationConfig().getGcsDestinationConfig().getPath();
    String gcsConnectionProfile = stream.getDestinationConfig().getDestinationConnectionProfileName();
    try {
      GcsProfile gcsProfile =
        datastream.projects().locations().connectionProfiles().get(gcsConnectionProfile).execute().getGcsProfile();
      this.bucketName = gcsProfile.getBucketName();
      this.gcsRootPath = gcsProfile.getRootPath();
    } catch (IOException e) {
      throw Utils.handleError(LOGGER, context, "Failed to get GCS connection profile : " + gcsConnectionProfile, e);
    }
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

  class ScanTask implements Runnable {
    private final HashMap<String, String> state;

    ScanTask(Offset offset) {
      this.state = new HashMap<>(offset.get());
    }
    @Override
    public void run() {
      /**
       Scanning strategy:
       Assume we have below SLO:
       Once the events with source timestamp Y finished writing , all the events happening before  Y - X mins  should
       already complete being written to GCS. And each time we only scan events happening from the source timestamp of
       the earliest event discovered in last scanning  - X mins. (Here the earliest event discovered in last scanning
       means the event with smallest source timestamp discovered in last scanning)

       Offset Structure:

       db.created                        → whether the db is created
       ${table_name}.dumped              → whether the table is dumped
       ${table_name}.processed.time      → creation time of the last processed cdc/dump file for the table
                                           those files that was created before this timestamp won't be processed again
       ${table_name}.path                → the path of last scanned events file of the table
       ${table_name}.pos                 → the position of last scanned record in the events file
       ${table_name}.source.time         → the smallest source timestamp of the events scanned so far
                                           this timestamp will be used to calculate the scanning time window for next
                                           scan. The window is "this timestamp - SLO (3 days) " to current time
       ${table_name}.last.done           → whether last scan of the table was done or not

       Algorithm:

       // Create DB
       If db.created is false :
         emit DDL for create DB
       Offset : db.created = true

       For each table do :

       // If snapshot is not done, scan only dump file
       If table.dumped == false :
         If  table.path is not in the state : // table has never been scanned even for dump file
             Scan table folders to get dump file folder
             emit DDL for create table
         scan all files under current folder order by time created starting from the path file
         If it’s event file skip
         If table.pos doesn’t exist:  pos = -1;
         set Offset: table.path = current path and table.timestamp = time created of current file
         Read events from file from pos + 1
           emit Insert DML event  (table.pos = current pos , pos++)

       If table.dumped is false and dump is completed
         Set Offset: table.dumped = true;

       If table.dumped is true
         scan all files starting from the table.source.timestamp - SLO or beginning :
         Start from current path
         If dump file skip
         If table.pos doesn’t exist:  pos = -1;
         set Offset: table.path = current path and table.timestamp = time created of current file
         Read events from file from pos + 1
           emit DML event  (table.pos = current pos , pos++)

       **/

      try {
        context.setOK();
      } catch (IOException e) {
        LOGGER.warn("Unable to set source state to OK.", e);
      }

      boolean dbCreated = Boolean.parseBoolean(state.getOrDefault(DB_CREATED_STATE_KEY, "false"));
      // Emit DDL for DB creation if DB not created yet
      if (!dbCreated) {
        emitEvent(DDLEvent.builder().setDatabaseName(config.getSid()).setOperation(DDLOperation.Type.CREATE_DATABASE)
          .setSourceTimestamp(0).setSnapshot(true).build());
        state.put(DB_CREATED_STATE_KEY, "true");
      }

      // construct the map table name -> source table definition,
      // table name is prefixed with schema name if schema name is not null
      Map<String, SourceTable> tables = new HashMap<>();
      for (SourceTable table : definition.getTables()) {
        tables.put(Utils.buildCompositeTableName(table.getSchema(), table.getTable()), table);
      }

      // get the target gcs bucket
      Bucket bucket = storage.get(bucketName);
      String replicatorPathPrefix = buildReplicatorPathPrefix();

      // Scan table by table
      for (Map.Entry<String, SourceTable> table : tables.entrySet()) {
        String tableName = table.getKey();
        SourceTable srcTable = table.getValue();

        // the GCS path prefix for the current scanned table
        String prefix = replicatorPathPrefix + tableName.toUpperCase() + "/";
        // check whether the snapshot of the table has been done
        boolean dumped = getDumped(tableName);
        if (!dumped) {
          // if the dump file of the table was ever scanned, path should have value
          String path = getPath(tableName);
          long lastProcessed = getLastProcessed(tableName);
          String dumpFilePathPrefix;
          if (path == null) {
            // get the dump file folder, dump files should be in the same folder
            // becasue dump files are put in the folder that represents the read time
            dumpFilePathPrefix = getDumpFilePathPrefix(bucket, prefix, srcTable);
            if (dumpFilePathPrefix == null) {
              //no dump file was ever found yet
              continue;
            }
            emitCreateTableDDL(srcTable);
          } else {
            dumpFilePathPrefix = path.substring(0, path.lastIndexOf("/") + 1);
          }
          //Scan all dump files of that table starting from the path only in current folder
          Page<Blob> blobs = bucket.list(Storage.BlobListOption.prefix(dumpFilePathPrefix), Storage.BlobListOption
            .fields(Storage.BlobField.NAME, Storage.BlobField.TIME_CREATED, Storage.BlobField.SIZE));
          scanEvents(blobs, tableName, srcTable, true);
          // TODO use Datastream API to determine whether dump is done, below is just a workaround
          // if new dump file found  then that means dump hasn't been completed
          if (!getPath(tableName).equals(path)) {
            continue;
          }
          // dump is finished
          saveDumped(tableName);
          removePath(tableName);
          removeTimeCreated(tableName);
          clearPosition(tableName);

        }

        // scanning window starts from ${table_name}.source.time - SLO
        String startTime = getStartingSourceTime(getSourceTime(tableName));
        ArrayList<Storage.BlobListOption> listOptions = new ArrayList<>(3);
        listOptions.add(Storage.BlobListOption.prefix(prefix));
        listOptions.add(Storage.BlobListOption.fields(Storage.BlobField.NAME, Storage.BlobField.TIME_CREATED,
          Storage.BlobField.SIZE));
        if (startTime != null) {
          listOptions.add(Storage.BlobListOption.startOffset(prefix + startTime));
        }
        scanEvents(bucket.list(listOptions.toArray(new Storage.BlobListOption[listOptions.size()])), tableName,
          srcTable, false);
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
        throw Utils.handleError(
          LOGGER, context, String.format("Failed to parse date from : %s", sourceTime), e);
      }
    }

    private void emitCreateTableDDL(SourceTable table) {
      DatastreamTableRegistry tableRegistry = new DatastreamTableRegistry(config, datastream);
      TableDetail tableDetail;
      try {
         tableDetail = tableRegistry.describeTable(table.getDatabase(), table.getSchema(), table.getTable());
      } catch (TableNotFoundException e) {
        throw Utils.handleError(LOGGER, context,
          String.format("Cannot find the table: database: %s, schema: %s, table: %s", table.getDatabase(),
            table.getSchema(), table.getTable()), e);
      } catch (IOException e) {
        throw Utils.handleError(LOGGER, context,
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
      long lastProcessed = getLastProcessed(tableName);
      long position = getPosition(tableName) + 1;
      int start = lastProcessed > 0 ? Collections.binarySearch(blobs, new BlobWrapper(lastProcessed, path)) : 0;
      if (getLastScanDone(tableName)) {
        setLastScanDone(tableName, false);
        // clear the source time before new scan
        clearSourceTime(tableName);
      }

      for (int i = start; i < blobs.size(); i++) {
        BlobWrapper blob = blobs.get(i);
        // each blob can be a folder or a file
        if (blob.getBlob().getSize() > 0) {

          path = blob.getName();
          lastProcessed = blob.getTimeCreated();
          savePath(tableName, path);
          saveTimeCreated(tableName, lastProcessed);

          if (!snapshot) {
            updateSourceTime(tableName, parseSourceTime(tableName, path));
          }

          DatastreamEventConsumer consumer =
            new DatastreamEventConsumer(blob.getBlob().getContent(), context, path, srcTable, position, state);
          if (consumer.isSnapshot() != snapshot) {
            continue;
          }
          while (consumer.hasNextEvent()) {
            DMLEvent event = consumer.nextEvent();
            //worker level DML blacklist
            if (!definition.getDmlBlacklist().contains(event.getOperation().getType())) {
              emitEvent(event);
            }
          }
          position = 0;
        }
      }
      setLastScanDone(tableName, true);
    }

    private String parseSourceTime(String tableName, String path) {
      return path.substring(path.lastIndexOf(tableName.toUpperCase()) + tableName.length() + 1, path.lastIndexOf("/"));
    }

    private void clearPosition(String tableName) {
      state.remove(tableName + POSITION_STATE_KEY_SUFFIX);
    }

    private long getPosition(String tableName) {
      return Long.parseLong(state.getOrDefault(tableName + POSITION_STATE_KEY_SUFFIX, "-1"));
    }

    private boolean getLastScanDone(String tableName) {
      return Boolean.parseBoolean(state.getOrDefault(tableName + SCAN_DONE_STATE_KEY_SUFFIX, "false"));
    }

    private void setLastScanDone(String tableName, boolean done) {
      state.put(tableName + SCAN_DONE_STATE_KEY_SUFFIX, String.valueOf(done));
    }

    private void clearSourceTime(String tableName) {
      state.remove(tableName + SOURCE_TIME_STATE_KEY_SUFFIX);
    }
    private void updateSourceTime(String tableName, String sourceTime) {
      String originalSourceTime = getSourceTime(tableName);
      // set the minimal source time of this scan
      if (originalSourceTime == null || originalSourceTime.compareTo(sourceTime) > 0) {
        state.put(tableName + SOURCE_TIME_STATE_KEY_SUFFIX, sourceTime);
      }
    }

    private boolean getDumped(String tableName) {
      return Boolean.parseBoolean(state.getOrDefault(tableName + DUMP_STATE_KEY_SUFFIX, "false"));
    }

    private void saveDumped(String tableName) {
      state.put(tableName + DUMP_STATE_KEY_SUFFIX, "true");
    }

    private long getLastProcessed(String tableName) {
      return Long.parseLong(state.getOrDefault(tableName + PROCESSED_TIME_STATE_KEY_SUFFIX, "0"));
    }
    private void saveTimeCreated(String tableName, long timeCreated) {
      state.put(tableName + PROCESSED_TIME_STATE_KEY_SUFFIX, String.valueOf(timeCreated));
    }

    private void removeTimeCreated(String tableName) {
      state.remove(tableName + PROCESSED_TIME_STATE_KEY_SUFFIX);
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

    private String getDumpFilePathPrefix(Bucket bucket, String prefix, SourceTable table) {
      Page<Blob> blobs = bucket.list(Storage.BlobListOption.prefix(prefix), Storage.BlobListOption.fields(
        Storage.BlobField.SIZE, Storage.BlobField.NAME));
      for (Blob blob : blobs.iterateAll()) {
        if (blob.getSize() > 0) {
          String path = blob.getName();
          DatastreamEventConsumer consumer =
            new DatastreamEventConsumer(blob.getContent(), context, path, table, 0, state);
          if (consumer.isSnapshot()) {
            return path.substring(0, path.lastIndexOf("/") + 1);
          }
        }
      }
      return null;
    }

    private String buildReplicatorPathPrefix() {
      // remove heading '/'
      gcsRootPath = gcsRootPath.startsWith("/") ? gcsRootPath.substring(1) : gcsRootPath;
      streamGcsPathPrefix = streamGcsPathPrefix.startsWith("/") ? streamGcsPathPrefix.substring(1) :
        streamGcsPathPrefix;
      if (gcsRootPath.isEmpty()) {
        return streamGcsPathPrefix + "/";
      }
      return String.format("%s/%s/", gcsRootPath, streamGcsPathPrefix);
    }

    private void emitEvent(ChangeEvent event) {
      LOGGER.error("###SEAN emit event: " + GSON.toJson(event));
      try {
        if (event instanceof DMLEvent) {
          emitter.emit((DMLEvent) event);
        } else {
          emitter.emit((DDLEvent) event);
        }
      } catch (InterruptedException e) {
        Utils.handleError(LOGGER, context, "Failed to emit event : " + GSON.toJson(event), e);
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
