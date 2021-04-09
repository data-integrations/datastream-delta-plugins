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
import com.google.cloud.datastream.v1alpha1.DatastreamClient;
import com.google.cloud.datastream.v1alpha1.GcsProfile;
import com.google.cloud.datastream.v1alpha1.Stream;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.schema.Schema;
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
 * Reads events from DatastreamClient
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
  private static final String DUMP_STATE_KEY_SUFFIX = ".snapshot.done";
  private static final String SCAN_DONE_STATE_KEY_SUFFIX = ".last.done";
  private static final String TABLE_DETAIL_STATE_KEY_SUFFIX = ".table.detail";
  private static final String SCHEMA_KEY_STATE_KEY_SUFFIX = ".schema.key";

  // Datastream suggested 3 days scanning window
  private static final int DATASTREAM_SLA_IN_MINUTES = 60 * 24 * 3;
  private static final int SCAN_INTERVAL_IN_SECONDS = 30;

  private final DatastreamClient datastream;
  private final DatastreamConfig config;
  private final DeltaSourceContext context;
  private final EventReaderDefinition definition;
  private final EventEmitter emitter;
  private final Storage storage;
  private final ScheduledExecutorService executorService;
  private final String streamPath;
  //Datastream allow each stream to specify a path prefix under which datastream result is written to
  private final String streamGcsPathPrefix;
  // The root GCS path under which datastream result is written to
  private final String gcsRootPath;
  // The GCS bucket in which datastream result is written to
  private final String bucketName;
  private final String databaseName;
  private final Stream stream;

  public DatastreamEventReader(DatastreamConfig config, EventReaderDefinition definition, DeltaSourceContext context,
    EventEmitter emitter, DatastreamClient datastream, Storage storage) throws Exception {
    this.context = context;
    this.config = config;
    this.definition = definition;
    this.emitter = emitter;
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.datastream = datastream;
    this.storage = storage;
    String streamId =
      config.isUsingExistingStream() ? config.getStreamId() : Utils.buildStreamName(Utils.buildReplicatorId(context));
    this.streamPath = Utils.buildStreamPath(Utils.buildParentPath(config.getProject(), config.getRegion()), streamId);

    //TODO optimize below logic to get information from config if not using existing stream
    try {
      this.stream = Utils.getStream(datastream, streamPath, LOGGER);
    } catch (Exception e) {
      throw Utils.buildException("Failed to get stream : " + streamPath, e, true);
    }
    String oracleProfileName = this.stream.getSourceConfig().getSourceConnectionProfileName();
    try {
      this.databaseName =
        Utils.getConnectionProfile(datastream, oracleProfileName, LOGGER).getOracleProfile().getDatabaseService();
    } catch (Exception e) {
      throw Utils
        .buildException("Failed to get oracle connection profile : " + oracleProfileName, e, true);
    }
    String path = this.stream.getDestinationConfig().getGcsDestinationConfig().getPath();
    this.streamGcsPathPrefix = path == null ? "" : path.startsWith("/") ? path.substring(1) : path;
    String gcsProfileName = this.stream.getDestinationConfig().getDestinationConnectionProfileName();
    try {
      GcsProfile gcsProfile = Utils.getConnectionProfile(datastream, gcsProfileName, LOGGER).getGcsProfile();
      this.bucketName = gcsProfile.getBucketName();
      path = gcsProfile.getRootPath();
      this.gcsRootPath = path.startsWith("/") ? path.substring(1) : path;
    } catch (Exception e) {
      throw Utils.buildException("Failed to get GCS connection profile : " + gcsProfileName, e, true);
    }
  }

  @Override
  public void start(Offset offset) {

    try {
      startStreamIfNot();
    } catch (Throwable e) {
      context.notifyFailed(e);
      return;
    }
    executorService.scheduleAtFixedRate(new ScanTask(offset), 0, SCAN_INTERVAL_IN_SECONDS, TimeUnit.SECONDS);

  }

  public void stop() throws InterruptedException {
    executorService.shutdownNow();
    // TODO change the DeltaWoker to send some signal about whether such stop is for restarting due to
    // temporary failure or it's stopped by the end user
    // if it's not stopped by the end user , we should not stop the stream because other workers are still working
    // below is just a workaround to avoid changes in delta app.
    // if the caller is from DeltaWorker.run it's a stop due to failure and don't need to stop stream
    // otherwise it's from DeltaWorker.stop which is intended by the end user.
    if (!new Throwable().getStackTrace()[1].getMethodName().startsWith("lambda$run")) {
      Utils.pauseStream(datastream, stream, LOGGER);
    }
    if (!executorService.awaitTermination(2, TimeUnit.MINUTES)) {
      LOGGER.warn("Unable to cleanly shutdown reader within the timeout.");
    }
  }

  private void startStreamIfNot() throws Exception {
    // start the stream if not
    if (Stream.State.RUNNING != stream.getState()) {
      Utils.startStream(datastream, stream, LOGGER);
    }
  }

  class ScanTask implements Runnable {
    private final Map<String, String> state;
    private final Map<String, StandardizedTableDetail> tableDetails;

    ScanTask(Offset offset) {
      this.state = new HashMap<>(offset.get());
      this.tableDetails = new HashMap<>();
    }

    @Override
    public void run() {
      try {
        scanResults();
      } catch (Throwable t) {
        Utils.handleError(LOGGER, context, t);
        context.notifyFailed(t);
      }
    }

    private void scanResults() throws Exception {
      /**
       Scanning strategy:
       Assume we have below SLO:
       Once the events with source timestamp Y finished writing , all the events happening before  Y - X mins  should
       already complete being written to GCS. And each time we only scan events happening from the source timestamp of
       the earliest event discovered in last scanning  - X mins. (Here the earliest event discovered in last scanning
       means the event with smallest source timestamp discovered in last scanning)

       Offset Structure:

       db.created                        → whether the db is created
       ${table_name}.snapshot.done       → whether the table is dumped
       ${table_name}.processed.time      → creation time of the last processed cdc/dump file for the table
                                           those files that was created before this timestamp won't be processed again
       ${table_name}.path                → the path of last scanned events file of the table
       ${table_name}.pos                 → the position of last scanned record in the events file
       ${table_name}.source.time         → the smallest source timestamp of the events scanned so far
       this timestamp will be used to calculate the scanning time window for next
       scan. The window is "this timestamp - SLO (3 days) " to current time
       ${table_name}.last.done           → whether last scan of the table was done or not


       ${table_name}.table.detail        → the the latest cdap table detail commited on the target, if it changes , then
       generate a DDL event. It's possible source table schema is changed while cdap
       schema doesn't need to change due to :
       a. source column data type changed but it's still mapped to the same
       cdap data type
       b. datastream doesn't generate separate DDL, we can only query the latest
       source table schema, when we were processing the old change events, we
       already got the latest table schema.
       ${table_name}.schema.key          → the schema key of latest events (the greatest source timestamp) seen when
       the schema hash was recorded. Each datastream result file is attached with
       a schema key, this key gives a hint about whether there is a schema change
       in the source. If this key changes, we query the actual source table
       schema to see whether we need to emit a DDL
       Algorithm:

       // Create DB
       If db.created is false :
       emit DDL for create DB
       Offset : db.created = true

       For each table do :

       // If snapshot is not done, scan only dump file
       If table.snapshot.done == false :
       If  table.path is not in the state : // table has never been scanned even for dump file
       Scan table folders to get dump file folder
       emit DDL for create table
       scan all files under current folder order by time created starting from the path file
       If it’s event file skip
       If table.pos doesn’t exist:  pos = -1;
       set Offset: table.path = current path and table.timestamp = time created of current file
       Read events from file from pos + 1
       emit Insert DML event  (table.pos = current pos , pos++)

       If table.snapshot.done is false and dump is completed
       Set Offset: table.snapshot.done = true;

       If table.snapshot.done is true
       scan all files starting from the table.source.timestamp - SLO or beginning :
       Start from current path
       If dump file skip
       If table.pos doesn’t exist:  pos = -1;
       set Offset: table.path = current path and table.timestamp = time created of current file
       Read events from file from pos + 1
       emit DML event  (table.pos = current pos , pos++)
       **/
      if (!Stream.State.RUNNING.equals(stream.getState())) {
        LOGGER.warn("Stream {} is in status : {}.", streamPath, stream.getState());
      } else {
        Exception error = null;
        try {
          error = Utils.fetchErrors(datastream, streamPath, LOGGER, context);
        } catch (Exception e) {
          LOGGER.warn("Failed to fetch errors for stream " + streamPath, e);
        }
        if (error != null) {
          throw error;
        }

        try {
          context.setOK();
        } catch (IOException e) {
          LOGGER.warn("Unable to set source state to OK.", e);
        }
      }

      boolean dbCreated = Boolean.parseBoolean(state.getOrDefault(DB_CREATED_STATE_KEY, "false"));
      // Emit DDL for DB creation if DB not created yet
      if (!dbCreated) {
        emitEvent(DDLEvent.builder().setDatabaseName(databaseName).setOperation(DDLOperation.Type.CREATE_DATABASE)
          .setSourceTimestamp(0).setSnapshot(true).setOffset(new Offset(state)).build());
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
          if (stream.hasBackfillNone()) {
            //snapshot is skipped
            emitCreateTableDDL(tableName, srcTable, null);
          } else {
            // if the dump file of the table was ever scanned, path should have value
            String path = getPath(tableName);
            long lastProcessed = getLastProcessed(tableName);

            // dump files are put in the folder that represents the read time
            // don't assume dump files are under one folder, because dump can be read multiple times
            // each time to replicate part of the dump
            // Scan all files of that table , because events may arrive out of order, don't assume
            // once we see dump files in a later timestamp folder, we don't need to scan earlier timestamp folder
            Page<Blob> blobs = bucket.list(Storage.BlobListOption.prefix(prefix), Storage.BlobListOption
              .fields(Storage.BlobField.NAME, Storage.BlobField.TIME_CREATED, Storage.BlobField.SIZE));

            scanEvents(blobs, tableName, srcTable, true, path != null);
            // TODO use Datastream API to determine whether dump is done, below is just a workaround
            // if new dump file found  then that means dump hasn't been completed
            if (getPath(tableName) == null || !getPath(tableName).equals(path)) {
              continue;
            }
          }
          // dump is finished
          dumpCompleted(tableName);
        }

        // scanning window starts from ${table_name}.source.time - SLO
        String startTime = getStartingSourceTime(getSourceTime(tableName));
        ArrayList<Storage.BlobListOption> listOptions = new ArrayList<>(3);
        listOptions.add(Storage.BlobListOption.prefix(prefix));
        listOptions.add(Storage.BlobListOption
          .fields(Storage.BlobField.NAME, Storage.BlobField.TIME_CREATED, Storage.BlobField.SIZE));
        if (startTime != null) {
          listOptions.add(Storage.BlobListOption.startOffset(prefix + startTime));
        }
        scanEvents(bucket.list(listOptions.toArray(new Storage.BlobListOption[listOptions.size()])), tableName,
          srcTable, false, true);
      }
    }

    private void dumpCompleted(String tableName) {
      saveDumped(tableName);
      removePath(tableName);
      removeTimeCreated(tableName);
      clearPosition(tableName);
    }

    private String getStartingSourceTime(String sourceTime) throws Exception {
      if (sourceTime == null) {
        return null;
      }
      SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd/HH/mm");
      formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

      try {
        return formatter
          .format(formatter.parse(sourceTime).getTime() - TimeUnit.MINUTES.toMillis(DATASTREAM_SLA_IN_MINUTES));
      } catch (ParseException e) {
        throw Utils.buildException(String.format("Failed to parse date from : %s", sourceTime), e, false);
      }
    }

    private void emitCreateTableDDL(String tableName, SourceTable table, String schemaKey) throws Exception {
      StandardizedTableDetail tableDetail = getStandardizedTableDetail(table);
      DDLEvent event = DDLEvent.builder().setDatabaseName(table.getDatabase()).setTableName(table.getTable())
        .setSchemaName(table.getSchema()).setOperation(DDLOperation.Type.CREATE_TABLE)
        .setPrimaryKey(tableDetail.getPrimaryKey()).setSourceTimestamp(0L).setSchema(tableDetail.getSchema())
        .setSnapshot(true).setOffset(new Offset(state)).build();
      tableDetail.getSchema();
      // for dump file we just record the shcema key of itself
      saveSchemaKey(tableName, schemaKey);
      saveTableDetail(tableName, tableDetail);
      tableDetails.put(tableName, tableDetail);
      emitEvent(event);
    }

    private StandardizedTableDetail getStandardizedTableDetail(SourceTable table) throws Exception {
      DatastreamTableRegistry tableRegistry = new DatastreamTableRegistry(config, datastream);
      TableDetail tableDetail;
      try {
        tableDetail = tableRegistry.describeTable(table.getDatabase(), table.getSchema(), table.getTable());
      } catch (TableNotFoundException e) {
        throw Utils.buildException(String
          .format("Cannot find the table: database: %s, schema: %s, table: %s", table.getDatabase(), table.getSchema(),
            table.getTable()), e, true);
      } catch (IOException e) {
        throw Utils.buildException(String
          .format("Failed to describe the table: database: %s, schema: %s, table: %s", table.getDatabase(),
            table.getSchema(), table.getTable()), e, true);
      }
      StandardizedTableDetail standardizedTableDetail = tableRegistry.standardize(tableDetail);
      return standardizedTableDetail;
    }

    private void scanEvents(Page<Blob> allBlobs, String tableName, SourceTable srcTable, boolean snapshot,
      boolean createTableDDLEmitted) throws Exception {
      List<BlobWrapper> blobs = new ArrayList<>();

      for (Blob blob : allBlobs.iterateAll()) {
        // each blob can be a folder or a file
        if (blob.getSize() > 0 && DatastreamEventConsumer.isSnapshot(blob.getName()) == snapshot) {
          blobs.add(new BlobWrapper(blob));
        }
      }
      if (blobs.isEmpty()) {
        // it's possible there are no snapshot files yet
        return;
      }

      if (!createTableDDLEmitted) {
        emitCreateTableDDL(tableName, srcTable, parseSchemaKey(blobs.get(0).getName()));
      }

      // get the schema key of the latest seen schema file
      Schema schema = updateTableSchema(tableName, srcTable, parseSchemaKey(blobs.get(blobs.size() - 1).getName()));
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
        path = blob.getName();
        lastProcessed = blob.getTimeCreated();
        savePath(tableName, path);
        saveTimeCreated(tableName, lastProcessed);

        if (!snapshot) {
          updateSourceTime(tableName, parseSourceTime(tableName, path));
        }

        DatastreamEventConsumer consumer =
          new DatastreamEventConsumer(blob.getBlob().getContent(), context, path, srcTable, position, state, schema);
        position = 0;
        while (consumer.hasNextEvent()) {
          DMLEvent event = consumer.nextEvent();
          //worker level DML blacklist
          if (!definition.getDmlBlacklist().contains(event.getOperation().getType())) {
            emitEvent(event);
          }
        }
      }
      setLastScanDone(tableName, true);
    }

    private String parseSchemaKey(String path) {
      int lastSlashPosition = path.lastIndexOf("/");
      return path.substring(lastSlashPosition + 1, path.indexOf("_", lastSlashPosition));
    }

    private Schema updateTableSchema(String tableName, SourceTable table, String schemaKey) throws Exception {
      StandardizedTableDetail tableDetail = tableDetails.computeIfAbsent(tableName, n -> getTableDetail(n));
      if (schemaKey.equals(getSchemaKey(tableName))) {
        // no schema changes
        return tableDetail.getSchema();
      }

      StandardizedTableDetail latestTableDetail = getStandardizedTableDetail(table);

      if (latestTableDetail.equals(tableDetail)) {
        return tableDetail.getSchema();
      }

      tableDetail = latestTableDetail;
      // has schema changes
      saveSchemaKey(tableName, schemaKey);
      saveTableDetail(tableName, tableDetail);
      emitEvent(DDLEvent.builder().setOffset(new Offset(state)).setSnapshot(false).setSchema(tableDetail.getSchema())
        .setSchemaName(tableDetail.getSchemaName()).setDatabaseName(tableDetail.getDatabase())
        .setSourceTimestamp(System.currentTimeMillis()).setTableName(tableDetail.getTable())
        .setOperation(DDLOperation.Type.ALTER_TABLE).setPrimaryKey(tableDetail.getPrimaryKey()).build());
      return tableDetail.getSchema();
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

    private void saveTableDetail(String tableName, StandardizedTableDetail tableDetail) {
      state.put(tableName + TABLE_DETAIL_STATE_KEY_SUFFIX, GSON.toJson(tableDetail));
    }

    private StandardizedTableDetail getTableDetail(String tableName) {
      return GSON.fromJson(state.get(tableName + TABLE_DETAIL_STATE_KEY_SUFFIX), StandardizedTableDetail.class);
    }

    private void saveSchemaKey(String tableName, String key) {
      if (key != null) {
        state.put(tableName + SCHEMA_KEY_STATE_KEY_SUFFIX, key);
      }
    }

    private String getSchemaKey(String tableName) {
      return state.get(tableName + SCHEMA_KEY_STATE_KEY_SUFFIX);
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
      return state.get(tableName + SOURCE_TIME_STATE_KEY_SUFFIX);
    }

    private String buildReplicatorPathPrefix() {
      if (gcsRootPath.isEmpty() && streamGcsPathPrefix.isEmpty()) {
        return "";
      }
      if (gcsRootPath.isEmpty()) {
        return streamGcsPathPrefix + "/";
      }
      if (streamGcsPathPrefix.isEmpty()) {
        return gcsRootPath + "/";
      }
      return String.format("%s/%s/", gcsRootPath, streamGcsPathPrefix);
    }

    private void emitEvent(ChangeEvent event) throws Exception {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Emitting event: " + GSON.toJson(event));
      }
      try {
        if (event instanceof DMLEvent) {
          emitter.emit((DMLEvent) event);
        } else {
          emitter.emit((DDLEvent) event);
        }
      } catch (Exception e) {
        throw Utils.buildException("Failed to emit event : " + GSON.toJson(event), e, true);
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
