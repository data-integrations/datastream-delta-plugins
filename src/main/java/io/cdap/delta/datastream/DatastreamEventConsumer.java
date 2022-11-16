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
effe
package io.cdap.delta.datastream;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.SortKey;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.datastream.util.Utils;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * DatastreamEventConsumer consumes the datastream cdc events and generate CDAP Delta CDC events
 */
public class DatastreamEventConsumer {

  public static final String POSITION_STATE_KEY_SUFFIX = ".pos";
  private static final Logger LOGGER = LoggerFactory.getLogger(DatastreamEventConsumer.class);
  private static final String SNAPSHOT_FILE_NAME = "backfill";
  private static final String ROW_ID_FIELD_NAME = "row_id";
  private static final String SOURCE_METADATA_FIELD_NAME = "source_metadata";
  private static final String CHANGE_TYPE_FIELD_NAME = "change_type";
  private static final String TRANSACTION_ID_FIELD_NAME = "tx_id";
  private static final String SOURCE_TIMESTAMP_FIELD_NAME = "source_timestamp";
  private static final String PAYLOAD_FIELD_NAME = "payload";
  private static final String SORT_KEYS_FIELD_NAME = "sort_keys";
  private static final String CHANGE_TYPE_UPDATE_DELETE = "UPDATE-DELETE";
  private static final String CHANGE_TYPE_UPDATE_INSERT = "UPDATE-INSERT";
  private final byte[] content;
  private final DeltaSourceContext context;
  private final String currentPath;
  private final DataFileReader<GenericRecord> dataFileReader;
  private final SourceTable table;
  private final Map<String, String> state;
  private final String tableName;
  private Schema schema;
  private long position;
  private GenericRecord record;
  private DMLEvent event;
  private final boolean isSnapshot;

  public DatastreamEventConsumer(byte[] content, DeltaSourceContext context, String currentPath, SourceTable table,
    long startingPosition, Map<String, String> state) throws Exception {
    this (content, context, currentPath, table, startingPosition, state, null);
  }

  public DatastreamEventConsumer(byte[] content, DeltaSourceContext context, String currentPath, SourceTable table,
    long startingPosition, Map<String, String> state, Schema schema) throws Exception {
    this.content = content;
    this.context = context;
    this.currentPath = currentPath;
    this.table = table;
    this.tableName = Utils.buildCompositeTableName(table.getSchema(), table.getTable());
    this.position = startingPosition;
    this.dataFileReader = parseContent();
    this.schema = schema;
    this.state = state;
    this.isSnapshot = isSnapshot(currentPath);
  }

  private Schema parseSchema(org.apache.avro.Schema schema) throws Exception {
    org.apache.avro.Schema.Field payload = schema.getField(PAYLOAD_FIELD_NAME);
    if (payload == null) {
      throw Utils.buildException("Failed to get payload schema from schema : " + schema, false);
    }
    org.apache.avro.Schema payloadSchema = payload.schema();
    Set<String> columns = table.getColumns().stream().map(column -> column.getName()).collect(Collectors.toSet());
    Stream<org.apache.avro.Schema.Field> fields = columns.isEmpty() ? payloadSchema.getFields().stream() :
      payloadSchema.getFields().stream().filter(field -> columns.contains(field.name()));
    return Schema
      .recordOf(payloadSchema.getName(), fields.map(DatastreamEventConsumer::convert).toArray(Schema.Field[]::new));
  }

  private static Schema.Field convert(org.apache.avro.Schema.Field field) {
    return Schema.Field.of(field.name(), convert(field.schema()));
  }

  /**
   * Convert Datastream AVRO schema to CDAP schema, they are almost same except:
   * 1. Datastream has a custom logical type "varchar" we will convert it to String
   * 2. Datastream has a custom logical type "number" we will convert it to String
   */
  @VisibleForTesting
  static Schema convert(org.apache.avro.Schema schema) {
    String logicalType = schema.getLogicalType() == null ? null : schema.getLogicalType().getName();

    switch (schema.getType()) {
      case RECORD:
        return Schema.recordOf(schema.getName(),
          schema.getFields().stream().map(DatastreamEventConsumer::convert).toArray(Schema.Field[]::new));
      case INT:
        if ("date".equals(logicalType)) {
          return Schema.of(Schema.LogicalType.DATE);
        }
        if ("time-millis".equals(logicalType)) {
          return Schema.of(Schema.LogicalType.TIME_MILLIS);
        }
        return Schema.of(Schema.Type.INT);
      case LONG:
        if ("time-micros".equals(logicalType)) {
          return Schema.of(Schema.LogicalType.TIME_MICROS);
        }
        if ("timestamp-millis".equals(logicalType)) {
          return Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS);
        }
        if ("timestamp-micros".equals(logicalType)) {
          return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
        }
        if ("local-timestamp-millis".equals(logicalType)) {
          return Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS);
        }
        if ("local-timestamp-micros".equals(logicalType)) {
          return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
        }
        return Schema.of(Schema.Type.LONG);
      case NULL:
        return Schema.of(Schema.Type.NULL);
      case FLOAT:
        return Schema.of(Schema.Type.FLOAT);
      case DOUBLE:
        return Schema.of(Schema.Type.DOUBLE);
      case BYTES:
      case FIXED:
        if ("decimal".equals(logicalType)) {
          return Schema.decimalOf((Integer) schema.getObjectProp("precision"), (Integer) schema.getObjectProp("scale"));
        }
        return Schema.of(Schema.Type.BYTES);
      case STRING:
        return Schema.of(Schema.Type.STRING);
      case BOOLEAN:
        return Schema.of(Schema.Type.BOOLEAN);
      case MAP:
        return Schema.mapOf(Schema.of(Schema.Type.STRING), convert(schema.getValueType()));
      case ENUM:
        return Schema.enumWith(schema.getEnumSymbols());
      case ARRAY:
        return Schema.arrayOf(convert(schema.getElementType()));
      case UNION:
        return Schema.unionOf(schema.getTypes().stream().map(DatastreamEventConsumer::convert).toArray(Schema[]::new));
      default:
        // should not reach here
        throw new IllegalArgumentException("Unsupported type : " + schema.getType());
    }
  }

  private DataFileReader<GenericRecord> parseContent() throws Exception {
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    SeekableByteArrayInput input = new SeekableByteArrayInput(content);
    DataFileReader<GenericRecord> reader = null;
    try {
       reader = new DataFileReader<>(input, datumReader);
    } catch (IOException e) {
      throw Utils.buildException("Failed to read or parse file : " + currentPath, e, true);
    }
    for (int i = 0; i < position; i++) {
      if (reader.hasNext()) {
        reader.next();
      } else {
        break;
      }
    }
    return reader;
  }

  /**
   * Check whether the given GCS file is a snapshot file
   * @param path the GCS path of the file
   * @return whether the content being read is from a snapshot
   */
  public static boolean isSnapshot(String path) {
    return path.contains(SNAPSHOT_FILE_NAME);
  }

  /**
   * @return whether there is more event available to be read
   */
  public boolean hasNextEvent() throws Exception {
    // lazy fetch
    if (event == null) {
      fetchNext();
    }
    return event != null;
  }

  private void fetchNext() throws Exception {
    while (dataFileReader.hasNext()) {
      record = dataFileReader.next();
      savePosition();
      GenericRecord sourceMetadata = (GenericRecord) record.get(SOURCE_METADATA_FIELD_NAME);
      DMLOperation.Type operationType = DMLOperation.Type.INSERT;

      if (!isSnapshot) {
        // if it's not snapshot , get the actual event type
        String changeType = sourceMetadata.get(CHANGE_TYPE_FIELD_NAME).toString();
        operationType = getOperationType(changeType);
        // if the event type is in the black list , skip this event
        if (table.getDmlBlacklist().contains(operationType)) {
          continue;
        }
      }
      GenericRecord payload = (GenericRecord) record.get(PAYLOAD_FIELD_NAME);
      StructuredRecord row = parseRecord(payload);

      DMLEvent.Builder eventBuilder =
        DMLEvent.builder().setDatabaseName(table.getDatabase()).setSchemaName(table.getSchema())
          .setTableName(table.getTable()).setIngestTimestamp(System.currentTimeMillis()).setOperationType(operationType)
          .setRow(row).setRowId(sourceMetadata.get(ROW_ID_FIELD_NAME).toString()).setTransactionId(
          sourceMetadata.get(TRANSACTION_ID_FIELD_NAME) == null ? null :
            sourceMetadata.get(TRANSACTION_ID_FIELD_NAME).toString()).setSnapshot(isSnapshot)
          .setSourceTimestamp((Long) record.get(SOURCE_TIMESTAMP_FIELD_NAME)).setOffset(new Offset(state))
                .setSortKeys(getSortKeys(record));

      // The datastream doesn't provide with previous row details, But we need to add the previous row details in order
      // to support merging with Primary key. we can set current row as previous row is because we assume primary key
      // is not changed for UPDATE event(if PK is changed, datastream will generate an UPDATE_DELETE and UPDATE_INSERT)
      if (operationType == DMLOperation.Type.UPDATE) {
        eventBuilder.setPreviousRow(row);
      }

      event = eventBuilder.build();
      return;
    }
  }

  /**
   * Gets the list of {@link SortKey} from the record.
   * <p>
   * Sort keys is a list of values that each DML record
   * can be sorted by in the order that the comparison
   * should be performed
   * <p>
   * Datastream generates sort keys of String/Long types
   * "sort_keys":{"elementType":["string","long"],"type":"array"}
   * <p>
   * For oracle - [source_timestamp (Long), scn (Long), rs_id (String), ssn (Long)]
   * e.g. [1609071865000, 680024, '0x000013.00000032.004c', 5]
   *
   * @param record Datastream record
   * @return List of {@link SortKey}
   */
  private List<SortKey> getSortKeys(GenericRecord record) {
    GenericArray<?> keys = (GenericArray<?>) record.get(SORT_KEYS_FIELD_NAME);
    List<SortKey> sortKeys = null;
    if (keys != null) {
      sortKeys = new ArrayList<>(keys.size());
      for (Object key : keys) {
        if (key instanceof Long) {
          sortKeys.add(new SortKey(Schema.Type.LONG, key));
        } else if (key instanceof Integer) {
          sortKeys.add(new SortKey(Schema.Type.INT, key));
        } else if (key instanceof CharSequence) {
          sortKeys.add(new SortKey(Schema.Type.STRING, key.toString()));
        } else {
          throw new IllegalArgumentException("Unsupported sort key type: " + key.getClass());
        }
      }
    }
    return sortKeys;
  }

  private DMLOperation.Type getOperationType(String changeType) {
    // Datastream splits an update event that modifies the primary keys into an delete (UPDATE-DELETE) and insert
    // (UPDATE-INSERT) event
    switch (changeType) {
      case CHANGE_TYPE_UPDATE_DELETE:
        return DMLOperation.Type.DELETE;
      case CHANGE_TYPE_UPDATE_INSERT:
        return DMLOperation.Type.UPDATE;
      default:
        return DMLOperation.Type.valueOf(changeType);
    }
  }

  /**
   * @return the next available DML event
   */
  public DMLEvent nextEvent() throws Exception {
    if (!hasNextEvent()) {
      throw new NoSuchElementException("No more event!");
    }
    DMLEvent result = event;
    event = null;
    return result;
  }

  private StructuredRecord parseRecord(GenericRecord payload) throws Exception {
    if (schema == null) {
      schema = parseSchema(dataFileReader.getSchema());
    }
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    if (schema.getFields() == null) {
      return builder.build();
    }
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Object value = payload.get(fieldName);
      if (value == null) {
        builder.set(fieldName, null);
        continue;
      }
      if (field.getSchema().getType() == Schema.Type.RECORD) {
        builder.set(fieldName, parseRecord((GenericRecord) value));
      } else {
        Schema nonNullableSchema =
          field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
        switch (nonNullableSchema.getType()) {
          case STRING:
            builder.set(fieldName, value.toString());
            break;
          default:
            builder.set(fieldName, value);
        }
      }
    }
    return builder.build();
  }

  private void savePosition() {
    state.put(tableName + POSITION_STATE_KEY_SUFFIX, String.valueOf(position++));
  }
}
