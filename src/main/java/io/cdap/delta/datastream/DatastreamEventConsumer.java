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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.datastream.util.Utils;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.cdap.delta.datastream.util.Utils.handleError;

/**
 * DatastreamEventConsumer consumes the datastream cdc events and generate CDAP Delta CDC events
 */
public class DatastreamEventConsumer {

  public static final String POSITION_STATE_KEY_SUFFIX = ".pos";
  private static final Logger LOGGER = LoggerFactory.getLogger(DatastreamEventConsumer.class);
  private static final String ROW_ID_FIELD_NAME = "row_id";
  private static final String SOURCE_METADATA_FIELD_NAME = "source_metadata";
  private static final String CHANGE_TYPE_FIELD_NAME = "change_type";
  private static final String TRANSACTION_ID_FIELD_NAME = "tx_id";
  private static final String SOURCE_TIMESTAMP_FIELD_NAME = "source_timestamp";
  private static final String PAYLOAD_FIELD_NAME = "payload";
  private final byte[] content;
  private final DeltaSourceContext context;
  private final String currentPath;
  private final DataFileReader<GenericRecord> dataFileReader;
  private final boolean isSnapshot;
  private final SourceTable table;
  private final Map<String, String> state;
  private final String tableName;
  private Schema schema;
  private long position;
  private GenericRecord record;
  private DMLEvent event;

  public DatastreamEventConsumer(byte[] content, DeltaSourceContext context, String currentPath, SourceTable table,
    long startingPosition, Map<String, String> state) {
    this (content, context, currentPath, table, startingPosition, state, null);
  }
  public DatastreamEventConsumer(byte[] content, DeltaSourceContext context, String currentPath, SourceTable table,
    long startingPosition, Map<String, String> state, Schema schema) {
    this.content = content;
    this.context = context;
    this.currentPath = currentPath;
    this.table = table;
    this.tableName = Utils.buildCompositeTableName(table.getSchema(), table.getTable());
    this.position = startingPosition;
    this.dataFileReader = parseContent();
    this.schema = schema;
    this.isSnapshot = isDumpFile(dataFileReader.getSchema());
    this.state = state;
  }

  private Schema parseSchema(org.apache.avro.Schema schema) {
    org.apache.avro.Schema.Field payload = schema.getField(PAYLOAD_FIELD_NAME);
    if (payload == null) {
      throw Utils.handleError(LOGGER, context, "Failed to get payload schema from schema : " + schema);
    }
    org.apache.avro.Schema payloadSchema = payload.schema();
    Set<String> columns = table.getColumns().stream().map(column -> column.getName()).collect(Collectors.toSet());
    Stream<org.apache.avro.Schema.Field> fields = columns.isEmpty() ? payloadSchema.getFields().stream() :
      payloadSchema.getFields().stream().filter(field -> columns.contains(field.name()));
    return Schema.recordOf(payloadSchema.getName(), fields.map(this::convert).toArray(Schema.Field[]::new));
  }

  private Schema.Field convert(org.apache.avro.Schema.Field field) {
    return Schema.Field.of(field.name(), convert(field.schema()));
  }

  /**
   * Convert Datastream AVRO schema to CDAP schema, they are almost same except:
   * 1. Datastream has a custom logical type "varchar" we will convert it to String
   * 2. Datastream has a custom logical type "number" we will convert it to String
   */
  private Schema convert(org.apache.avro.Schema schema) {
    String logicalType = schema.getLogicalType() == null ? null : schema.getLogicalType().getName();

    switch (schema.getType()) {
      case RECORD:
        return Schema
          .recordOf(schema.getName(), schema.getFields().stream().map(this::convert).toArray(Schema.Field[]::new));
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
        return Schema.unionOf(schema.getTypes().stream().map(this::convert).toArray(Schema[]::new));
      default:
        // should not reach here
        throw new IllegalArgumentException("Unsupported type : " + schema.getType());
    }
  }

  private DataFileReader<GenericRecord> parseContent() {
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    SeekableByteArrayInput input = new SeekableByteArrayInput(content);
    DataFileReader<GenericRecord> reader = null;
    try {
       reader = new DataFileReader<>(input, datumReader);
    } catch (IOException e) {
      throw Utils.handleError(LOGGER, context, "Failed to read or parse file : " + currentPath, e);
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

  private boolean isDumpFile(org.apache.avro.Schema schema) {
    org.apache.avro.Schema.Field metaData = schema.getField(SOURCE_METADATA_FIELD_NAME);
    if (metaData == null) {
      throw handleError(LOGGER, context,
        String.format("Cannot find %s field in the schema %s", SOURCE_METADATA_FIELD_NAME, schema.toString()));
    }
    return metaData.schema().getField(CHANGE_TYPE_FIELD_NAME) == null;
  }

  /**
   * @return whether the content being read is from a snapshot
   */
  public boolean isSnapshot() {
    return isSnapshot;
  }

  /**
   * @return whether there is more event available to be read
   */
  public boolean hasNextEvent() {
    // lazy fetch
    if (event == null) {
      fetchNext();
    }
    return event != null;
  }

  private void fetchNext() {
    while (dataFileReader.hasNext()) {
      try {
        record = dataFileReader.next(record);
        savePosition();
      } catch (IOException e) {
        throw Utils.handleError(LOGGER, context, "Failed to read next record in : " + currentPath, e);
      }
      GenericRecord sourceMetadata = (GenericRecord) record.get(SOURCE_METADATA_FIELD_NAME);
      DMLOperation.Type eventType = DMLOperation.Type.INSERT;
      // if it's not snapshot , get the actual event type
      if (!isSnapshot) {
        eventType = DMLOperation.Type.valueOf(sourceMetadata.get(CHANGE_TYPE_FIELD_NAME).toString());
        // if the event type is in the black list , skip this event
        if (table.getDmlBlacklist().contains(eventType)) {
          continue;
        }
      }
      GenericRecord payload = (GenericRecord) record.get(PAYLOAD_FIELD_NAME);
      StructuredRecord row = parseRecord(payload);
      event = DMLEvent.builder().setDatabaseName(table.getDatabase()).setSchemaName(table.getSchema())
        .setTableName(table.getTable()).setIngestTimestamp(System.currentTimeMillis()).setOperationType(
          isSnapshot ? DMLOperation.Type.INSERT :
            DMLOperation.Type.valueOf(sourceMetadata.get(CHANGE_TYPE_FIELD_NAME).toString())).setRow(row)
        .setRowId(sourceMetadata.get(ROW_ID_FIELD_NAME).toString()).setTransactionId(
          sourceMetadata.get(TRANSACTION_ID_FIELD_NAME) == null ? null :
            sourceMetadata.get(TRANSACTION_ID_FIELD_NAME).toString()).setSnapshot(isSnapshot)
        .setSourceTimestamp((Long) record.get(SOURCE_TIMESTAMP_FIELD_NAME)).setOffset(new Offset(state)).build();
      return;
    }
  }

  /**
   * @return the next available DML event
   */
  public DMLEvent nextEvent() {
    if (!hasNextEvent()) {
      throw new NoSuchElementException("No more event!");
    }
    DMLEvent result = event;
    event = null;
    return result;
  }

  private StructuredRecord parseRecord(GenericRecord payload) {
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
