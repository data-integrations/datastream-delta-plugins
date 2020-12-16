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
import io.cdap.delta.api.ReplicationError;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.datastream.util.DatastreamUtils;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.cdap.delta.datastream.util.DatastreamUtils.handleError;

/**
 * DatastreamEventConsumer consumes the datastream cdc events and generate CDAP Delta CDC events
 */
public class DatastreamEventConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatastreamEventConsumer.class);
  private static final String ROW_ID_FIELD_NAME = "row_id";
  private static final String SOURCE_METADATA_FIELD_NAME = "source_metadata";
  private static final String CHANGE_TYPE_FIELD_NAME = "change_type";
  private static final String TRANSACTION_ID_FIELD_NAME = "tx_id";
  private static final String SOURCE_TIMESTAMP_FIELD_NAME = "source_timestamp";
  private static final String DUMP_SUFFIX = "_dump";
  private static final String PAYLOAD_FIELD_NAME = "payload";
  private final byte[] content;
  private final DeltaSourceContext context;
  private final String currentPath;
  private final DataFileReader<GenericRecord> dataFileReader;
  private final boolean isSnapshot;
  private final SourceTable table;
  private final Schema schema;
  private final Map<String, String> state;
  private GenericRecord event;

  public DatastreamEventConsumer(byte[] content, DeltaSourceContext context, String currentPath, SourceTable table,
    long startingPosition, Map<String, String> state) {
    this.content = content;
    this.context = context;
    this.currentPath = currentPath;
    this.table = table;
    this.dataFileReader = parseContent(startingPosition);
    this.schema = parseSchema(dataFileReader.getSchema());
    this.isSnapshot = isDumpFile(dataFileReader.getSchema());
    this.state = state;
  }

  private Schema parseSchema(org.apache.avro.Schema schema) {
    org.apache.avro.Schema.Field payload = schema.getField(PAYLOAD_FIELD_NAME);
    if (payload == null) {
      throw DatastreamUtils.handleError(LOGGER, context, "Failed to get payload schema from schema : " + schema);
    }
    org.apache.avro.Schema payloadSchema = payload.schema();
    Set<String> columns = table.getColumns().stream().map(column -> column.getName()).collect(Collectors.toSet());
    Stream<org.apache.avro.Schema.Field> fields = columns.isEmpty() ? payloadSchema.getFields().stream() :
      payloadSchema.getFields().stream().filter(columns::contains);
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

  private DataFileReader<GenericRecord> parseContent(long startingPosition) {
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    SeekableByteArrayInput input = new SeekableByteArrayInput(content);
    DataFileReader<GenericRecord> reader = null;
    try {
       reader = new DataFileReader<>(input, datumReader);
    } catch (IOException e) {
      LOGGER.error("Failed to read or parse file : " + currentPath, e);
      try {
        context.setError(new ReplicationError(e));
      } catch (IOException ioException) {
        LOGGER.error("Error setting error in context!", e);
        throw new RuntimeException(ioException);
      }
      throw new RuntimeException(e);
    }
    for (int i = 0; i < startingPosition; i++) {
      if (reader.hasNext()) {
        reader.next();
      } else {
        throw handleError(
          LOGGER, context,
          String.format("starting position %d exceeds the max position %d in path: %s", startingPosition, i - 1,
            currentPath));
      }
    }
    return reader;
  }

  private boolean isDumpFile(org.apache.avro.Schema schema) {
    org.apache.avro.Schema.Field readMethod = schema.getField(SOURCE_METADATA_FIELD_NAME);
    if (readMethod == null) {
      throw handleError(LOGGER, context,
        String.format("Cannot find %s field in the schema %s", SOURCE_METADATA_FIELD_NAME, schema.toString()));
    }
    return readMethod.schema().getField(CHANGE_TYPE_FIELD_NAME) == null;
  }

  public boolean isSnapshot() {
    return isSnapshot;
  }



  public boolean hasNext() {
    return dataFileReader.hasNext();
  }

  public DMLEvent next() {
    try {

      event = dataFileReader.next(event);
      GenericRecord payload = (GenericRecord) event.get(PAYLOAD_FIELD_NAME);
      StructuredRecord row = parseRecord(payload);
      GenericRecord sourceMetadata = (GenericRecord) event.get(SOURCE_METADATA_FIELD_NAME);


      return DMLEvent.builder().setDatabaseName(table.getDatabase()).setTableName(table.getTable())
        .setIngestTimestamp(System.currentTimeMillis()).setOperationType(isSnapshot ? DMLOperation.Type.INSERT :
          DMLOperation.Type.valueOf(sourceMetadata.get(CHANGE_TYPE_FIELD_NAME).toString())).setRow(row)
        .setRowId(sourceMetadata.get(ROW_ID_FIELD_NAME).toString()).setTransactionId(
          sourceMetadata.get(TRANSACTION_ID_FIELD_NAME) == null ? null :
            sourceMetadata.get(TRANSACTION_ID_FIELD_NAME).toString()).setSnapshot(isSnapshot)
        .setSourceTimestamp((Long) event.get(SOURCE_TIMESTAMP_FIELD_NAME)).setOffset(new Offset(state)).build();

    } catch (IOException e) {
       throw DatastreamUtils.handleError(LOGGER, context, "Failed to read next event in : " + currentPath, e);
    }
  }

  private StructuredRecord parseRecord(GenericRecord payload) {

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
}
