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

import com.google.common.io.ByteStreams;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DMLOperation;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.SourceColumn;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.datastream.util.MockSourceContext;
import org.apache.avro.LogicalType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatastreamEventConsumerTest {

  @Test
  public void testIsSnapshot() {
    assertTrue(
      DatastreamEventConsumer.isSnapshot("d32354dfd16542bbd20553f18d98edb5c2afc7a5_oracle-backfill_21989784_0_0"));
    assertFalse(
      DatastreamEventConsumer.isSnapshot("d32354dfd16542bbd20553f18d98edb5c2afc7a5_oracle-cdc-logminer_21989784_0_0"));
  }

  @Test
  public void testDump() throws Exception {
    byte[] content = ByteStreams.toByteArray(this.getClass().getClassLoader().getResourceAsStream("dump.avro"));
    String path = "current_path_backfill";
    Map<String, String> state = new HashMap<>();
    String database = "xe";
    String schema = "HR";
    String table = "EMPLOYEES";
    int startingPosition = 2;
    String column1 = "EMPLOYEE_ID";
    String column2 = "SALARY";
    DatastreamEventConsumer consumer = new DatastreamEventConsumer(content, new MockSourceContext(), path,
      new SourceTable(database, table, schema,
        new HashSet<>(Arrays.asList(new SourceColumn(column1), new SourceColumn(column2))),
        new HashSet<>(Arrays.asList(DMLOperation.Type.INSERT)), Collections.emptySet()), startingPosition, state);
    int count = 0;
    long startTime = System.currentTimeMillis();
    while (consumer.hasNextEvent()) {
      DMLEvent event = consumer.nextEvent();
      DMLOperation operation = event.getOperation();
      assertEquals(database, operation.getDatabaseName());
      assertEquals(schema, operation.getSchemaName());
      assertEquals(table, operation.getTableName());
      assertEquals(DMLOperation.Type.INSERT, operation.getType());
      assertTrue(operation.getIngestTimestampMillis() >= startTime);
      assertTrue(operation.getSizeInBytes() > 0);
      assertFalse(event.getRowId().isEmpty());
      assertTrue(event.getIngestTimestampMillis() >= startTime);
      assertNull(event.getPreviousRow());
      assertNull(event.getTransactionId());
      HashMap<String, String> newState = new HashMap<>(state);
      newState.put(schema + "_" + table + ".pos", String.valueOf(startingPosition + count));
      assertEquals(new Offset(newState), event.getOffset());
      StructuredRecord row = event.getRow();
      Schema schema1 = Schema.of(Schema.Type.LONG);
      Schema schema2 = Schema.decimalOf(8, 2);
      assertEquals(Schema
        .recordOf("payload", Schema.Field.of(column1, Schema.unionOf(Schema.of(Schema.Type.NULL), schema1)),
          Schema.Field.of(column2, Schema.unionOf(Schema.of(Schema.Type.NULL), schema2))), row.getSchema());
      assertTrue(row.<Long>get(column1) > 0);
      assertTrue(convert(row.get(column2), schema2).compareTo(BigDecimal.valueOf(0.0)) > 0);
      count++;
    }
    assertEquals(106, count);
  }

  private BigDecimal convert(ByteBuffer value, Schema schema) {
    int scale = schema.getScale();
    byte[] bytes = new byte[value.remaining()];
    value.get(bytes);
    return new BigDecimal(new BigInteger(bytes), scale);
  }

  @Test
  public void testBlacklist() throws Exception {
    byte[] content = ByteStreams.toByteArray(this.getClass().getClassLoader().getResourceAsStream("insert.avro"));
    String path = "current_path";
    Map<String, String> state = new HashMap<>();
    String database = "xe";
    String schema = "HR";
    String table = "EMPLOYEES";
    int startingPosition = 0;
    DatastreamEventConsumer consumer = new DatastreamEventConsumer(content, new MockSourceContext(), path,
      new SourceTable(database, table, schema, Collections.emptySet(),
        new HashSet<>(Arrays.asList(DMLOperation.Type.INSERT)), Collections.emptySet()), startingPosition, state);
    assertFalse(consumer.hasNextEvent());
  }

  @Test
  public void testInsert() throws Exception {
    byte[] content = ByteStreams.toByteArray(this.getClass().getClassLoader().getResourceAsStream("insert.avro"));
    String path = "current_path";
    Map<String, String> state = new HashMap<>();
    String database = "xe";
    String schema = "HR";
    String table = "EMPLOYEES";
    int startingPosition = 0;
    DatastreamEventConsumer consumer = new DatastreamEventConsumer(content, new MockSourceContext(), path,
      new SourceTable(database, table, schema, Collections.emptySet(), Collections.emptySet(), Collections.emptySet()),
      startingPosition, state);
    int count = 0;
    long startTime = System.currentTimeMillis();
    while (consumer.hasNextEvent()) {
      DMLEvent event = consumer.nextEvent();
      DMLOperation operation = event.getOperation();
      assertEquals(database, operation.getDatabaseName());
      assertEquals(schema, operation.getSchemaName());
      assertEquals(table, operation.getTableName());
      assertEquals(DMLOperation.Type.INSERT, operation.getType());
      assertTrue(operation.getIngestTimestampMillis() >= startTime);
      assertTrue(operation.getSizeInBytes() > 0);
      assertFalse(event.getRowId().isEmpty());
      assertTrue(event.getIngestTimestampMillis() >= startTime);
      assertNull(event.getPreviousRow());
      assertNotNull(event.getTransactionId());
      HashMap<String, String> newState = new HashMap<>(state);
      newState.put(schema + "_" + table + ".pos", String.valueOf(startingPosition + count));
      assertEquals(new Offset(newState), event.getOffset());
      StructuredRecord row = event.getRow();
      assertEquals(Schema.recordOf("payload", Schema.Field.of("EMPLOYEE_ID", nullableOf(Schema.of(Schema.Type.LONG))),
        Schema.Field.of("FIRST_NAME", nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("LAST_NAME", nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("EMAIL", nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("PHONE_NUMBER", nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("HIRE_DATE", nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
        Schema.Field.of("JOB_ID", nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("SALARY", nullableOf(Schema.decimalOf(8, 2))),
        Schema.Field.of("COMMISSION_PCT", nullableOf(Schema.decimalOf(2, 2))),
        Schema.Field.of("MANAGER_ID", nullableOf(Schema.of(Schema.Type.LONG))),
        Schema.Field.of("DEPARTMENT_ID", nullableOf(Schema.of(Schema.Type.LONG)))), row.getSchema());
      assertEquals(210L, row.<Long>get("EMPLOYEE_ID"));
      assertEquals("Sean", row.<String>get("FIRST_NAME"));
      assertEquals("Zhou", row.<String>get("LAST_NAME"));
      assertEquals("seanzhou@google.com", row.<String>get("EMAIL"));
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
      assertEquals("2020-01-09T00:00:00Z",
        sdf.format(new java.util.Date(row.<Long>get("HIRE_DATE").longValue() / 1000)));
      assertEquals("AD_PRES", row.<String>get("JOB_ID"));
      assertEquals(new BigDecimal(BigInteger.valueOf(1213100L), 2),
        convert(row.<ByteBuffer>get("SALARY"), Schema.decimalOf(8, 2)));
      assertNull(row.<ByteBuffer>get("COMMISSION_PCT"));
      assertEquals(205L, row.<Long>get("MANAGER_ID"));
      assertEquals(110L, row.<Long>get("DEPARTMENT_ID"));
      count++;
    }
    assertEquals(1, count);
  }

  private Schema nullableOf(Schema schema) {
    return Schema.unionOf(Schema.of(Schema.Type.NULL), schema);
  }

  @Test
  public void testUpdate() throws Exception {
    byte[] content = ByteStreams.toByteArray(this.getClass().getClassLoader().getResourceAsStream("update.avro"));
    String path = "current_path";
    Map<String, String> state = new HashMap<>();
    String database = "xe";
    String schema = "HR";
    String table = "EMPLOYEES";
    int startingPosition = 0;
    DatastreamEventConsumer consumer = new DatastreamEventConsumer(content, new MockSourceContext(), path,
      new SourceTable(database, table, schema, Collections.emptySet(), Collections.emptySet(), Collections.emptySet()),
      startingPosition, state);
    int count = 0;
    long startTime = System.currentTimeMillis();
    while (consumer.hasNextEvent()) {
      DMLEvent event = consumer.nextEvent();
      DMLOperation operation = event.getOperation();
      assertEquals(database, operation.getDatabaseName());
      assertEquals(schema, operation.getSchemaName());
      assertEquals(table, operation.getTableName());
      assertEquals(DMLOperation.Type.UPDATE, operation.getType());
      assertTrue(operation.getIngestTimestampMillis() >= startTime);
      assertTrue(operation.getSizeInBytes() > 0);
      assertFalse(event.getRowId().isEmpty());
      assertTrue(event.getIngestTimestampMillis() >= startTime);
      assertNull(event.getPreviousRow());
      assertNotNull(event.getTransactionId());
      HashMap<String, String> newState = new HashMap<>(state);
      newState.put(schema + "_" + table + ".pos", String.valueOf(startingPosition + count));
      assertEquals(new Offset(newState), event.getOffset());
      StructuredRecord row = event.getRow();
      assertEquals(Schema.recordOf("payload", Schema.Field.of("EMPLOYEE_ID", nullableOf(Schema.of(Schema.Type.LONG))),
        Schema.Field.of("FIRST_NAME", nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("LAST_NAME", nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("EMAIL", nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("PHONE_NUMBER", nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("HIRE_DATE", nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
        Schema.Field.of("JOB_ID", nullableOf(Schema.of(Schema.Type.STRING))),
        Schema.Field.of("SALARY", nullableOf(Schema.decimalOf(8, 2))),
        Schema.Field.of("COMMISSION_PCT", nullableOf(Schema.decimalOf(2, 2))),
        Schema.Field.of("MANAGER_ID", nullableOf(Schema.of(Schema.Type.LONG))),
        Schema.Field.of("DEPARTMENT_ID", nullableOf(Schema.of(Schema.Type.LONG)))), row.getSchema());
      assertEquals(210L, row.<Long>get("EMPLOYEE_ID"));
      assertEquals("Sean", row.<String>get("FIRST_NAME"));
      assertEquals("Zhou", row.<String>get("LAST_NAME"));
      assertEquals("seanzhou@google.com", row.<String>get("EMAIL"));
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
      assertEquals("2020-01-09T00:00:00Z",
        sdf.format(new java.util.Date(row.<Long>get("HIRE_DATE").longValue() / 1000)));
      assertEquals("AD_PRES", row.<String>get("JOB_ID"));
      assertEquals(new BigDecimal(BigInteger.valueOf(888800L), 2),
        convert(row.<ByteBuffer>get("SALARY"), Schema.decimalOf(8, 2)));
      assertNull(row.<ByteBuffer>get("COMMISSION_PCT"));
      assertEquals(205L, row.<Long>get("MANAGER_ID"));
      assertEquals(110L, row.<Long>get("DEPARTMENT_ID"));
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testDelete() throws Exception {
    byte[] content = ByteStreams.toByteArray(this.getClass().getClassLoader().getResourceAsStream("delete.avro"));
    String path = "current_path";
    Map<String, String> state = new HashMap<>();
    String database = "xe";
    String schema = "HR";
    String table = "EMPLOYEES";
    int startingPosition = 0;
    DatastreamEventConsumer consumer = new DatastreamEventConsumer(content, new MockSourceContext(), path,
      new SourceTable(database, table, schema, Collections.emptySet(), Collections.emptySet(), Collections.emptySet()),
      startingPosition, state);
    int count = 0;
    long startTime = System.currentTimeMillis();
    while (consumer.hasNextEvent()) {
      DMLEvent event = consumer.nextEvent();
      DMLOperation operation = event.getOperation();
      assertEquals(database, operation.getDatabaseName());
      assertEquals(schema, operation.getSchemaName());
      assertEquals(table, operation.getTableName());
      assertEquals(DMLOperation.Type.DELETE, operation.getType());
      assertTrue(operation.getIngestTimestampMillis() >= startTime);
      assertTrue(operation.getSizeInBytes() == 0);
      assertFalse(event.getRowId().isEmpty());
      assertTrue(event.getIngestTimestampMillis() >= startTime);
      //TODO once CDAP-17919 is fixed we should expect previous row to be null for delete event
      // this is just a workaround for the issue.
      assertNotNull(event.getPreviousRow());
      assertNotNull(event.getTransactionId());
      HashMap<String, String> newState = new HashMap<>(state);
      newState.put(schema + "_" + table + ".pos", String.valueOf(startingPosition + count));
      assertEquals(new Offset(newState), event.getOffset());
      StructuredRecord row = event.getRow();
      assertEquals(210L, row.<Long>get("EMPLOYEE_ID"));
      assertEquals("Sean", row.<String>get("FIRST_NAME"));
      assertEquals("Zhou", row.<String>get("LAST_NAME"));
      assertEquals("seanzhou@google.com", row.<String>get("EMAIL"));
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
      assertEquals("2020-01-09T00:00:00Z",
        sdf.format(new java.util.Date(row.<Long>get("HIRE_DATE").longValue() / 1000)));
      assertEquals("AD_PRES", row.<String>get("JOB_ID"));
      assertEquals(new BigDecimal(BigInteger.valueOf(888800L), 2),
        convert(row.<ByteBuffer>get("SALARY"), Schema.decimalOf(8, 2)));
      assertNull(row.<ByteBuffer>get("COMMISSION_PCT"));
      assertEquals(205L, row.<Long>get("MANAGER_ID"));
      assertEquals(110L, row.<Long>get("DEPARTMENT_ID"));
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testConvert() {
    org.apache.avro.Schema decimalBytes = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES);
    decimalBytes.addProp("precision", 5);
    decimalBytes.addProp("scale", 3);

    org.apache.avro.Schema averoSchema = org.apache.avro.Schema.createRecord("name", "doc", "namespace", false, Arrays
      .asList(
        new org.apache.avro.Schema.Field("INT", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), "INT",
          0), new org.apache.avro.Schema.Field("DATE",
          new LogicalType("date").addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)), "DATE",
          0), new org.apache.avro.Schema.Field("TIME_MILLIS",
          new LogicalType("time-millis").addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)),
          "TIME_MILLIS", 0),
        new org.apache.avro.Schema.Field("LONG", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
          "LONG", 0), new org.apache.avro.Schema.Field("TIME_MICROS",
          new LogicalType("time-micros").addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG)),
          "TIME_MICROS", 0), new org.apache.avro.Schema.Field("TIMESTAMP_MILLIS", new LogicalType("timestamp-millis")
          .addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG)), "TIMESTAMP_MILLIS", 0),
        new org.apache.avro.Schema.Field("TIMESTAMP_MICROS", new LogicalType("timestamp-micros")
          .addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG)), "TIMESTAMP_MICROS", 0),
        new org.apache.avro.Schema.Field("LOCAL_TIMESTAMP_MILLIS", new LogicalType("local-timestamp-millis")
          .addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG)), "LOCAL_TIMESTAMP_MILLIS", 0),
        new org.apache.avro.Schema.Field("LOCAL_TIMESTAMP_MICROS", new LogicalType("local-timestamp-micros")
          .addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG)), "LOCAL_TIMESTAMP_MICROS", 0),
        new org.apache.avro.Schema.Field("NULL", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
          "NULL", (Object) null),
        new org.apache.avro.Schema.Field("FLOAT", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.FLOAT),
          "FLOAT", 0.0f),
        new org.apache.avro.Schema.Field("DOUBLE", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE),
          "DOUBLE", 0.0d),
        new org.apache.avro.Schema.Field("BYTES", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES),
          "BYTES", new byte[0]),
        new org.apache.avro.Schema.Field("FIXED", org.apache.avro.Schema.createFixed("FIXED", "FIXED", "FIXED", 1),
          "FIXED", new byte[1]),
        new org.apache.avro.Schema.Field("DECIMAL", new LogicalType("decimal").addToSchema(decimalBytes), "DECIMAL",
          new byte[0]),
        new org.apache.avro.Schema.Field("STRING", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
          "STRING", ""),
        new org.apache.avro.Schema.Field("BOOLEAN", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BOOLEAN),
          "BOOLEAN", true), new org.apache.avro.Schema.Field("MAP",
          org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)), "MAP",
          new HashMap<String, String>()), new org.apache.avro.Schema.Field("ENUM",
          org.apache.avro.Schema.createEnum("ENUM", "ENUM", "ENUM", Arrays.asList("VAL1", "VAL2")), "ENUM",
          (Object) null), new org.apache.avro.Schema.Field("ARRAY",
          org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)), "ARRAY",
          (Object) null), new org.apache.avro.Schema.Field("UNION", org.apache.avro.Schema
          .createUnion(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)), "ARRAY", (Object) null)));

    Schema cdapSchema = DatastreamEventConsumer.convert(averoSchema);
    assertEquals("name", cdapSchema.getRecordName());
    List<Schema.Field> fields = cdapSchema.getFields();
    assertEquals(averoSchema.getFields().size(), fields.size());

    Schema.Field field = fields.get(0);
    assertEquals("INT", field.getName());
    assertEquals(Schema.of(Schema.Type.INT), field.getSchema());

    field = fields.get(1);
    assertEquals("DATE", field.getName());
    assertEquals(Schema.of(Schema.LogicalType.DATE), field.getSchema());

    field = fields.get(2);
    assertEquals("TIME_MILLIS", field.getName());
    assertEquals(Schema.of(Schema.LogicalType.TIME_MILLIS), field.getSchema());

    field = fields.get(3);
    assertEquals("LONG", field.getName());
    assertEquals(Schema.of(Schema.Type.LONG), field.getSchema());

    field = fields.get(4);
    assertEquals("TIME_MICROS", field.getName());
    assertEquals(Schema.of(Schema.LogicalType.TIME_MICROS), field.getSchema());

    field = fields.get(5);
    assertEquals("TIMESTAMP_MILLIS", field.getName());
    assertEquals(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS), field.getSchema());

    field = fields.get(6);
    assertEquals("TIMESTAMP_MICROS", field.getName());
    assertEquals(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS), field.getSchema());

    field = fields.get(7);
    assertEquals("LOCAL_TIMESTAMP_MILLIS", field.getName());
    assertEquals(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS), field.getSchema());

    field = fields.get(8);
    assertEquals("LOCAL_TIMESTAMP_MICROS", field.getName());
    assertEquals(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS), field.getSchema());

    field = fields.get(9);
    assertEquals("NULL", field.getName());
    assertEquals(Schema.of(Schema.Type.NULL), field.getSchema());

    field = fields.get(10);
    assertEquals("FLOAT", field.getName());
    assertEquals(Schema.of(Schema.Type.FLOAT), field.getSchema());

    field = fields.get(11);
    assertEquals("DOUBLE", field.getName());
    assertEquals(Schema.of(Schema.Type.DOUBLE), field.getSchema());

    field = fields.get(12);
    assertEquals("BYTES", field.getName());
    assertEquals(Schema.of(Schema.Type.BYTES), field.getSchema());

    field = fields.get(13);
    assertEquals("FIXED", field.getName());
    assertEquals(Schema.of(Schema.Type.BYTES), field.getSchema());

    field = fields.get(14);
    assertEquals("DECIMAL", field.getName());
    assertEquals(Schema.decimalOf(5, 3), field.getSchema());

    field = fields.get(15);
    assertEquals("STRING", field.getName());
    assertEquals(Schema.of(Schema.Type.STRING), field.getSchema());

    field = fields.get(16);
    assertEquals("BOOLEAN", field.getName());
    assertEquals(Schema.of(Schema.Type.BOOLEAN), field.getSchema());

    field = fields.get(17);
    assertEquals("MAP", field.getName());
    assertEquals(Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)), field.getSchema());

    field = fields.get(18);
    assertEquals("ENUM", field.getName());
    assertEquals(Schema.enumWith("VAL1", "VAL2"), field.getSchema());

    field = fields.get(19);
    assertEquals("ARRAY", field.getName());
    assertEquals(Schema.arrayOf(Schema.of(Schema.Type.INT)), field.getSchema());

    field = fields.get(20);
    assertEquals("UNION", field.getName());
    assertEquals(Schema.unionOf(Schema.of(Schema.Type.INT), Schema.of(Schema.Type.STRING)), field.getSchema());

  }
}
