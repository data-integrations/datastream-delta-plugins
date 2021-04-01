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
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
      assertNull(event.getPreviousRow());
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
}
