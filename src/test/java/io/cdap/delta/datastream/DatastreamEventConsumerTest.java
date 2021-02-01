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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatastreamEventConsumerTest {

  @Test
  public void testDump() throws Exception {
    byte[] content = ByteStreams.toByteArray(this.getClass().getClassLoader().getResourceAsStream("dump.avro"));
    String path = "current_path";
    Map<String, String> state = new HashMap<>();
    String database = "xe";
    String schema = "HR";
    String table = "JOBS";
    int startingPosition = 2;
    String column1 = "JOB_ID";
    Schema.Type columnType1 = Schema.Type.STRING;
    String column2 = "MIN_SALARY";
    Schema.Type columnType2 = Schema.Type.LONG;
    DatastreamEventConsumer consumer = new DatastreamEventConsumer(content, new MockSourceContext(), path,
      new SourceTable(database, table, schema,
        new HashSet<>(Arrays.asList(new SourceColumn(column1), new SourceColumn(column2))),
        new HashSet<>(Arrays.asList(DMLOperation.Type.INSERT)), Collections.emptySet()), startingPosition, state);
    assertTrue(consumer.isSnapshot());
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
      Schema.recordOf("payload", Schema.Field.of(column1, Schema.nullableOf(Schema.of(columnType1))),
        Schema.Field.of(column2, Schema.nullableOf(Schema.of(columnType2))));
      assertFalse(((String) row.get(column1)).isEmpty());
      assertTrue((Long) row.get(column2) > 0);
      count++;
    }
    assertEquals(18, count);
  }

  @Test
  public void testBlacklist() throws Exception {
    byte[] content = ByteStreams.toByteArray(this.getClass().getClassLoader().getResourceAsStream("insert.avro"));
    String path = "current_path";
    Map<String, String> state = new HashMap<>();
    String database = "xe";
    String schema = "HR";
    String table = "JOBS";
    int startingPosition = 0;
    String column1 = "JOB_ID";
    Schema.Type columnType1 = Schema.Type.STRING;
    String column2 = "MIN_SALARY";
    Schema.Type columnType2 = Schema.Type.LONG;
    DatastreamEventConsumer consumer = new DatastreamEventConsumer(content, new MockSourceContext(), path,
      new SourceTable(database, table, schema,
        new HashSet<>(Arrays.asList(new SourceColumn(column1), new SourceColumn(column2))),
        new HashSet<>(Arrays.asList(DMLOperation.Type.INSERT)), Collections.emptySet()), startingPosition, state);
    assertFalse(consumer.isSnapshot());
    assertFalse(consumer.hasNextEvent());
  }

  @Test
  public void testInsert() throws Exception {
    byte[] content = ByteStreams.toByteArray(this.getClass().getClassLoader().getResourceAsStream("insert.avro"));
    String path = "current_path";
    Map<String, String> state = new HashMap<>();
    String database = "xe";
    String schema = "HR";
    String table = "JOBS";
    int startingPosition = 0;
    String column1 = "JOB_ID";
    Schema.Type columnType1 = Schema.Type.STRING;
    String column2 = "MIN_SALARY";
    Schema.Type columnType2 = Schema.Type.LONG;
    DatastreamEventConsumer consumer = new DatastreamEventConsumer(content, new MockSourceContext(), path,
      new SourceTable(database, table, schema,
        new HashSet<>(Arrays.asList(new SourceColumn(column1), new SourceColumn(column2))),
        Collections.emptySet(), Collections.emptySet()), startingPosition, state);
    assertFalse(consumer.isSnapshot());
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
      Schema.recordOf("payload", Schema.Field.of(column1, Schema.nullableOf(Schema.of(columnType1))),
        Schema.Field.of(column2, Schema.nullableOf(Schema.of(columnType2))));
      assertEquals("Kerry", row.get(column1));
      assertNull(row.get(column2));
      count++;
    }
    assertEquals(1, count);
  }

  @Test
  public void testUpdate() throws Exception {
    byte[] content = ByteStreams.toByteArray(this.getClass().getClassLoader().getResourceAsStream("update.avro"));
    String path = "current_path";
    Map<String, String> state = new HashMap<>();
    String database = "xe";
    String schema = "HR";
    String table = "JOBS";
    int startingPosition = 0;
    String column1 = "JOB_ID";
    Schema.Type columnType1 = Schema.Type.STRING;
    String column2 = "MIN_SALARY";
    Schema.Type columnType2 = Schema.Type.LONG;
    DatastreamEventConsumer consumer = new DatastreamEventConsumer(content, new MockSourceContext(), path,
      new SourceTable(database, table, schema,
        new HashSet<>(Arrays.asList(new SourceColumn(column1), new SourceColumn(column2))),
        Collections.emptySet(), Collections.emptySet()), startingPosition, state);
    assertFalse(consumer.isSnapshot());
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
      Schema.recordOf("payload", Schema.Field.of(column1, Schema.nullableOf(Schema.of(columnType1))),
        Schema.Field.of(column2, Schema.nullableOf(Schema.of(columnType2))));
      assertEquals("SEAN", row.get(column1));
      assertEquals(2000, (Long) row.get(column2));
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
    String table = "JOBS";
    int startingPosition = 0;
    String column1 = "JOB_ID";
    Schema.Type columnType1 = Schema.Type.STRING;
    String column2 = "MIN_SALARY";
    Schema.Type columnType2 = Schema.Type.LONG;
    DatastreamEventConsumer consumer = new DatastreamEventConsumer(content, new MockSourceContext(), path,
      new SourceTable(database, table, schema,
        new HashSet<>(Arrays.asList(new SourceColumn(column1), new SourceColumn(column2))),
        Collections.emptySet(), Collections.emptySet()), startingPosition, state);
    assertFalse(consumer.isSnapshot());
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
      Schema.recordOf("payload", Schema.Field.of(column1, Schema.nullableOf(Schema.of(columnType1))),
        Schema.Field.of(column2, Schema.nullableOf(Schema.of(columnType2))));
      assertEquals("Kerry", row.get(column1));
      assertNull(row.get(column2));
      count++;
    }
    assertEquals(1, count);
  }
}
