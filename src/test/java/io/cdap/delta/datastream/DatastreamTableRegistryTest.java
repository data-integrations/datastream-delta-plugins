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

import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.api.assessment.TableSummary;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DatastreamTableRegistryTest extends BaseIntegrationTestCase {

  @Test
  public void testListDescribeTable_new() throws IOException, TableNotFoundException {
    testListDesribeTable(false);
  }

  private void testListDesribeTable(boolean usingExisting) throws IOException, TableNotFoundException {
    TableList tableList = null;
    DatastreamConfig config = buildDatastreamConfig(usingExisting);
    try (DatastreamTableRegistry registry = new DatastreamTableRegistry(config, datastream)) {
      tableList = registry.listTables();
    }
    assertNotNull(tableList);
    List<TableSummary> tables = tableList.getTables();
    assertNotNull(tables);
    assertFalse(tables.isEmpty());
    System.out.println("table counts:" + tables.size());
    for (TableSummary table : tables) {
      assertNotNull(table);
      assertEquals(oracleDb, table.getDatabase());
      String schema = table.getSchema();
      assertNotNull(schema);
      String tableName = table.getTable();
      assertNotNull(tableName);
      System.out.println(String.format("table : %s.%s", schema, tableName));
      TableDetail tableDetail = null;
      try (DatastreamTableRegistry registry = new DatastreamTableRegistry(config, datastream)) {
        tableDetail = registry.describeTable(oracleDb, schema, tableName);
      }
      assertNotNull(tableDetail);
      List<ColumnDetail> columns = tableDetail.getColumns();
      assertNotNull(columns);
      assertFalse(columns.isEmpty());
      for (ColumnDetail column : columns) {
        assertNotNull(column);
        assertNotNull(column.getName());
        Map<String, String> properties = column.getProperties();
        assertNotNull(properties);
        assertNotNull(column.getType());
        System.out.println(String.format("column: %s type : %s", column.getName(), column.getType()));
      }
      List<String> primaryKeys = tableDetail.getPrimaryKey();
      System.out.println("primary keys: " + primaryKeys);
      assertNotNull(primaryKeys);
      for (String key : primaryKeys) {
        assertNotNull(key);
        assertFalse(key.isEmpty());
      }
    }
  }

  @Test
  public void testListDescribeTable_existing() throws IOException, TableNotFoundException {
    testListDesribeTable(true);
  }
}
