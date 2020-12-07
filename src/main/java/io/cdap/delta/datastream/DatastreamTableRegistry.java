/*
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

import com.google.cloud.ServiceOptions;
import com.google.cloud.datastream.v1alpha1.ConnectionProfile;
import com.google.cloud.datastream.v1alpha1.DatastreamClient;
import com.google.cloud.datastream.v1alpha1.DiscoverConnectionProfileRequest;
import com.google.cloud.datastream.v1alpha1.DiscoverConnectionProfileResponse;
import com.google.cloud.datastream.v1alpha1.OracleColumn;
import com.google.cloud.datastream.v1alpha1.OracleRdbms;
import com.google.cloud.datastream.v1alpha1.OracleSchema;
import com.google.cloud.datastream.v1alpha1.OracleTable;
import com.google.common.collect.Iterables;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.ColumnSupport;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.api.assessment.TableSummary;
import io.cdap.delta.datastream.util.DatastreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lists and describes tables.
 */
public class DatastreamTableRegistry implements TableRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamTableRegistry.class);
  private final DatastreamConfig config;
  private final DatastreamClient datastreamClient;
  // parent path of datastream resources in form of "projects/projectId/locations/region"
  private final String parentPath;
  private static final Set<String> SYSTEM_SCHEMA = new HashSet<>(Arrays.asList("SYS", "SYSTEM", "CTXSYS", "XDB",
    "MDSYS", "FLOWS_FILES", "APEX_040000", "OUTLN"));


  public DatastreamTableRegistry(DatastreamConfig config, DatastreamClient datastreamClient) {
    this.config = config;
    this.datastreamClient = datastreamClient;
    //TODO validate whether the region is valid

    this.parentPath = String
      .format("projects/%s/locations/%s", ServiceOptions.getDefaultProjectId(), config.getRegion());
  }

  @Override
  public TableList listTables() throws IOException {
    LOG.debug(String.format("List tables..."));
    List<TableSummary> tables = new ArrayList<>();

    DiscoverConnectionProfileResponse response = discover();
    for (OracleSchema schema : response.getOracleRdbms().getOracleSchemasList()) {
      String schemaName = schema.getSchemaName();
      if (SYSTEM_SCHEMA.contains(schemaName.toUpperCase())) {
        //skip system tables
        continue;
      }
      for (OracleTable table : schema.getOracleTablesList()) {
        String tableName = table.getTableName();
        tables.add(
          new TableSummary(config.getSid(), tableName, table.getOracleColumnsCount(), schemaName));
      }
    }
    return new TableList(tables);
  }

  @Override
  public TableDetail describeTable(String db, String schema, String table) throws TableNotFoundException, IOException {
    LOG.debug(String.format("Describe table, db: %s, table: %s, schema: %s", db, table, schema));
    DiscoverConnectionProfileResponse discoverResponse = discover(schema, table);
    //TODO handle schema/table doesn't exist case, currently datastream only throw a very generic
    // exception : com.google.api.gax.rpc.UnknownException without signals indicating this error
    // case

    OracleSchema oracleSchema =
      Iterables.getOnlyElement(discoverResponse.getOracleRdbms().getOracleSchemasList());
    OracleTable oracleTable = Iterables.getOnlyElement(oracleSchema.getOracleTablesList());

    List<ColumnDetail> columns = new ArrayList<>(oracleTable.getOracleColumnsCount());
    List<String> primaryKeys = new ArrayList<>();
    for (OracleColumn column : oracleTable.getOracleColumnsList()) {
      Map<String, String> properties = new HashMap<>();
      properties.put(DatastreamTableAssessor.PRECISION, Integer.toString(column.getPrecision()));
      properties.put(DatastreamTableAssessor.SCALE,
                     Integer.toString(column.getScale()));
      columns.add(
        new ColumnDetail(column.getColumnName(), DatastreamUtils.convertStringDataTypetoSQLType(column.getDataType()),
          column.getNullable(), properties));
      if (column.getPrimaryKey()) {
        primaryKeys.add(column.getColumnName());
      }
    }
    return new TableDetail.Builder(db, table, schema).setColumns(columns).setPrimaryKey(primaryKeys).build();
  }

  @Override
  public StandardizedTableDetail standardize(TableDetail tableDetail) {
    List<Schema.Field> columnSchemas = new ArrayList<>();
    for (ColumnDetail detail : tableDetail.getColumns()) {
      ColumnEvaluation evaluation = DatastreamTableAssessor.evaluateColumn(detail);
      if (evaluation.getAssessment().getSupport().equals(ColumnSupport.NO)) {
        continue;
      }
      columnSchemas.add(evaluation.getField());
    }
    Schema schema = Schema.recordOf("outputSchema", columnSchemas);
    return new StandardizedTableDetail(tableDetail.getDatabase(), tableDetail.getTable(),
      tableDetail.getPrimaryKey(), schema);
  }

  @Override
  public void close() throws IOException {
    datastreamClient.close();
  }



  private ConnectionProfile.Builder buildOracleConnectionProfile(String name) {
    return DatastreamUtils.buildOracleConnectionProfile(config).setDisplayName(name);
  }

  private DiscoverConnectionProfileResponse discover(String schema, String table) {
    return datastreamClient.discoverConnectionProfile(
      DiscoverConnectionProfileRequest.newBuilder().setParent(parentPath)
        .setConnectionProfile(DatastreamUtils.buildOracleConnectionProfile(config)).setOracleRdbms(
        OracleRdbms.newBuilder().addOracleSchemas(OracleSchema.newBuilder().setSchemaName(schema).addOracleTables(
          OracleTable.newBuilder().setTableName(table)))).build());
  }

  private DiscoverConnectionProfileResponse discover() {
    return datastreamClient.discoverConnectionProfile(
      DiscoverConnectionProfileRequest.newBuilder().setParent(parentPath)
        .setConnectionProfile(DatastreamUtils.buildOracleConnectionProfile(config)).setRecursive(true).build());
  }
}
