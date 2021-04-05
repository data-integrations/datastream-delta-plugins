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

import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.datastream.v1alpha1.DatastreamClient;
import com.google.cloud.datastream.v1alpha1.DiscoverConnectionProfileRequest;
import com.google.cloud.datastream.v1alpha1.DiscoverConnectionProfileResponse;
import com.google.cloud.datastream.v1alpha1.OracleColumn;
import com.google.cloud.datastream.v1alpha1.OracleRdbms;
import com.google.cloud.datastream.v1alpha1.OracleSchema;
import com.google.cloud.datastream.v1alpha1.OracleTable;
import com.google.common.collect.Iterables;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.ColumnSupport;
import io.cdap.delta.api.assessment.StandardizedTableDetail;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.api.assessment.TableRegistry;
import io.cdap.delta.api.assessment.TableSummary;
import io.cdap.delta.datastream.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.RemoteException;
import java.sql.SQLType;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(DatastreamTableRegistry.class);
  private static final Gson GSON = new Gson();
  private final DatastreamConfig config;
  private final DatastreamClient datastream;
  // parent path of datastream resources in form of "projects/projectId/locations/region"
  private final String parentPath;
  // TODO find a better way to get system schemas for different version of Oracle.
  //  May need additional support from datasteam for :
  //  1. get the version of oracle and we map it to system schemas
  //  or
  //  2. get information about whether the schema is a system schema
  private static final Set<String> SYSTEM_SCHEMA =
    new HashSet<>(Arrays.asList("SYS", "SYSTEM", "CTXSYS", "XDB", "MDSYS", "FLOWS_FILES", "APEX_040000", "OUTLN"));


  public DatastreamTableRegistry(DatastreamConfig config, DatastreamClient datastream) {
    this.config = config;
    this.datastream = datastream;
    //TODO validate whether the region is valid
    this.parentPath = Utils.buildParentPath(config.getProject(), config.getRegion());
  }

  @Override
  public TableList listTables() throws IOException {
    List<TableSummary> tables = new ArrayList<>();

    String databaseName;
    String sourceConnectionProfileName = null;
    if (config.isUsingExistingStream()) {
      sourceConnectionProfileName =
        Utils.getStream(datastream, Utils.buildStreamPath(parentPath, config.getStreamId()), LOGGER).getSourceConfig()
          .getSourceConnectionProfileName();
      databaseName = Utils.getConnectionProfile(datastream, sourceConnectionProfileName, LOGGER).getOracleProfile()
        .getDatabaseService();
    } else {
      databaseName = config.getSid();
    }
    DiscoverConnectionProfileRequest request =
      buildDiscoverConnectionProfileRequest(sourceConnectionProfileName).build();
    DiscoverConnectionProfileResponse response;
    try {
      response = Utils.discoverConnectionProfile(datastream, request, LOGGER);
    } catch (InvalidArgumentException e) {
      throw new RuntimeException("Failed to connect to the database. Please double check whether the connection " +
        "information you input is correct.", e);
    } catch (Exception e) {
      throw new RuntimeException("Failed to connect to the database due to below error :" + e.getMessage(), e);
    }

    for (OracleSchema schema : response.getOracleRdbms().getOracleSchemasList()) {
      String schemaName = schema.getSchemaName();
      if (SYSTEM_SCHEMA.contains(schemaName.toUpperCase())) {
        //skip system tables
        continue;
      }
      for (OracleTable table : schema.getOracleTablesList()) {
        String tableName = table.getTableName();
        tables.add(new TableSummary(databaseName, tableName, table.getOracleColumnsCount(), schemaName));
      }
    }
    return new TableList(tables);
  }

  @Override
  public TableDetail describeTable(String db, String schema, String table) throws TableNotFoundException, IOException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Describe table, db: {}, table: {}, schema: {}", db, table, schema);
    }
    DiscoverConnectionProfileResponse discoverResponse;
    try {

      DiscoverConnectionProfileRequest request = buildDiscoverConnectionProfileRequest(config.isUsingExistingStream() ?
        Utils.getStream(datastream, Utils.buildStreamPath(parentPath, config.getStreamId()), LOGGER).getSourceConfig()
          .getSourceConnectionProfileName() : null).setOracleRdbms(OracleRdbms.newBuilder().addOracleSchemas(
        OracleSchema.newBuilder().setSchemaName(schema).addOracleTables(OracleTable.newBuilder().setTableName(table))))
        .build();

      discoverResponse = Utils.discoverConnectionProfile(datastream, request, LOGGER);
    } catch (NotFoundException e) {
      throw new TableNotFoundException(db, schema, table, e.getMessage(), e);
    }

    OracleSchema oracleSchema = Iterables.getOnlyElement(discoverResponse.getOracleRdbms().getOracleSchemasList());
    OracleTable oracleTable = Iterables.getOnlyElement(oracleSchema.getOracleTablesList());
    List<ColumnDetail> columns = new ArrayList<>(oracleTable.getOracleColumnsCount());
    List<String> primaryKeys = new ArrayList<>();
    for (OracleColumn column : oracleTable.getOracleColumnsList()) {
      Map<String, String> properties = new HashMap<>();
      if (column.getPrecision() > 0) {
        properties.put(DatastreamTableAssessor.PRECISION, Integer.toString(column.getPrecision()));
      }
      if (column.getScale() > 0) {
        properties.put(DatastreamTableAssessor.SCALE, Integer.toString(column.getScale()));
      }
      SQLType sqlType = Utils.convertStringDataTypeToSQLType(column.getDataType());
      columns
        .add(new ColumnDetail(column.getColumnName(), sqlType, Boolean.TRUE.equals(column.getNullable()), properties));
      if (Boolean.TRUE.equals(column.getPrimaryKey())) {
        primaryKeys.add(column.getColumnName());
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Found column : {}, data type : {} (converted to {}), precision: {}, scale: {} , isPrimary: {}",
          column.getColumnName(), column.getDataType(), sqlType, column.getPrecision(), column.getScale(),
          column.getPrimaryKey());
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
    return new StandardizedTableDetail(tableDetail.getDatabase(), tableDetail.getSchema(), tableDetail.getTable(),
      tableDetail.getPrimaryKey(), schema);
  }

  @Override
  public void close() throws IOException {
  }

  private DiscoverConnectionProfileRequest.Builder buildDiscoverConnectionProfileRequest(
    String sourceConnectionProfileName) throws IOException {
    DiscoverConnectionProfileRequest.Builder request =
      DiscoverConnectionProfileRequest.newBuilder().setParent(parentPath).setRecursive(true);

    if (sourceConnectionProfileName == null || sourceConnectionProfileName.isEmpty()) {
      return request.setConnectionProfile(Utils.buildOracleConnectionProfile("", config));
    }
    return request.setConnectionProfileName(sourceConnectionProfileName).setParent(parentPath);
  }
}
