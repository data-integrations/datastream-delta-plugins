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

package io.cdap.delta.datastream.util;

import com.google.api.services.datastream.v1alpha1.DataStream;
import com.google.api.services.datastream.v1alpha1.model.AvroFileFormat;
import com.google.api.services.datastream.v1alpha1.model.ConnectionProfile;
import com.google.api.services.datastream.v1alpha1.model.DestinationConfig;
import com.google.api.services.datastream.v1alpha1.model.ForwardSshTunnelConnectivity;
import com.google.api.services.datastream.v1alpha1.model.GcsDestinationConfig;
import com.google.api.services.datastream.v1alpha1.model.GcsProfile;
import com.google.api.services.datastream.v1alpha1.model.NoConnectivitySettings;
import com.google.api.services.datastream.v1alpha1.model.Operation;
import com.google.api.services.datastream.v1alpha1.model.OracleProfile;
import com.google.api.services.datastream.v1alpha1.model.OracleRdbms;
import com.google.api.services.datastream.v1alpha1.model.OracleSchema;
import com.google.api.services.datastream.v1alpha1.model.OracleSourceConfig;
import com.google.api.services.datastream.v1alpha1.model.OracleTable;
import com.google.api.services.datastream.v1alpha1.model.SourceConfig;
import com.google.api.services.datastream.v1alpha1.model.StaticServiceIpConnectivity;
import com.google.api.services.datastream.v1alpha1.model.Stream;
import com.google.common.base.Joiner;
import io.cdap.delta.api.DeltaFailureException;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.ReplicationError;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.datastream.DatastreamConfig;
import io.cdap.delta.datastream.OracleDataType;
import org.slf4j.Logger;

import java.io.IOException;
import java.sql.SQLType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static io.cdap.delta.datastream.DatastreamConfig.AUTHENTICATION_METHOD_PASSWORD;
import static io.cdap.delta.datastream.DatastreamConfig.AUTHENTICATION_METHOD_PRIVATE_PUBLIC_KEY;
import static io.cdap.delta.datastream.DatastreamConfig.CONNECTIVITY_METHOD_FORWARD_SSH_TUNNEL;
import static io.cdap.delta.datastream.DatastreamConfig.CONNECTIVITY_METHOD_IP_ALLOWLISTING;

/**
 * Common Utils for Datastream source plugins
 */
public final class Utils {

  private static final long FILE_ROTATION_INTERVAL_IN_SECONDS = 15L;
  private static final int FILE_ROTATIONS_SIZE_IN_MB = 1;
  private static final String ORACLE_PROFILE_NAME_PREFIX = "DF-ORA-";
  private static final String GCS_PROFILE_NAME_PREFIX = "DF-GCS-";
  private static final String STREAM_NAME_PREFIX = "DF-Stream-";

  private Utils() {
  }

  /**
   * Convert the string oracle data type returned by Datastream to SQLType
   *
   * @param oracleDataType Oracle data type in form of string
   * @return corresponding SQLType of the oracle data type
   */
  public static SQLType convertStringDataTypeToSQLType(String oracleDataType) {

    oracleDataType = oracleDataType.toUpperCase();
    if (oracleDataType.startsWith("BINARY FLOAT")) {
      return OracleDataType.BINARY_FLOAT;
    }
    if (oracleDataType.startsWith("DECIMAL")) {
      return OracleDataType.DECIMAL;
    }
    if (oracleDataType.startsWith("FLOAT")) {
      return OracleDataType.FLOAT;
    }
    if (oracleDataType.startsWith("NUMBER")) {
      return OracleDataType.NUMBER;
    }
    if (oracleDataType.startsWith("TIMESTAMP")) {
      if (oracleDataType.endsWith("WITH TIME ZONE")) {
        return OracleDataType.TIMESTAMP_WITH_TIME_ZONE;
      }
      return OracleDataType.TIMESTAMP;
    }
    switch (oracleDataType) {
      case "ANYDATA":
        return OracleDataType.ANYDATA;
      case "BFILE":
        return OracleDataType.BFILE;
      case "BINARY DOUBLE":
        return OracleDataType.BINARY_DOUBLE;
      case "BLOB":
        return OracleDataType.BLOB;
      case "CHAR":
        return OracleDataType.CHAR;
      case "CLOB":
        return OracleDataType.CLOB;
      case "DATE":
        return OracleDataType.DATE;
      case "DOUBLE PRECISION":
        return OracleDataType.DOUBLE_PRECISION;
      case "INTEGER":
        return OracleDataType.INTEGER;
      case "INTERVAL DAY TO SECOND":
        return OracleDataType.INTERVAL_DAY_TO_SECOND;
      case "INTERVAL DAY TO MONTH":
        return OracleDataType.INTERVAL_YEAR_TO_MONTH;
      case "LONG":
        return OracleDataType.LONG;
      case "LONG_RAW":
        return OracleDataType.LONG_RAW;
      case "NCHAR":
        return OracleDataType.NCHAR;
      case "NCLOB":
        return OracleDataType.NCLOB;
      case "NVARCHAR2":
        return OracleDataType.NVARCHAR2;
      case "RAW":
        return OracleDataType.RAW;
      case "REAL":
        return OracleDataType.REAL;
      case "ROWID":
        return OracleDataType.ROWID;
      case "SMALLINT":
        return OracleDataType.SMALLINT;
      case "UDT":
        return OracleDataType.UDT;
      case "VARCHAR":
        return OracleDataType.VARCHAR;
      case "VARCHAR2":
        return OracleDataType.VARCHAR2;
      case "XMLTYPE":
        return OracleDataType.XMLTYPE;
      default:
        return OracleDataType.OTHER;
    }
  }

  /**
   * Build an oracle connection profile based on Datastream delta source config
   *
   * @param name
   * @param config Datastream delta source config
   * @return the oracle connection profile
   */
  public static ConnectionProfile buildOracleConnectionProfile(@Nullable String name, DatastreamConfig config) {
    ConnectionProfile profile = new ConnectionProfile().setOracleProfile(
      new OracleProfile().setHostname(config.getHost()).setUsername(config.getUser()).setPassword(config.getPassword())
        .setDatabaseService(config.getSid()).setPort(config.getPort())).setDisplayName(name);
    switch (config.getConnectivityMethod()) {
      case CONNECTIVITY_METHOD_FORWARD_SSH_TUNNEL:
        ForwardSshTunnelConnectivity forwardSSHTunnelConnectivity =
          new ForwardSshTunnelConnectivity().setHostname(config.getSshHost()).setPassword(config.getSshPassword())
            .setPort(config.getSshPort()).setUsername(config.getSshUser());
        switch (config.getSshAuthenticationMethod()) {
          case AUTHENTICATION_METHOD_PASSWORD:
            forwardSSHTunnelConnectivity.setPassword(config.getSshPassword());
            break;
          case AUTHENTICATION_METHOD_PRIVATE_PUBLIC_KEY:
            forwardSSHTunnelConnectivity.setPrivateKey(config.getSshPrivateKey());
            break;
          default:
            throw new IllegalArgumentException(
              "Unsupported authentication method: " + config.getSshAuthenticationMethod());
        }
        return profile.setForwardSshConnectivity(forwardSSHTunnelConnectivity);
      case CONNECTIVITY_METHOD_IP_ALLOWLISTING:
        return profile.setStaticServiceIpConnectivity(new StaticServiceIpConnectivity());
      default:
        throw new IllegalArgumentException("Unsupported connectivity method: " + config.getConnectivityMethod());
    }
  }


  /**
   * Build the parent path of a stream based on the region of the stream
   *
   * @param project the project in which the stream is in
   * @param region  the region of the stream
   * @return parent path of the stream
   */
  public static String buildParentPath(String project, String region) {
    return String.format("projects/%s/locations/%s", project, region);
  }

  /**
   * Build a Datastream stream config
   *
   * @param parentPath the parent path of the stream to be crated
   * @param name       the name of the stream to be created
   * @param sourcePath the path of the source connection profile
   * @param targetPath the path of the target connection profile
   * @param tables     tables to be tracked changes of
   * @return the Datastream stream config
   */
  public static Stream buildStreamConfig(String parentPath, String name, String sourcePath, String targetPath,
    Set<SourceTable> tables) {
    return new Stream().setDisplayName(name).setDestinationConfig(
      new DestinationConfig().setDestinationConnectionProfileName(targetPath).setGcsDestinationConfig(
        new GcsDestinationConfig().setAvroFileFormat(new AvroFileFormat()).setPath("/" + name)
          .setFileRotationMb(FILE_ROTATIONS_SIZE_IN_MB)
          .setFileRotationInterval(FILE_ROTATION_INTERVAL_IN_SECONDS + "s"))).setSourceConfig(
      new SourceConfig().setSourceConnectionProfileName(sourcePath)
        .setOracleSourceConfig(new OracleSourceConfig().setAllowlist(buildAllowlist(tables))));
  }

  // build an allow list of what tables to be tracked change of
  private static OracleRdbms buildAllowlist(Set<SourceTable> tables) {
    List<OracleSchema> schemas = new ArrayList<>();
    addTablesToAllowList(tables, schemas);
    return new OracleRdbms().setOracleSchemas(schemas);
  }

  private static void addTablesToAllowList(Set<SourceTable> tables, @Nullable List<OracleSchema> schemas) {
    // if the stream has allow list as "*.*" , the schemas will be null
    if (schemas == null) {
      return;
    }
    Map<String, Set<String>> schemaToTables = schemas.stream().collect(Collectors.toMap(s -> s.getSchemaName(),
      s -> {
        List<OracleTable> oracleTables = s.getOracleTables();
        // if the stream has allow list as "hr.*", then the schema name will be "hr" and oracleTables will be null
        if (oracleTables == null) {
          return Collections.emptySet();
        }
        return oracleTables.stream().map(o -> o.getTableName()).collect(Collectors.toSet());
      }));
    Map<String, OracleSchema> nameToSchema = schemas.stream().collect(Collectors.toMap(s -> s.getSchemaName(), s -> s));

    tables.forEach(table -> {
      Set<String> oracleTables = schemaToTables.computeIfAbsent(table.getSchema(), name -> new HashSet<>());
      OracleSchema oracleSchema = nameToSchema.computeIfAbsent(table.getSchema(), name -> {
        OracleSchema newSchema = new OracleSchema().setSchemaName(name);
        schemas.add(newSchema);
        newSchema.setOracleTables(new ArrayList<>());
        return newSchema;
      });
      // if oracleSchema.getOracleTables() is null it means it allows all tables under this schema
      if (oracleSchema.getOracleTables() != null && !oracleTables.contains(table.getTable())) {
        oracleSchema.getOracleTables().add(new OracleTable().setTableName(table.getTable()));
        oracleTables.add(table.getTable());
      }
    });
  }

  /**
   * Wait until the specified operation is completed.
   *
   * @param operation the operation to wait for
   * @return the refreshed operation with latest status
   */
  public static Operation waitUntilComplete(DataStream datastream, Operation operation, Logger logger) {
    if (operation == null) {
      return null;
    }
    try {
      while (!operation.getDone()) {
        TimeUnit.MILLISECONDS.sleep(200L);
        operation = datastream.projects().locations().operations().get(operation.getName()).execute();
      }
    } catch (Exception e) {
      throw new DatastreamDeltaSourceException(
        String.format("Failed to query status of operation: %s", operation.toString()), e);
    }
    if (operation.getError() != null) {
      throw new DatastreamDeltaSourceException(String
        .format("Operation %s failed with error code :%s and error message: %s", operation.toString(),
          operation.getError().getCode(), operation.getError().getMessage()));
    }
    return operation;
  }

  /**
   * Build a Datastream GCS connection profile
   *
   * @param parentPath    the parent path of the connection profile to be created
   * @param name          the name of the connection profile to be created
   * @param gcsBucket     the name of GCS bucket where the stream result will be written to
   * @param gcsPathPrefix the prefix of the path for the stream result
   * @return the Datastream GCS connection profile
   */
  public static ConnectionProfile buildGcsConnectionProfile(String parentPath, String name, String gcsBucket,
    String gcsPathPrefix) {
    return new ConnectionProfile().setDisplayName(name).setNoConnectivity(new NoConnectivitySettings())
      .setGcsProfile(new GcsProfile().setBucketName(gcsBucket).setRootPath(gcsPathPrefix));
  }

  /**
   * Build the path of a Datastream connection profile
   *
   * @param parentPath the parent path of the connection profile
   * @param name       the name of the connection profile
   * @return the path of the connection profile
   */
  public static String buildConnectionProfilePath(String parentPath, String name) {
    return String.format("%s/connectionProfiles/%s", parentPath, name);
  }

  /**
   * Build the predefined Datastream Oracle connection profile name for a replicator instance
   *
   * @param replicatorId the id of a replicator
   * @return the predefined Datastream Oracle connection profile name
   */
  public static String buildOracleProfileName(String replicatorId) {
    return ORACLE_PROFILE_NAME_PREFIX + replicatorId;
  }

  /**
   * Build the predefined Datastream GCS connection profile name for a replicator instance
   *
   * @param replicatorId the id of a replicator
   * @return the predefined Datastream GCS connection profile name
   */
  public static String buildGcsProfileName(String replicatorId) {
    return GCS_PROFILE_NAME_PREFIX + replicatorId;
  }

  /**
   * Build the path of a Datastream stream
   *
   * @param parentPath the parent path of the stream
   * @param name       the name of the stream
   * @return the path of the stream
   */
  public static String buildStreamPath(String parentPath, String name) {
    return String.format("%s/streams/%s", parentPath, name);
  }

  /**
   * Build the predefined Datastream stream name for a replicator instance
   *
   * @param replicatorId the id of a replicator
   * @return the predefined Datastream source connection profile name
   */
  public static String buildStreamName(String replicatorId) {
    return STREAM_NAME_PREFIX + replicatorId;
  }

  /**
   * Build the stringed form of an id for a replicator
   *
   * @param context the delta source context
   * @return the stringed form of an id for the replicator
   */
  public static String buildReplicatorId(DeltaSourceContext context) {
    DeltaPipelineId pipelineId = context.getPipelineId();
    return pipelineId.getNamespace() + "-" + pipelineId.getApp() + "-" + pipelineId.getGeneration();
  }

  /**
   * Log the error with corresponding error message and construct the corresponding runtime exception with this message
   *
   * @param logger       the logger to log the error
   * @param context      the Delta source context
   * @param errorMessage the error message
   * @param recoverable  whether the error is recoverable
   * @return the runtime exception constructed from the error message
   */
  public static Exception handleError(Logger logger, DeltaSourceContext context,
    String errorMessage, boolean recoverable) {
    Exception e;
    if (recoverable) {
      e = new DatastreamDeltaSourceException(errorMessage);
    } else {
      e = new DeltaFailureException(errorMessage);
    }
    setError(logger, context, e);
    return e;
  }

  /**
   * Log the error with corresponding error message and the exception of the cause of the error and construct the
   * runtime exception with this message and cause
   *
   * @param logger       the logger to log the error
   * @param context      the Delta source context
   * @param errorMessage the error message
   * @param cause        the exception for the cause of error
   * @param recoverable  whether the error is recoverable
   * @return the runtime exception constructed from the error message and the casue
   */
  public static Exception handleError(Logger logger, DeltaSourceContext context,
    String errorMessage, Exception cause, boolean recoverable) {
    setError(logger, context, cause);
    if (recoverable) {
      return new DatastreamDeltaSourceException(errorMessage, cause);
    }
    return new DeltaFailureException(errorMessage, cause);
  }

  /**
   * Set the error in the Delta source context
   *
   * @param logger  the logger to log the error
   * @param context the Delta source context
   * @param cause   the exception for the cause
   */
  public static void setError(Logger logger, DeltaSourceContext context, Exception cause) {
    try {
      context.setError(new ReplicationError(cause));
    } catch (IOException ioException) {
      logger.warn("Unable to set error for source status!", cause);
    }
  }

  /**
   * Build a table name with schema name as prefix if schema name is not null
   *
   * @param schema name of the schema where the table is in
   * @param table  name of the table
   * @return a composite table name prefixed with schema name if schema name is not null
   */
  public static String buildCompositeTableName(String schema, String table) {
    return Joiner.on("_").skipNulls().join(schema, table);
  }

  /**
   * Adds the specified tables to the allowlist of the stream
   *
   * @param stream the stream the specified tables will be added to
   * @param tables the tables to be added to the allowlist of the stream
   */
  public static void addToAllowList(Stream stream, Set<SourceTable> tables) {
    addTablesToAllowList(tables, stream.getSourceConfig().getOracleSourceConfig().getAllowlist().getOracleSchemas());
  }
}
