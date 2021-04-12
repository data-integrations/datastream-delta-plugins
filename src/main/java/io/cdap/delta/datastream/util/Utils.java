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

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datastream.v1alpha1.AvroFileFormat;
import com.google.cloud.datastream.v1alpha1.ConnectionProfile;
import com.google.cloud.datastream.v1alpha1.CreateConnectionProfileRequest;
import com.google.cloud.datastream.v1alpha1.CreateStreamRequest;
import com.google.cloud.datastream.v1alpha1.DatastreamClient;
import com.google.cloud.datastream.v1alpha1.DatastreamSettings;
import com.google.cloud.datastream.v1alpha1.DestinationConfig;
import com.google.cloud.datastream.v1alpha1.DiscoverConnectionProfileRequest;
import com.google.cloud.datastream.v1alpha1.DiscoverConnectionProfileResponse;
import com.google.cloud.datastream.v1alpha1.Error;
import com.google.cloud.datastream.v1alpha1.FetchErrorsRequest;
import com.google.cloud.datastream.v1alpha1.FetchErrorsResponse;
import com.google.cloud.datastream.v1alpha1.ForwardSshTunnelConnectivity;
import com.google.cloud.datastream.v1alpha1.GcsDestinationConfig;
import com.google.cloud.datastream.v1alpha1.GcsProfile;
import com.google.cloud.datastream.v1alpha1.NoConnectivitySettings;
import com.google.cloud.datastream.v1alpha1.OperationMetadata;
import com.google.cloud.datastream.v1alpha1.OracleProfile;
import com.google.cloud.datastream.v1alpha1.OracleRdbms;
import com.google.cloud.datastream.v1alpha1.OracleSchema;
import com.google.cloud.datastream.v1alpha1.OracleSourceConfig;
import com.google.cloud.datastream.v1alpha1.OracleTable;
import com.google.cloud.datastream.v1alpha1.PrivateConnectivity;
import com.google.cloud.datastream.v1alpha1.SourceConfig;
import com.google.cloud.datastream.v1alpha1.StaticServiceIpConnectivity;
import com.google.cloud.datastream.v1alpha1.Stream;
import com.google.cloud.datastream.v1alpha1.UpdateStreamRequest;
import com.google.common.base.Joiner;
import com.google.protobuf.Duration;
import com.google.protobuf.Empty;
import com.google.protobuf.FieldMask;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static io.cdap.delta.datastream.DatastreamConfig.AUTHENTICATION_METHOD_PASSWORD;
import static io.cdap.delta.datastream.DatastreamConfig.AUTHENTICATION_METHOD_PRIVATE_PUBLIC_KEY;
import static io.cdap.delta.datastream.DatastreamConfig.CONNECTIVITY_METHOD_FORWARD_SSH_TUNNEL;
import static io.cdap.delta.datastream.DatastreamConfig.CONNECTIVITY_METHOD_IP_ALLOWLISTING;
import static io.cdap.delta.datastream.DatastreamConfig.CONNECTIVITY_METHOD_PRIVATE_CONNECTIVITY;

/**
 * Common Utils for DataStream source plugins
 */
public final class Utils {

  private static final String FIELD_STATE = "state";
  private static final String FIELD_ALLOWLIST = "source_config.oracle_source_config.allowlist";
  private static final String GCS_BUCKET_NAME_PREFIX = "df-rds-";
  private static final long FILE_ROTATION_INTERVAL_IN_SECONDS = 15L;
  private static final int FILE_ROTATIONS_SIZE_IN_MB = 1;
  private static final String ORACLE_PROFILE_NAME_PREFIX = "DF-ORA-";
  private static final String GCS_PROFILE_NAME_PREFIX = "DF-GCS-";
  private static final String STREAM_NAME_PREFIX = "DF-Stream-";
  private static final int DATASTREAM_CLIENT_POOL_SIZE = 20;
  private static final float DAASTREAM_CLIENT_POOL_LOAD_FACTOR = 0.75f;
  private static final LinkedHashMap<GoogleCredentials, DatastreamClient> datastreamClientPool =
    new LinkedHashMap<GoogleCredentials, DatastreamClient>(
      (int) (DATASTREAM_CLIENT_POOL_SIZE / DAASTREAM_CLIENT_POOL_LOAD_FACTOR), DAASTREAM_CLIENT_POOL_LOAD_FACTOR,
      true) {
      @Override
      protected boolean removeEldestEntry(java.util.Map.Entry<GoogleCredentials, DatastreamClient> eldest) {
        // Remove the eldest element whenever size of cache exceeds the capacity
        return (size() > DATASTREAM_CLIENT_POOL_SIZE);
      }
    };

  private Utils() {
  }

  /**
   * Convert the string oracle data type returned by DataStream to SQLType
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
   * Build an oracle connection profile based on DataStream delta source config
   * @oaran parentPath the parent path of the connection profile
   * @param name the name of the connection profile
   * @param config DataStream delta source config
   * @return the oracle connection profile
   */
  public static ConnectionProfile buildOracleConnectionProfile(String parentPath, @Nullable String name,
    DatastreamConfig config) {
    OracleProfile.Builder oracleProfileBuilder =
      OracleProfile.newBuilder().setHostname(config.getHost()).setUsername(config.getUser())
        .setPassword(config.getPassword()).setDatabaseService(config.getSid()).setPort(config.getPort());
    ConnectionProfile.Builder profileBuilder = ConnectionProfile.newBuilder().setOracleProfile(
      oracleProfileBuilder)
      .setDisplayName(name);
    switch (config.getConnectivityMethod()) {
      case CONNECTIVITY_METHOD_FORWARD_SSH_TUNNEL:
        ForwardSshTunnelConnectivity.Builder forwardSSHTunnelConnectivity =
          ForwardSshTunnelConnectivity.newBuilder().setHostname(config.getSshHost())
            .setPassword(config.getSshPassword()).setPort(config.getSshPort()).setUsername(config.getSshUser());
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
        return profileBuilder.setForwardSshConnectivity(forwardSSHTunnelConnectivity).build();
      case CONNECTIVITY_METHOD_IP_ALLOWLISTING:
        return profileBuilder.setStaticServiceIpConnectivity(StaticServiceIpConnectivity.getDefaultInstance()).build();
      case CONNECTIVITY_METHOD_PRIVATE_CONNECTIVITY:
        return profileBuilder.setPrivateConnectivity(PrivateConnectivity.newBuilder()
          .setPrivateConnectionName(buildPrivateConnectionPath(parentPath, config.getPrivateConnectionName()))).build();
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
   * Build a DataStream stream config
   *
   * @param parentPath the parent path of the stream to be crated
   * @param name       the name of the stream to be created
   * @param sourcePath the path of the source connection profile
   * @param targetPath the path of the target connection profile
   * @param tables     tables to be tracked changes of
   * @return the DataStream stream config
   */
  public static Stream buildStreamConfig(String parentPath, String name, String sourcePath, String targetPath,
    Set<SourceTable> tables, boolean replicateExistingData) {
    OracleSourceConfig.Builder oracleSourceConfigBuilder =
      OracleSourceConfig.newBuilder().setAllowlist(buildAllowlist(tables));
    SourceConfig.Builder sourceConfigBuilder = SourceConfig.newBuilder().setSourceConnectionProfileName(sourcePath)
      .setOracleSourceConfig(oracleSourceConfigBuilder);
    Duration.Builder durationBuilder = Duration.newBuilder().setSeconds(FILE_ROTATION_INTERVAL_IN_SECONDS);
    GcsDestinationConfig.Builder gcsDestinationConfigBuilder =
      GcsDestinationConfig.newBuilder().setAvroFileFormat(AvroFileFormat.getDefaultInstance()).setPath("/" + name)
        .setFileRotationMb(FILE_ROTATIONS_SIZE_IN_MB)
        .setFileRotationInterval(durationBuilder);
    Stream.Builder streamBuilder = Stream.newBuilder().setDisplayName(name).setDestinationConfig(
      DestinationConfig.newBuilder().setDestinationConnectionProfileName(targetPath)
        .setGcsDestinationConfig(gcsDestinationConfigBuilder)).setSourceConfig(sourceConfigBuilder);
    if (replicateExistingData) {
      streamBuilder.setBackfillAll(Stream.BackfillAllStrategy.getDefaultInstance());
    } else {
      streamBuilder.setBackfillNone(Stream.BackfillNoneStrategy.getDefaultInstance());
    }
    return streamBuilder.build();
  }

  // build an allow list of what tables to be tracked change of
  private static OracleRdbms.Builder buildAllowlist(Set<SourceTable> tables) {
    List<OracleSchema.Builder> schemas = new ArrayList<>();
    addTablesToAllowList(tables, schemas);
    return OracleRdbms.newBuilder()
      .addAllOracleSchemas(schemas.stream().map(OracleSchema.Builder::build).collect(Collectors.toList()));
  }

  private static void addTablesToExistingAllowList(Set<SourceTable> tables, List<OracleSchema.Builder> schemas) {
    // if the stream has allow list as "*.*" , the schemas list will be empty
    if (schemas.isEmpty()) {
      return;
    }
    addTablesToAllowList(tables, schemas);
  }

  private static void addTablesToAllowList(Set<SourceTable> tables, List<OracleSchema.Builder> schemas) {
    Map<String, Set<String>> schemaToTables = schemas.stream().collect(Collectors.toMap(s -> s.getSchemaName(), s ->
      // if the stream has allow list as "hr.*", then the schema name will be "hr" and oracleTables will be empty
      s.getOracleTablesList().stream().map(o -> o.getTableName()).collect(Collectors.toSet())));

    Map<String, OracleSchema.Builder> nameToSchema =
      schemas.stream().collect(Collectors.toMap(s -> s.getSchemaName(), s -> s));

    tables.forEach(table -> {
      Set<String> oracleTables = schemaToTables.get(table.getSchema());
      // if oracleTables is empty , that means all tables are allowed under this schema
      if (oracleTables != null && oracleTables.isEmpty()) {
        return;
      }
      if (oracleTables == null) {
        oracleTables = new HashSet<>();
        schemaToTables.put(table.getSchema(), oracleTables);
      }
      OracleSchema.Builder oracleSchema = nameToSchema.computeIfAbsent(table.getSchema(), name -> {
        OracleSchema.Builder newSchema = OracleSchema.newBuilder().setSchemaName(name);
        schemas.add(newSchema);
        return newSchema;
      });

      if (oracleTables.add(table.getTable())) {
        OracleTable.Builder oracleTableBuilder = OracleTable.newBuilder().setTableName(table.getTable());
        oracleSchema.addOracleTables(oracleTableBuilder);
      }
    });
  }

  /**
   * Wait until the specified operation is completed.
   *
   * @param operation the operation to wait for
   * @param request the stringfied request for this operation
   * @param logger the logger
   * @return the result that operation returns
   */
  public static <T> T waitUntilComplete(OperationFuture<T, OperationMetadata> operation, String request,
    Logger logger) {
    try {
      return operation.get();
    } catch (Exception e) {
      String errorMessage = String.format("Failed to query status of operation %s, error: %s", request, e.toString());
      throw new DatastreamDeltaSourceException(errorMessage, e, operation.getMetadata());
    }
  }

  /**
   * Build a DataStream GCS connection profile
   *
   * @param parentPath    the parent path of the connection profile to be created
   * @param name          the name of the connection profile to be created
   * @param gcsBucket     the name of GCS bucket where the stream result will be written to
   * @param gcsPathPrefix the prefix of the path for the stream result
   * @return the DataStream GCS connection profile
   */
  public static ConnectionProfile buildGcsConnectionProfile(String parentPath, String name, String gcsBucket,
    String gcsPathPrefix) {
    GcsProfile.Builder setGcsProfileBuilder =
      GcsProfile.newBuilder().setBucketName(gcsBucket).setRootPath(gcsPathPrefix);
    return ConnectionProfile.newBuilder().setDisplayName(name)
      .setNoConnectivity(NoConnectivitySettings.getDefaultInstance())
      .setGcsProfile(setGcsProfileBuilder).build();
  }

  /**
   * Build the path of a DataStream connection profile
   *
   * @param parentPath the parent path of the connection profile
   * @param name       the name of the connection profile
   * @return the path of the connection profile
   */
  public static String buildConnectionProfilePath(String parentPath, String name) {
    return String.format("%s/connectionProfiles/%s", parentPath, name);
  }

  /**
   * Build the path of a DataStream private connection
   *
   * @param parentPath the parent path of the private connection
   * @param name       the name of the private connection
   * @return the path of the private connection
   */
  public static String buildPrivateConnectionPath(String parentPath, String name) {
    return String.format("%s/privateConnections/%s", parentPath, name);
  }

  /**
   * Build the predefined DataStream Oracle connection profile name for a replicator instance
   *
   * @param replicatorId the id of a replicator
   * @return the predefined DataStream Oracle connection profile name
   */
  public static String buildOracleProfileName(String replicatorId) {
    return ORACLE_PROFILE_NAME_PREFIX + replicatorId;
  }

  /**
   * Build the predefined DataStream GCS connection profile name for a replicator instance
   *
   * @param replicatorId the id of a replicator
   * @return the predefined DataStream GCS connection profile name
   */
  public static String buildGcsProfileName(String replicatorId) {
    return GCS_PROFILE_NAME_PREFIX + replicatorId;
  }

  /**
   * Build the path of a DataStream stream
   *
   * @param parentPath the parent path of the stream
   * @param name       the name of the stream
   * @return the path of the stream
   */
  public static String buildStreamPath(String parentPath, String name) {
    return String.format("%s/streams/%s", parentPath, name);
  }

  /**
   * Build the predefined DataStream stream name for a replicator instance
   *
   * @param replicatorId the id of a replicator
   * @return the predefined DataStream source connection profile name
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
  public static Exception handleError(Logger logger, DeltaSourceContext context, String errorMessage,
    boolean recoverable) {
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
  public static Exception handleError(Logger logger, DeltaSourceContext context, String errorMessage, Exception cause,
    boolean recoverable) {
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
  public static void addToAllowList(Stream.Builder stream, Set<SourceTable> tables) {
    OracleRdbms.Builder allowlist =
      stream.getSourceConfigBuilder().getOracleSourceConfigBuilder().getAllowlistBuilder();
    List<OracleSchema.Builder> oracleSchemas = new ArrayList<>(allowlist.getOracleSchemasBuilderList());
    addTablesToAllowList(tables, oracleSchemas);
    allowlist.clearOracleSchemas()
      .addAllOracleSchemas(oracleSchemas.stream().map(OracleSchema.Builder::build).collect(Collectors.toList()));
  }

  /**
   * Builds a name for GCS bucket by prefixing the specified name with some prefix
   *
   * @param name the name of the GCS bucket
   * @return GCS bucket name by prefixing the specified name with some prefix
   */
  public static String buildBucketName(String name) {
    return GCS_BUCKET_NAME_PREFIX + name;
  }

  /**
   * Fetch errors of a stream. If the stream has any errors, return an exception that contains error message of all the
   * errors otherwise return null;
   *
   * @param datastream the DataStream client
   * @param streamPath the full stream resource path
   * @param logger     the logger
   * @param context    the delta source context
   * @throws Exception the exception that contains the error message of all the stream errors
   */

  public static Exception fetchErrors(DatastreamClient datastream, String streamPath, Logger logger,
    DeltaSourceContext context) throws IOException {
    // check stream errors
    FetchErrorsRequest request = FetchErrorsRequest.newBuilder().setStream(streamPath).build();
    String requestStr = "FetchErrors Request:\n" + request.toString();
    if (logger.isTraceEnabled()) {
      logger.trace(requestStr);
    }
    OperationFuture<FetchErrorsResponse, OperationMetadata> operation = datastream.fetchErrorsAsync(request);
    FetchErrorsResponse fetchErrorsResponse = Utils.waitUntilComplete(operation, requestStr, logger);
    if (logger.isTraceEnabled()) {
      logger.trace("FetchErrros Response:\n" + fetchErrorsResponse.toString());
    }
    if (fetchErrorsResponse != null) {
      List<Error> errors = fetchErrorsResponse.getErrorsList();
      if (!errors.isEmpty()) {
        return Utils.handleError(logger, context, errors.stream().map(error -> {
          return String.format("%s, id: %s, reason: %s", error.getMessage(), error.getErrorUuid(), error.getReason());
        }).collect(Collectors.joining("\n")), true);
      }
    }
    return null;
  }

  /**
   * Get the specified stream
   *
   * @param datastream DataStream client
   * @param path       the full stream resource path
   * @param logger     the logger
   * @return the stream with the specified path
   */
  public static Stream getStream(DatastreamClient datastream, String path, Logger logger) {
    if (logger.isDebugEnabled()) {
      logger.debug("GetStream Request:\n" + path);
    }
    Stream stream = datastream.getStream(path);
    if (logger.isDebugEnabled()) {
      logger.debug("GetStream Response:\n" + stream.toString());
    }
    return stream;
  }

  /**
   * Update the specified stream
   *
   * @param datastream DataStream client
   * @param request    update stream request
   * @param logger     the logger
   * @return the updated stream
   */
  public static Stream updateStream(DatastreamClient datastream, UpdateStreamRequest request, Logger logger) {
    String requestStr = "UpdateStream Request:\n" + request.toString();
    if (logger.isDebugEnabled()) {
      logger.debug(requestStr);
    }
    OperationFuture<Stream, OperationMetadata> operation = datastream.updateStreamAsync(request);
    Stream stream = waitUntilComplete(operation, requestStr, logger);
    if (logger.isDebugEnabled()) {
      logger.debug("UpdateStream Response:\n" + stream.toString());
    }
    return stream;
  }

  /**
   * Get the specified connection profile
   *
   * @param datastream the DataStream client
   * @param path       the full path of the connection profile resource
   * @param logger     the logger
   * @return the conneciton profile with the specified path
   */
  public static ConnectionProfile getConnectionProfile(DatastreamClient datastream, String path, Logger logger) {
    if (logger.isDebugEnabled()) {
      logger.debug("GetConnectionProfile Request:\n" + path);
    }
    ConnectionProfile connectionProfile = datastream.getConnectionProfile(path);
    if (logger.isDebugEnabled()) {
      logger.debug("GetConnectionProfile Response:\n" + connectionProfile.toString());
    }
    return connectionProfile;
  }

  /**
   * Create connection profile
   *
   * @param datastream DataStream client
   * @param reqeust    the create conneciton profile request
   * @param logger     the logger
   * @return the created connection profile
   */
  public static ConnectionProfile createConnectionProfile(DatastreamClient datastream,
    CreateConnectionProfileRequest reqeust, Logger logger) {
    String createConnectionProfileRequestStr = "CreateConnectionProfile Request:\n" + reqeust.toString();
    if (logger.isDebugEnabled()) {
      logger.debug(createConnectionProfileRequestStr);
    }
    ConnectionProfile connectionProfile =
      waitUntilComplete(datastream.createConnectionProfileAsync(reqeust), createConnectionProfileRequestStr, logger);
    if (logger.isDebugEnabled()) {
      logger.debug("CreateConnectionProfile Response:\n" + connectionProfile.toString());
    }
    return connectionProfile;
  }

  /**
   * Create stream
   *
   * @param datastream          DataStream client
   * @param createStreamRequest the create stream request
   * @param logger              the logger
   * @return the created stream
   */
  public static Stream createStream(DatastreamClient datastream, CreateStreamRequest createStreamRequest,
    Logger logger) {
    String createStreamRequestStr = "CreateStream Request:\n" + createStreamRequest.toString();
    if (logger.isDebugEnabled()) {
      logger.debug(createStreamRequestStr);
    }
    Stream stream =
      waitUntilComplete(datastream.createStreamAsync(createStreamRequest), createStreamRequestStr, logger);
    if (logger.isDebugEnabled()) {
      logger.debug("CreateStream Response:\n" + stream.toString());
    }
    return stream;
  }

  /**
   * Start the specified stream
   *
   * @param datastream DataStream client
   * @param stream     the stream to be started
   * @param logger     the logger
   * @return the started stream
   */
  public static Stream startStream(DatastreamClient datastream, Stream stream, Logger logger) {
    return updateStreamState(datastream, stream.getName(), Stream.State.RUNNING, logger);
  }

  /**
   * Pause the specified stream
   *
   * @param datastream DataStream client
   * @param stream     the stream to be paused
   * @param logger     the logger
   * @return the paused stream
   */
  public static Stream pauseStream(DatastreamClient datastream, Stream stream, Logger logger) {
    return updateStreamState(datastream, stream.getName(), Stream.State.PAUSED, logger);
  }

  private static Stream updateStreamState(DatastreamClient datastream, String streamName, Stream.State state,
    Logger logger) {
    Stream.Builder streamBuilder = Stream.newBuilder().setName(streamName).setState(state);
    FieldMask.Builder fieldMaskBuilder = FieldMask.newBuilder().addPaths(FIELD_STATE);
    return updateStream(datastream,
      UpdateStreamRequest.newBuilder().setStream(streamBuilder).setUpdateMask(fieldMaskBuilder).build(), logger);
  }

  /**
   * Update the allowlist of the specified stream
   * allowlist is a DataStream concept which means the list of tables that DataStream will stream change events from.
   * This method could throw runtime exception if you pass in a stream with name not existing.
   *
   * @param datastream DataStream client
   * @param stream     the builder of the stream to be updated
   * @param logger     the logger
   * @return the updated stream
   */
  public static Stream updateAllowlist(DatastreamClient datastream, Stream stream, Logger logger) {
    Stream.Builder streamBuilder = Stream.newBuilder().setName(stream.getName());
    streamBuilder.getSourceConfigBuilder().getOracleSourceConfigBuilder()
      .setAllowlist(stream.getSourceConfig().getOracleSourceConfig().getAllowlist());
    return updateStream(datastream, UpdateStreamRequest.newBuilder().setStream(streamBuilder)
      .setUpdateMask(FieldMask.newBuilder().addPaths(FIELD_ALLOWLIST)).build(), logger);
  }

  /**
   * Delete the specified stream
   *
   * @param datastream DataStream client
   * @param path       the full stream resource path
   * @param logger     the logger
   */
  public static void deleteStream(DatastreamClient datastream, String path, Logger logger) {
    String requestStr = "DeleteStream Request:\n" + path;
    if (logger.isDebugEnabled()) {
      logger.debug(requestStr);
    }
    OperationFuture<Empty, OperationMetadata> operation = datastream.deleteStreamAsync(path);
    Empty response = waitUntilComplete(operation, requestStr, logger);
    if (logger.isDebugEnabled()) {
      logger.debug("DeleteStream Response:\n" + response.toString());
    }
  }

  /**
   * Delete the specified connection profile
   *
   * @param datastream DataStream client
   * @param path       the full connection profile resource path
   * @param logger     the logger
   */
  public static void deleteConnectionProfile(DatastreamClient datastream, String path, Logger logger) {
    String requestStr = "DeleteConnectionProfile Request:\n" + path;
    if (logger.isDebugEnabled()) {
      logger.debug(requestStr);
    }
    OperationFuture<Empty, OperationMetadata> operation = datastream.deleteConnectionProfileAsync(path);
    Empty response = waitUntilComplete(operation, requestStr, logger);
    if (logger.isDebugEnabled()) {
      logger.debug("DeleteConnectionProfile Response:\n" + response.toString());
    }
  }

  /**
   * Discover connection profile
   *
   * @param datastream DataStream client
   * @param request    discover connection profile request
   * @param logger     the logger
   * @return the discover connection profile response
   */
  public static DiscoverConnectionProfileResponse discoverConnectionProfile(DatastreamClient datastream,
    DiscoverConnectionProfileRequest request, Logger logger) {

    if (logger.isDebugEnabled()) {
      String requestStr = "DiscoverConnectionProfile Request:\n" + request.toString();
      logger.debug(requestStr);
    }
    DiscoverConnectionProfileResponse response = datastream.discoverConnectionProfile(request);
    if (logger.isDebugEnabled()) {
      logger.debug("DiscoverConnectionProfile Response:\n" + response.toString());
    }
    return response;
  }

  /**
   * Get DataStream client given the credentials from the DataStream client pool
   * @param credentials the Google credentials for the DataStream client
   * @return the DataStream client according to the given credentials
   */
  public static DatastreamClient getDataStreamClient(GoogleCredentials credentials) throws IOException {
    DatastreamClient client = datastreamClientPool.get(credentials);
    if (client == null) {
      synchronized (Utils.class) {
        client = datastreamClientPool.get(credentials);
        if (client == null) {
          client = createDatastreamClient(credentials);
          datastreamClientPool.put(credentials, client);
        }
      }
    }
    return client;
  }

  private static DatastreamClient createDatastreamClient(GoogleCredentials credentials) throws IOException {

    return DatastreamClient.create(DatastreamSettings.newBuilder().setCredentialsProvider(new CredentialsProvider() {
      @Override
      public Credentials getCredentials() throws IOException {
        return credentials;
      }
    }).build());
  }
}
