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
import com.google.api.gax.rpc.AbortedException;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.FailedPreconditionException;
import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datastream.v1.AvroFileFormat;
import com.google.cloud.datastream.v1.ConnectionProfile;
import com.google.cloud.datastream.v1.CreateConnectionProfileRequest;
import com.google.cloud.datastream.v1.CreateStreamRequest;
import com.google.cloud.datastream.v1.DatastreamClient;
import com.google.cloud.datastream.v1.DatastreamSettings;
import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.DiscoverConnectionProfileRequest;
import com.google.cloud.datastream.v1.DiscoverConnectionProfileResponse;
import com.google.cloud.datastream.v1.ForwardSshTunnelConnectivity;
import com.google.cloud.datastream.v1.GcsDestinationConfig;
import com.google.cloud.datastream.v1.GcsProfile;
import com.google.cloud.datastream.v1.OperationMetadata;
import com.google.cloud.datastream.v1.OracleProfile;
import com.google.cloud.datastream.v1.OracleRdbms;
import com.google.cloud.datastream.v1.OracleSchema;
import com.google.cloud.datastream.v1.OracleSourceConfig;
import com.google.cloud.datastream.v1.OracleTable;
import com.google.cloud.datastream.v1.PrivateConnectivity;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.StaticServiceIpConnectivity;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.datastream.v1.StreamObject;
import com.google.cloud.datastream.v1.UpdateStreamRequest;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
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
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.Fallback;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;

import java.io.IOException;
import java.sql.SQLType;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static io.cdap.delta.datastream.DatastreamConfig.AUTHENTICATION_METHOD_PASSWORD;
import static io.cdap.delta.datastream.DatastreamConfig.AUTHENTICATION_METHOD_PRIVATE_PUBLIC_KEY;
import static io.cdap.delta.datastream.DatastreamConfig.CONNECTIVITY_METHOD_FORWARD_SSH_TUNNEL;
import static io.cdap.delta.datastream.DatastreamConfig.CONNECTIVITY_METHOD_IP_ALLOWLISTING;
import static io.cdap.delta.datastream.DatastreamConfig.CONNECTIVITY_METHOD_PRIVATE_CONNECTIVITY;

/**
 * Common Utils for Datastream source plugins
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
  private static final int GCS_ERROR_CODE_CONFLICT = 409;
  private static final int GCS_ERROR_CODE_INVALID_REQUEST = 400;
  private static final int DATASTREAM_CLIENT_POOL_SIZE = 20;
  private static final float DATASTREAM_CLIENT_POOL_LOAD_FACTOR = 0.75f;
  private static final LinkedHashMap<GoogleCredentials, DatastreamClient> datastreamClientPool =
    new LinkedHashMap<GoogleCredentials, DatastreamClient>(
      (int) (DATASTREAM_CLIENT_POOL_SIZE / DATASTREAM_CLIENT_POOL_LOAD_FACTOR), DATASTREAM_CLIENT_POOL_LOAD_FACTOR,
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
      case "INTERVAL YEAR TO MONTH":
        return OracleDataType.INTERVAL_YEAR_TO_MONTH;
      case "LONG":
        return OracleDataType.LONG;
      case "LONG RAW":
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
   * @oaran parentPath the parent path of the connection profile
   * @param name the name of the connection profile
   * @param config Datastream delta source config
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
          .setPrivateConnection(buildPrivateConnectionPath(parentPath, config.getPrivateConnectionName()))).build();
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
                                         Set<SourceTable> tables, boolean replicateExistingData) {
    OracleSourceConfig.Builder oracleSourceConfigBuilder =
      OracleSourceConfig.newBuilder().setIncludeObjects(buildAllowlist(tables));
    SourceConfig.Builder sourceConfigBuilder = SourceConfig.newBuilder().setSourceConnectionProfile(sourcePath)
      .setOracleSourceConfig(oracleSourceConfigBuilder);
    Duration.Builder durationBuilder = Duration.newBuilder().setSeconds(FILE_ROTATION_INTERVAL_IN_SECONDS);
    GcsDestinationConfig.Builder gcsDestinationConfigBuilder =
      GcsDestinationConfig.newBuilder().setAvroFileFormat(AvroFileFormat.getDefaultInstance()).setPath("/" + name)
        .setFileRotationMb(FILE_ROTATIONS_SIZE_IN_MB)
        .setFileRotationInterval(durationBuilder);
    Stream.Builder streamBuilder = Stream.newBuilder().setDisplayName(name).setDestinationConfig(
      DestinationConfig.newBuilder().setDestinationConnectionProfile(targetPath)
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
    Map<String, Set<String>> schemaToTables = schemas.stream().collect(Collectors.toMap(s -> s.getSchema(), s ->
      // if the stream has allow list as "hr.*", then the schema name will be "hr" and oracleTables will be empty
      s.getOracleTablesList().stream().map(o -> o.getTable()).collect(Collectors.toSet())));

    Map<String, OracleSchema.Builder> nameToSchema =
      schemas.stream().collect(Collectors.toMap(s -> s.getSchema(), s -> s));

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
        OracleSchema.Builder newSchema = OracleSchema.newBuilder().setSchema(name);
        schemas.add(newSchema);
        return newSchema;
      });

      if (oracleTables.add(table.getTable())) {
        OracleTable.Builder oracleTableBuilder = OracleTable.newBuilder().setTable(table.getTable());
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
      String errorMessage = String.format("Failed to query status of operation %s.", request);
      // log exception as debug because the operation could be retried. Let the outmost error handling code to log
      // error level log.
      logger.debug(errorMessage, e);
      throw new DatastreamDeltaSourceException(errorMessage, e, operation.getMetadata());
    }
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
    GcsProfile.Builder setGcsProfileBuilder =
      GcsProfile.newBuilder().setBucket(gcsBucket).setRootPath(gcsPathPrefix);
    return ConnectionProfile.newBuilder().setDisplayName(name)
      .setGcsProfile(setGcsProfileBuilder).build();
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
   * Build the path of a Datastream private connection
   *
   * @param parentPath the parent path of the private connection
   * @param name       the name of the private connection
   * @return the path of the private connection
   */
  public static String buildPrivateConnectionPath(String parentPath, String name) {
    return String.format("%s/privateConnections/%s", parentPath, name);
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
   * Build the exception with error messages specified based on whether it's recoverable
   *
   * @param errorMessage the error message
   * @param recoverable  whether the error is recoverable
   * @return the runtime exception constructed from the error message
   */
  public static Exception buildException(String errorMessage, boolean recoverable) {
    Exception e;
    if (recoverable) {
      e = new DatastreamDeltaSourceException(errorMessage);
    } else {
      e = new DeltaFailureException(errorMessage);
    }
    return e;
  }

  /**
   * Build the exception with error messages and cause specified based on whether it's recoverable
   *
   * @param errorMessage the error message
   * @param cause        the exception for the cause of error
   * @param recoverable  whether the error is recoverable
   * @return the runtime exception constructed from the error message and the casue
   */
  public static Exception buildException(String errorMessage, Exception cause, boolean recoverable) {
    if (recoverable) {
      return new DatastreamDeltaSourceException(errorMessage, cause);
    }
    return new DeltaFailureException(errorMessage, cause);
  }

  /**
   * Set the error in the Delta source context and the log the errors
   *
   * @param logger  the logger to log the error
   * @param context the Delta source context
   * @param cause   the exception for the cause
   */
  public static void handleError(Logger logger, DeltaSourceContext context, Throwable cause) {
    logger.error("Datastream event reader encounter errors.", cause);
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
      stream.getSourceConfigBuilder().getOracleSourceConfigBuilder().getIncludeObjectsBuilder();
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
   * Get the specified stream
   *
   * @param datastream Datastream client
   * @param path       the full stream resource path
   * @param logger     the logger
   * @return the stream with the specified path
   */
  public static Stream getStream(DatastreamClient datastream, String path, Logger logger) {
    if (logger.isDebugEnabled()) {
      logger.debug("GetStream Request:\n" + path);
    }
    Stream stream =
      Failsafe.with(Utils.<Stream>createDataStreamRetryPolicy()
        // if a stream is being created, it will still be returned with empty stream config
        // so need to retry until it's fully created.
        .handleResultIf(result -> !result.hasSourceConfig())).get(() -> datastream.getStream(path));
    if (logger.isDebugEnabled()) {
      logger.debug("GetStream Response:\n" + stream.toString());
    }
    return stream;
  }

  /**
   * Continuously to get stream until stream state equals the specified target state or max duration reached (5 minutes)
   * @param datastream the Datastream client
   * @param state the specified target state
   * @param path the full resource path of the stream
   * @param logger the logger
   * @return the stream to get
   */
  public static Stream getStreamUntilStateEquals(DatastreamClient datastream, Stream.State state, String path,
    Logger logger) {
    return Failsafe.with(Utils.<Stream>createDataStreamRetryPolicy()
      //no need to retry on exception
      .handleIf(e -> false)
      .handleResultIf(result -> result.getState() != state)).get(() -> getStream(datastream, path, logger));
  }

  /**
   * Update the specified stream
   *
   * @param datastream Datastream client
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
    Stream stream =
      Failsafe.with(createDataStreamRetryPolicy()).get(() -> waitUntilComplete(operation, requestStr, logger));
    if (logger.isDebugEnabled()) {
      logger.debug("UpdateStream Response:\n" + stream.toString());
    }
    return stream;
  }

  /**
   * Get the specified connection profile
   *
   * @param datastream the Datastream client
   * @param path       the full path of the connection profile resource
   * @param logger     the logger
   * @return the conneciton profile with the specified path
   */
  public static ConnectionProfile getConnectionProfile(DatastreamClient datastream, String path, Logger logger) {
    if (logger.isDebugEnabled()) {
      logger.debug("GetConnectionProfile Request:\n" + path);
    }
    ConnectionProfile connectionProfile = Failsafe.with(Utils.<ConnectionProfile>createDataStreamRetryPolicy()
      // If a connection profile is being created, it will still be return with empty profile
      // so need to retry until conneciton profile is fully created.
      .handleResultIf(result -> !result.hasGcsProfile() && !result.hasOracleProfile()))
      .get(() -> datastream.getConnectionProfile(path));
    if (logger.isDebugEnabled()) {
      logger.debug("GetConnectionProfile Response:\n" + connectionProfile.toString());
    }
    return connectionProfile;
  }

  /**
   * Create connection profile if not existing
   *
   * @param datastream Datastream client
   * @param reqeust    the create conneciton profile request
   * @param logger     the logger
   * @return whether the connection profile is actually created by this method
   */
  public static boolean createConnectionProfileIfNotExisting(DatastreamClient datastream,
                                                             CreateConnectionProfileRequest reqeust, Logger logger) {
    String createConnectionProfileRequestStr = "CreateConnectionProfile Request:\n" + reqeust.toString();

    if (logger.isDebugEnabled()) {
      logger.debug(createConnectionProfileRequestStr);
    }
    ConnectionProfile connectionProfile = null;
    boolean created = true;
    try {
      connectionProfile = Failsafe.with(createDataStreamRetryPolicy()).get(
        () -> waitUntilComplete(datastream.createConnectionProfileAsync(reqeust), createConnectionProfileRequestStr,
                                logger));
    } catch (Exception ce) {
      if (!isAlreadyExisted(ce)) {
        throw ce;
      }
      created = false;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("CreateConnectionProfile Response:\n" + connectionProfile);
    }
    return created;
  }

  /**
   * Create stream if not existing
   *
   * @param datastream          Datastream client
   * @param createStreamRequest the create stream request
   * @param logger              the logger
   * @return whether the stream is actually created by this method
   */
  public static boolean createStreamIfNotExisting(DatastreamClient datastream, CreateStreamRequest createStreamRequest,
                                                 Logger logger) {
    String createStreamRequestStr = "CreateStream Request:\n" + createStreamRequest.toString();
    if (logger.isDebugEnabled()) {
      logger.debug(createStreamRequestStr);
    }
    Stream stream = null;
    boolean created = true;
    try {
      stream = Failsafe.with(createDataStreamRetryPolicy()).get(
        () -> waitUntilComplete(datastream.createStreamAsync(createStreamRequest), createStreamRequestStr, logger));
    } catch (Exception ex) {
      if (!Utils.isAlreadyExisted(ex)) {
        throw ex;
      }
      created = false;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("CreateStream Response:\n" + stream);
    }
    return created;
  }

  /**
   * Start the specified stream
   *
   * @param datastream Datastream client
   * @param stream     the stream to be started
   * @param logger     the logger
   * @return the started stream
   */
  public static Stream startStream(DatastreamClient datastream, Stream stream, Logger logger) {
    return updateStreamState(datastream, stream.getName(), Stream.State.RUNNING, Stream.State.STARTING, logger);
  }

  /**
   * Pause the specified stream
   *
   * @param datastream Datastream client
   * @param stream     the stream to be paused
   * @param logger     the logger
   * @return the paused stream
   */
  public static Stream pauseStream(DatastreamClient datastream, Stream stream, Logger logger) {
    return updateStreamState(datastream, stream.getName(), Stream.State.PAUSED, Stream.State.DRAINING, logger);
  }

  private static Stream updateStreamState(DatastreamClient datastream, String streamName, Stream.State state,
                                          Stream.State transitionState, Logger logger) {
    Stream.Builder streamBuilder = Stream.newBuilder().setName(streamName).setState(state);
    FieldMask.Builder fieldMaskBuilder = FieldMask.newBuilder().addPaths(FIELD_STATE);

    // TODO below is a workaround for datastream only supporting 1 queued
    //  operation. once the issue is resolve we can remove those workaround
    // a predicate that indicate that we try to start/pause a stream when it is already in the progress of
    // starting/pausing
    Predicate<? extends Throwable> streamBeingChangedPredicate = t -> t.getCause() instanceof ExecutionException &&
            // If there is already an operation queued, an AbortedException will be thrown
            // Check if the stream is in one of transition or target state
            (t.getCause().getCause() instanceof AbortedException) &&
            streamStateIn(datastream, streamName, ImmutableSet.of(state, transitionState), logger);

    return Failsafe.with(
            // fall back to get stream until stream state reaches target state
            Fallback.of(() -> getStreamUntilStateEquals(datastream, state, streamName, logger))
                    .handleIf(streamBeingChangedPredicate)).get(() -> updateStream(datastream,
            UpdateStreamRequest.newBuilder().setStream(streamBuilder).setUpdateMask(fieldMaskBuilder).build(), logger));
  }

  private static boolean streamStateIn(DatastreamClient datastream, String streamName, Set<Stream.State> validStates,
                                       Logger logger) {
    Stream stream = getStream(datastream, streamName, logger);
    boolean isStateValid = validStates.contains(stream.getState());
    if (!isStateValid) {
      logger.error("Stream {} is not in valid state {}", streamName, stream.getState());
    }
    return isStateValid;
  }

  /**
   * Update the allowlist of the specified stream
   * allowlist is a Datastream concept which means the list of tables that Datastream will stream change events from.
   * This method could throw runtime exception if you pass in a stream with name not existing.
   *
   * @param datastream Datastream client
   * @param stream     the builder of the stream to be updated
   * @param validateOnly only validate the changes to the stream
   * @param logger     the logger
   * @return the updated stream
   */
  public static Stream updateAllowlist(DatastreamClient datastream, Stream stream, boolean validateOnly,
                                        Logger logger) {
    Stream.Builder streamBuilder = Stream.newBuilder().setName(stream.getName());
    streamBuilder.getSourceConfigBuilder().getOracleSourceConfigBuilder()
      .setIncludeObjects(stream.getSourceConfig().getOracleSourceConfig().getIncludeObjects());
    UpdateStreamRequest request = UpdateStreamRequest.newBuilder().setStream(streamBuilder)
            .setUpdateMask(FieldMask.newBuilder().addPaths(FIELD_ALLOWLIST))
            .setValidateOnly(validateOnly).build();
    return updateStream(datastream, request, logger);
  }

  /**
   * Delete the specified stream
   *
   * @param datastream Datastream client
   * @param path       the full stream resource path
   * @param logger     the logger
   */
  public static void deleteStream(DatastreamClient datastream, String path, Logger logger) {
    String requestStr = "DeleteStream Request:\n" + path;
    if (logger.isDebugEnabled()) {
      logger.debug(requestStr);
    }
    OperationFuture<Empty, OperationMetadata> operation = datastream.deleteStreamAsync(path);
    Empty response = null;
    try {
      response =
        Failsafe.with(createDataStreamRetryPolicy()).get(() -> waitUntilComplete(operation, requestStr, logger));
    } catch (NotFoundException e) {
      logger.warn("Stream {} to be deleted does not exit.", path);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("DeleteStream Response:\n" + response);
    }
  }

  /**
   * Delete the specified connection profile
   *
   * @param datastream Datastream client
   * @param path       the full connection profile resource path
   * @param logger     the logger
   */
  public static void deleteConnectionProfile(DatastreamClient datastream, String path, Logger logger) {
    String requestStr = "DeleteConnectionProfile Request:\n" + path;
    if (logger.isDebugEnabled()) {
      logger.debug(requestStr);
    }
    OperationFuture<Empty, OperationMetadata> operation = datastream.deleteConnectionProfileAsync(path);
    Empty response = null;
    try {
      response =
        Failsafe.with(createDataStreamRetryPolicy()).get(() -> waitUntilComplete(operation, requestStr, logger));
    } catch (NotFoundException e) {
      logger.warn("ConnectionProfile {} to be deleted does not exit.", path);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("DeleteConnectionProfile Response:\n" + response);
    }
  }

  /**
   * Discover connection profile
   *
   * @param datastream Datastream client
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

    DiscoverConnectionProfileResponse response =
      Failsafe.with(createDataStreamRetryPolicy()).get(() -> datastream.discoverConnectionProfile(request));

    if (logger.isDebugEnabled()) {
      logger.debug("DiscoverConnectionProfile Response:\n" + response.toString());
    }
    return response;
  }

  /**
   * Create a GCS bucket if it doesn't exist
   * @param storage the GCS client
   * @param bucketName the name of the bucket to be created
   * @param location location where bucket will be created
   * @return whether the GCS bucket is newly created by this method
   */
  public static boolean createBucketIfNotExisting(Storage storage, String bucketName,
                                                  String location) throws IOException {
    // create corresponding GCS bucket
    try {
      Failsafe.with(createRetryPolicy()
                      .abortOn(t -> t instanceof StorageException && (
                          ((StorageException) t).getCode() == GCS_ERROR_CODE_CONFLICT ||
                          ((StorageException) t).getCode() == GCS_ERROR_CODE_INVALID_REQUEST)
                      ))
        .run(() -> {
          BucketInfo.Builder builder = BucketInfo.newBuilder(bucketName);
          if (location != null && !location.trim().isEmpty()) {
            builder.setLocation(location);
          }
          storage.create(builder.build());
        });
    } catch (StorageException se) {
      // It is possible that in multiple worker instances scenario
      // bucket is created by another worker instance after this worker instance
      // determined that the bucket does not exists. Ignore error if bucket already exists.
      if (se.getCode() != GCS_ERROR_CODE_CONFLICT) {
        throw se;
      }
      return false;
    }
    return true;
  }

  /**
   * Delete the specified GCS bucket
   * @param storage the GCS client
   * @param bucketName the name of the GCS bucket to be deleted
   */
  public static void deleteBucket(Storage storage, String bucketName) {
    Failsafe.with(createRetryPolicy()).run(() -> storage.delete(bucketName));
  }

  private static <T> RetryPolicy<T> createDataStreamRetryPolicy() {
    return Utils.<T>createRetryPolicy()
      .abortOn(t ->
        t instanceof NotFoundException ||
        t instanceof InvalidArgumentException ||
        t.getCause() instanceof ExecutionException && (
        // connection profile already exists
        t.getCause().getCause() instanceof AlreadyExistsException ||
        // create argument is not correct
        t.getCause().getCause() instanceof IllegalArgumentException ||
        // validation failed
        t.getCause().getCause() instanceof FailedPreconditionException));
  }

  private static <T> RetryPolicy<T> createRetryPolicy() {
    return new RetryPolicy<T>().withMaxAttempts(Integer.MAX_VALUE)
            .withMaxDuration(java.time.Duration.of(5, ChronoUnit.MINUTES)).withBackoff(1, 60, ChronoUnit.SECONDS);
  }

  /**
   * Check whether the exception was thrown due to validation failure
   * @param e the exception to be checked
   * @return true if the exception was thrown due to validation failure
   */
  public static boolean isValidationFailed(Throwable e) {
    while (e != null) {
      if (e instanceof FailedPreconditionException) {
        return true;
      }
      if (e == e.getCause()) {
        return false;
      }
      e = e.getCause();
    }
    return false;
  }


  /**
   * Check whether the exception was thrown due to resource already existed
   * @param e the exception to be checked
   * @return true if the exception was thrown due to resource already existed
   */
  public static boolean isAlreadyExisted(Throwable e) {
    while (e != null) {
      if (e instanceof AlreadyExistsException) {
        return true;
      }
      if (e == e.getCause()) {
        return false;
      }
      e = e.getCause();
    }
    return false;
  }

  /**
   * Get Datastream client given the credentials from the Datastream client pool
   * @param credentials the Google credentials for the Datastream client
   * @return the Datastream client according to the given credentials
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

  /**
   * Get all StreamObjects associated to a Stream (There will be one StreamObject associated to each table)
   * @param datastreamClient
   * @param streamPath
   * @param logger
   * @return
   */
  public static List<StreamObject> getStreamObjects(DatastreamClient datastreamClient,
                                                    String streamPath, Logger logger) {
    DatastreamClient.ListStreamObjectsPagedResponse listStreamObjectsPagedResponse =
      Failsafe.with(Utils.<DatastreamClient.ListStreamObjectsPagedResponse>createRetryPolicy())
        .get(() -> datastreamClient.listStreamObjects(streamPath));

    if (logger.isDebugEnabled()) {
      logger.debug("ListStreamObjects Response : \n");
    }
    List<StreamObject> streamObjects = new ArrayList<>();
    for (StreamObject streamObject : listStreamObjectsPagedResponse.iterateAll()) {
      streamObjects.add(streamObject);
      if (logger.isDebugEnabled()) {
        logger.debug(streamObject.toString());
      }
    }
    return streamObjects;
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
