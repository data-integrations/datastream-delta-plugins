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

import com.google.cloud.ServiceOptions;
import com.google.cloud.datastream.v1alpha1.ConnectionProfile;
import com.google.cloud.datastream.v1alpha1.CreateConnectionProfileRequest;
import com.google.cloud.datastream.v1alpha1.CreateStreamRequest;
import com.google.cloud.datastream.v1alpha1.DestinationConfig;
import com.google.cloud.datastream.v1alpha1.ForwardSshTunnelConnectivity;
import com.google.cloud.datastream.v1alpha1.GcsDestinationConfig;
import com.google.cloud.datastream.v1alpha1.GcsFileFormat;
import com.google.cloud.datastream.v1alpha1.GcsProfile;
import com.google.cloud.datastream.v1alpha1.NoConnectivitySettings;
import com.google.cloud.datastream.v1alpha1.OracleProfile;
import com.google.cloud.datastream.v1alpha1.OracleRdbms;
import com.google.cloud.datastream.v1alpha1.OracleSchema;
import com.google.cloud.datastream.v1alpha1.OracleSourceConfig;
import com.google.cloud.datastream.v1alpha1.OracleTable;
import com.google.cloud.datastream.v1alpha1.SourceConfig;
import com.google.cloud.datastream.v1alpha1.StaticServiceIpConnectivity;
import com.google.cloud.datastream.v1alpha1.Stream;
import com.google.protobuf.Duration;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.datastream.DatastreamConfig;
import io.cdap.delta.datastream.OracleDataType;

import java.sql.SQLType;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.cdap.delta.datastream.DatastreamConfig.AUTHENTICATION_METHOD_PASSWORD;
import static io.cdap.delta.datastream.DatastreamConfig.AUTHENTICATION_METHOD_PRIVATE_PUBLIC_KEY;
import static io.cdap.delta.datastream.DatastreamConfig.CONNECTIVITY_METHOD_FORWARD_SSH_TUNNEL;
import static io.cdap.delta.datastream.DatastreamConfig.CONNECTIVITY_METHOD_IP_ALLOWLISTING;

/**
 *  Common Utils for Datastream source plugins
 */
public final class Utils {

  private static final long FILE_ROTATION_INTERVAL_IN_SECONDS = 15L;
  private static final int FILE_ROTATIONS_SIZE_IN_MB = 1;
  private static final String SOURCE_PROFILE_NAME_PREFIX = "CDF-Src-";
  private static final String TARGET_PROFILE_NAME_PREFIX = "CDF-Tgt-";
  private static final String STREAM_NAME_PREFIX = "CDF-Stream-";

  private Utils() { };

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
      case "TIMESTAMP":
        return OracleDataType.TIMESTAMP;
      case "TIMESTAMP WITH TIME ZONE":
        return OracleDataType.TIMESTAMP_WITH_TIME_ZONE;
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
   * @param config Datastream delta source config
   * @return teh oracle connection profile builder
   */
  public static ConnectionProfile.Builder buildOracleConnectionProfile(DatastreamConfig config) {
    ConnectionProfile.Builder profileBuilder = ConnectionProfile.newBuilder().setOracleProfile(
      OracleProfile.newBuilder().setHostname(config.getHost()).setUsername(config.getUser())
        .setPassword(config.getPassword()).setDatabaseService(config.getSid()).setPort(config.getPort()));
    switch (config.getConnectivityMethod()) {
      case CONNECTIVITY_METHOD_FORWARD_SSH_TUNNEL:
        ForwardSshTunnelConnectivity.Builder forwardSSHTunnelConnectivityBuilder =
          ForwardSshTunnelConnectivity.newBuilder().setHostname(config.getSshHost())
            .setPassword(config.getSshPassword()).setPort(config.getSshPort())
            .setUsername(config.getSshUser());
        switch (config.getSshAuthenticationMethod()) {
          case AUTHENTICATION_METHOD_PASSWORD:
            forwardSSHTunnelConnectivityBuilder.setPassword(config.getSshPassword());
            break;
          case AUTHENTICATION_METHOD_PRIVATE_PUBLIC_KEY:
            forwardSSHTunnelConnectivityBuilder.setPrivateKey(config.getSshPrivateKey());
            break;
          default:
            throw new IllegalArgumentException(
              "Unsupported authentication method: " + config.getSshAuthenticationMethod());
        }
        return profileBuilder.setForwardSshConnectivity(forwardSSHTunnelConnectivityBuilder);
      case CONNECTIVITY_METHOD_IP_ALLOWLISTING:
        return profileBuilder
          .setStaticServiceIpConnectivity(StaticServiceIpConnectivity.getDefaultInstance());
      default:
        throw new IllegalArgumentException(
          "Unsupported connectivity method: " + config.getConnectivityMethod());
    }
  }


  /**
   * Build the parent path of a stream based on the region of the stream
   * @param region the region of the stream
   * @return parent path of the stream
   */
  public static String buildParentPath(String region) {
    return String.format("projects/%s/locations/%s", ServiceOptions.getDefaultProjectId(), region);
  }

  /**
   * Build a Datastream create stream request
   * @param parentPath the parent path of the stream to be crated
   * @param name the name of the stream to be created
   * @param sourcePath the path of the source connection profile
   * @param targetPath the path of the target connection profile
   * @param tables tables to be tracked changes of
   * @return the Datastream create stream request
   */
  public static CreateStreamRequest buildStreamCreationRequest(String parentPath, String name, String sourcePath,
    String targetPath, Set<SourceTable> tables) {
    return CreateStreamRequest.newBuilder().setParent(parentPath).setStreamId(name).setStream(
      Stream.newBuilder().setDisplayName(name).setDestinationConfig(
        DestinationConfig.newBuilder().setDestinationConnectionProfileName(targetPath).setGcsDestinationConfig(
          GcsDestinationConfig.newBuilder().setGcsFileFormat(GcsFileFormat.AVRO).setPath("/" + name)
            .setFileRotationMb(FILE_ROTATIONS_SIZE_IN_MB)
            .setFileRotationInterval(Duration.newBuilder().setSeconds(FILE_ROTATION_INTERVAL_IN_SECONDS))))
        .setSourceConfig(SourceConfig.newBuilder().setSourceConnectionProfileName(sourcePath)
          .setOracleSourceConfig(OracleSourceConfig.newBuilder().setAllowlist(buildAllowlist(tables))))).build();
  }

  // build an allow list of what tables to be tracked change of
  private static OracleRdbms buildAllowlist(Set<SourceTable> tables) {
    OracleRdbms.Builder rdbms = OracleRdbms.newBuilder();
    Map<String, OracleSchema.Builder> schemaToTables = new HashMap<>();
    // TODO decide whether we should filter tables here, because it will make the stream hard to reuse
    // But datastream claims to support modifying a stream without stoping it
    tables.forEach(
      table -> schemaToTables.computeIfAbsent(table.getSchema(), name -> OracleSchema.newBuilder().setSchemaName(name))
        .addOracleTables(OracleTable.newBuilder().setTableName(table.getTable())));
    schemaToTables.values().forEach(rdbms::addOracleSchemas);
    return rdbms.build();
  }

  /**
   * Wait until the specified future is completed or cancelled.
   * @param future the future to wait for
   */
  public static void waitUntilComplete(Future<?> future) {
    while (!future.isDone() && !future.isCancelled()) {
      try {
        TimeUnit.MILLISECONDS.sleep(200L);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Build a Datastream create connection profile request for stream target
   * @param parentPath the parent path of the connection profile to be created
   * @param name the name of the connection profile to be created
   * @param gcsBucket the name of GCS bucket where the stream result will be written to
   * @param gcsPathPrefix the prefix of the path for the stream result
   * @return the Datastream create connection profile request for the target
   */
  public static CreateConnectionProfileRequest buildTargetProfileCreationRequest(String parentPath, String name,
    String gcsBucket, String gcsPathPrefix) {
    return CreateConnectionProfileRequest.newBuilder().setParent(parentPath).setConnectionProfileId(name)
      .setConnectionProfile(ConnectionProfile.newBuilder().setDisplayName(name)
        .setNoConnectivity(NoConnectivitySettings.getDefaultInstance()).setGcsProfile(
          GcsProfile.newBuilder().setBucketName(gcsBucket).setRootPath(gcsPathPrefix))).build();
  }

  /**
   * Build a Datastream create connection profile request for stream source
   * @param parentPath the parent path of the connection profile to be created
   * @param name the name of the connection profile to be created
   * @param connectionProfile the connection profile to be created
   * @return the Datastream create connection profile request for the source
   */
  public static CreateConnectionProfileRequest buildSourceProfileCreationRequest(String parentPath, String name,
    ConnectionProfile.Builder connectionProfile) {
    return CreateConnectionProfileRequest.newBuilder().setParent(parentPath).setConnectionProfileId(name)
      .setConnectionProfile(connectionProfile.setDisplayName(name)).build();
  }

  /**
   * Build the path of a Datastream connection profile
   * @param parentPath the parent path of the connection profile
   * @param name the name of the connection profile
   * @return the path of the connection profile
   */
  public static String buildConnectionProfilePath(String parentPath, String name) {
    return String.format("%s/connectionProfiles/%s", parentPath, name);
  }

  /**
   * Build the predefined Datastream source connection profile name for a replicator instance
   * @param replicatorId the id of a replicator
   * @return the predefined Datastream source connection profile name
   */
  public static String buildSourceProfileName(String replicatorId) {
    return SOURCE_PROFILE_NAME_PREFIX + replicatorId;
  }

  /**
   * Build the predefined Datastream target connection profile name for a replicator instance
   * @param replicatorId the id of a replicator
   * @return the predefined Datastream target connection profile name
   */
  public static String buildTargetProfileName(String replicatorId) {
    return TARGET_PROFILE_NAME_PREFIX + replicatorId;
  }

  /**
   * Build the path of a Datastream stream
   * @param parentPath the parent path of the stream
   * @param name the name of the stream
   * @return the path of the stream
   */
  public static String buildStreamPath(String parentPath, String name) {
    return String.format("%s/streams/%s", parentPath, name);
  }

  /**
   * Build the predefined Datastream stream name for a replicator instance
   * @param replicatorId the id of a replicator
   * @return the predefined Datastream source connection profile name
   */
  public static String buildStreamName(String replicatorId) {
    return STREAM_NAME_PREFIX + replicatorId;
  }

  /**
   * Build the stringed form of an id for a replicator
   * @param context the delta source context
   * @return the stringed form of an id for the replicator
   */
  public static String buildReplicatorId(DeltaSourceContext context) {
    DeltaPipelineId pipelineId = context.getPipelineId();
    return pipelineId.getNamespace() + "-" + pipelineId.getApp() + "-" + pipelineId.getGeneration();
  }
}
