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


import com.google.api.core.ApiFuture;
import com.google.cloud.datastream.v1.CreateConnectionProfileRequest;
import com.google.cloud.datastream.v1.CreateStreamRequest;
import com.google.cloud.datastream.v1.DatastreamClient;
import com.google.cloud.datastream.v1.OperationMetadata;
import com.google.cloud.datastream.v1.OracleRdbms;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.datastream.v1.Validation;
import com.google.cloud.datastream.v1.ValidationMessage;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.LogicalType;
import io.cdap.cdap.api.data.schema.Schema.Type;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.api.assessment.Assessment;
import io.cdap.delta.api.assessment.ColumnAssessment;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.ColumnSuggestion;
import io.cdap.delta.api.assessment.ColumnSupport;
import io.cdap.delta.api.assessment.Problem;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.datastream.util.DatastreamDeltaSourceException;
import io.cdap.delta.datastream.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.cdap.delta.datastream.util.Utils.buildOracleConnectionProfile;
import static io.cdap.delta.datastream.util.Utils.buildStreamPath;

/**
 * Datastream table assessor.
 */
public class DatastreamTableAssessor implements TableAssessor<TableDetail> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatastreamTableAssessor.class);
  private static final Gson GSON = new Gson();
  static final String PRECISION = "PRECISION";
  static final String SCALE = "SCALE";
  private final DatastreamConfig conf;
  private final DatastreamClient datastream;
  private final Storage storage;
  private final List<SourceTable> tables;

  DatastreamTableAssessor(DatastreamConfig conf, DatastreamClient datastream, Storage storage,
    List<SourceTable> tables) {
    this.conf = conf;
    this.datastream = datastream;
    this.storage = storage;
    this.tables = tables;
  }

  static ColumnEvaluation evaluateColumn(ColumnDetail detail) {
    Schema schema;
    OracleDataType oracleDataType = (OracleDataType) detail.getType();
    ColumnSupport support = ColumnSupport.YES;
    ColumnSuggestion suggestion = null;
    Map<String, String> properties = detail.getProperties();
    String precision = properties.get(PRECISION);
    String scale = properties.get(SCALE);

    // convert oracle data type to CDAP schema
    // ref: https://cloud.google.com/datastream/docs/unified-types
    switch (oracleDataType) {
      case BFILE:
      case CHAR:
      case NCHAR:
      case NVARCHAR2:
      case ROWID:
      case VARCHAR:
      case VARCHAR2:
        schema = Schema.of(Type.STRING);
        break;
      case REAL:
        schema = Schema.of(Type.FLOAT);
        break;
      case BINARY_DOUBLE:
      case DOUBLE_PRECISION:
      case FLOAT:
        schema = Schema.of(Type.DOUBLE);
        break;
      case BINARY_FLOAT:
        schema = Schema.of(Type.FLOAT);
        break;
      case RAW:
        schema = Schema.of(Type.BYTES);
        break;
      case DATE:
      case TIMESTAMP:
        schema = Schema.of(LogicalType.TIMESTAMP_MICROS);
        break;
      case DECIMAL:
        schema =
          Schema.decimalOf(parseInt(oracleDataType, "precision", precision), parseInt(oracleDataType, "scale", scale));
        break;
      case INTEGER:
      case SMALLINT:
        schema = Schema.of(Type.INT);
        break;
      case NUMBER:
        if (precision == null || precision.isEmpty()) {
          schema = Schema.of(Type.STRING);
        } else {
          if (scale == null || scale.isEmpty() || parseInt(oracleDataType, "scale", scale) <= 0) {
            if ("*".equals(precision) || parseInt(oracleDataType, "precision", precision) > 18) {
              schema = Schema.of(Type.STRING);
            } else {
              schema = Schema.of(Type.LONG);
            }
          } else {
            if ("*".equals(precision)) {
              schema = Schema
                .decimalOf(38, parseInt(oracleDataType, "scale", scale));
            } else {
              schema = Schema
                .decimalOf(parseInt(oracleDataType, "precision", precision), parseInt(oracleDataType, "scale", scale));
            }
          }
        }
        break;
      case TIMESTAMP_WITH_TIME_ZONE:
        schema = Schema.recordOf("timestampTz", Schema.Field.of("timestampTz", Schema.of(LogicalType.TIMESTAMP_MICROS)),
          Schema.Field.of("offset", Schema.of(LogicalType.TIMESTAMP_MILLIS)));
        break;
      default:
        support = ColumnSupport.NO;
        suggestion = new ColumnSuggestion("Unsupported Oracle Data Type: " + detail.getType(), Collections.emptyList());
        schema = null;
    }

    Schema.Field field = schema == null ? null :
      Schema.Field.of(detail.getName(), detail.isNullable() ? Schema.nullableOf(schema) : schema);
    ColumnAssessment assessment =
      ColumnAssessment.builder(detail.getName(), detail.getType().getName()).setSupport(support)
        .setSuggestion(suggestion).build();
    return new ColumnEvaluation(field, assessment);
  }

  private static int parseInt(OracleDataType type, String property, String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
        String.format("Oracle datatype % should have %s as number , but got %s", type.getName(), property, value), e);
    }
  }

  @Override
  public Assessment assess() {
    String parentPath = Utils.buildParentPath(conf.getProject(), conf.getRegion());
    if (conf.isUsingExistingStream()) {
      // validate stream by updating existing stream
      String streamPath = Utils.buildStreamPath(parentPath, conf.getStreamId());
      Stream.Builder stream = null;
      try {
        stream = Utils.getStream(datastream, streamPath, LOGGER).toBuilder();
      } catch (Exception e) {
        throw new RuntimeException(
          String.format("Fail to assess replicator pipeline due to failure of getting existing stream %s:\n%s",
                        streamPath, e.getLocalizedMessage()), e);
      }

      OracleRdbms originalAllowList = stream.getSourceConfig().getOracleSourceConfig().getIncludeObjects();
      Utils.addToAllowList(stream, new HashSet<>(tables));
      try {
        Utils.updateAllowlist(datastream, stream.build(), true, LOGGER);
      } catch (DatastreamDeltaSourceException e) {
        if (Utils.isValidationFailed(e)) {
          return buildAssessment(e.getMetadata());
        }
        throw new RuntimeException(String.format("Fail to assess replicator pipeline due to failure of updating " +
                                                   "existing stream %s:\n%s", streamPath, e.getLocalizedMessage()), e);
      }
    } else {
      // validate by creating new stream
      String oracleProfilePath = null;
      String gcsProfilePath = null;
      String streamPath = null;
      String bucketName = null;
      boolean bucketCreated = false;

      try {
        String uuid = UUID.randomUUID().toString() + System.currentTimeMillis();
        String oracleProfileName = Utils.buildOracleProfileName(uuid);
        try {
          // create the oracle profile
          CreateConnectionProfileRequest createConnectionProfileRequest =
            CreateConnectionProfileRequest.newBuilder().setParent(parentPath)
              .setConnectionProfile(buildOracleConnectionProfile(parentPath, oracleProfileName, conf))
              .setConnectionProfileId(oracleProfileName).build();
          if (Utils.createConnectionProfileIfNotExisting(datastream, createConnectionProfileRequest, LOGGER)) {
            oracleProfilePath = Utils.buildConnectionProfilePath(parentPath, oracleProfileName);
          }
        } catch (Exception e) {
          throw new RuntimeException(String.format("Fail to assess replicator pipeline due to failure of creating " +
                                                     "source connection profile:\n%s", e.getLocalizedMessage()), e);
        }

        bucketName = conf.getGcsBucket();
        if (bucketName == null || bucketName.trim().isEmpty()) {
          bucketName = Utils.buildBucketName(uuid);
        }
        // create corresponding GCS bucket
        try {
          bucketCreated = Utils.createBucketIfNotExisting(storage, bucketName, conf.getGcsBucketLocation());
        } catch (Exception e) {
          throw new RuntimeException(String.format("Fail to assess replicator pipeline due to failure of creating GCS" +
                                                     " Bucket:\n%s.", e.getLocalizedMessage()), e);
        }
        // create the gcs connection profile
        String gcsProfileName = Utils.buildGcsProfileName(uuid);
        try {
          CreateConnectionProfileRequest createConnectionProfileRequest =
            CreateConnectionProfileRequest.newBuilder().setParent(parentPath).setConnectionProfile(
              Utils.buildGcsConnectionProfile(parentPath, gcsProfileName, bucketName, conf.getGcsPathPrefix()))
              .setConnectionProfileId(gcsProfileName).build();
          if (Utils.createConnectionProfileIfNotExisting(datastream, createConnectionProfileRequest, LOGGER)) {
            gcsProfilePath = Utils.buildConnectionProfilePath(parentPath, gcsProfileName);
          }
        } catch (Exception e) {
          throw new RuntimeException(
            String.format("Fail to assess replicator pipeline due to failure of creating destination connection " +
                            "profile:\n%s", e.getLocalizedMessage()), e);
        }

        try {
          String streamName = Utils.buildStreamName(uuid);
          CreateStreamRequest createStreamRequest = CreateStreamRequest.newBuilder().setParent(parentPath).setStream(
            Utils.buildStreamConfig(parentPath, streamName, oracleProfilePath, gcsProfilePath, new HashSet<>(tables),
              conf.shouldReplicateExistingData()))
                  .setValidateOnly(true)
                  .setStreamId(streamName).build();
          if (Utils.createStreamIfNotExisting(datastream, createStreamRequest, LOGGER)) {
            streamPath = buildStreamPath(parentPath, streamName);
          }
        } catch (Exception e) {
          if (Utils.isValidationFailed(e)) {
            return buildAssessment(((DatastreamDeltaSourceException) e).getMetadata());
          }
          throw new RuntimeException(String.format("Fail to assess replicator pipeline due to failure of creating " +
                                                     "stream:\n%s", e.getLocalizedMessage()), e);
        }
      } finally {
        clearTempResources(oracleProfilePath, gcsProfilePath, streamPath, bucketName, bucketCreated);
      }
    }
    return new Assessment(Collections.emptyList(), Collections.emptyList());
  }

  private void clearTempResources(String oracleProfilePath, String gcsProfilePath, String streamPath, String bucketName,
    boolean bucketCreated) {
    //clear temporary connectionProfile
    if (oracleProfilePath != null) {
      try {
        Utils.deleteConnectionProfile(datastream, oracleProfilePath, LOGGER);
      } catch (Exception e) {
        LOGGER.warn(String.format("Fail to delete temporary connection profile : %s", oracleProfilePath), e);
      }
    }
    if (gcsProfilePath != null) {
      try {
        Utils.deleteConnectionProfile(datastream, gcsProfilePath, LOGGER);
      } catch (Exception e) {
        LOGGER.warn(String.format("Fail to delete temporary connection profile : %s", gcsProfilePath), e);
      }
    }
    //remove temporarily created GCS bucket
    if (bucketCreated) {
      try {
        Utils.deleteBucket(storage, bucketName);
      } catch (StorageException e) {
        LOGGER.warn(String.format("Fail to delete temporary GCS bucket : %s", bucketName), e);
      }
    }
  }

  @VisibleForTesting
  static Assessment buildAssessment(ApiFuture<OperationMetadata> metadataFuture) {
    OperationMetadata metadata = null;
    try {
      metadata = metadataFuture.get();
    } catch (Exception e) {
      throw new RuntimeException(String.format("Fail to assess replicator pipeline due to failure of getting " +
                                                 "operation metadata:\n%s", e.getLocalizedMessage()), e);
    }

    LOGGER.error("Stream validation failed : {}", metadata);

    List<Problem> connectivityIssues = new ArrayList<>();
    List<Problem> missingFeatures = new ArrayList<>();
    for (Validation validation : metadata.getValidationResult().getValidationsList()) {
      Validation.State state = validation.getState();
      if (Validation.State.FAILED.equals(state)) {
        String code = validation.getCode();
        String description = validation.getDescription();
        String message = String.join("\n",
          validation.getMessageList().stream().map(ValidationMessage::getMessage).collect(Collectors.toList()));
        switch (code) {
          case "ORACLE_VALIDATE_TUNNEL_CONNECTIVITY":
            connectivityIssues.add(new Problem("Oracle Connectivity Failure",
              String.format("Issue : %s found when %s", message, description),
              "Check your Forward SSH tunnel configurations.",
              "Cannot read any snapshot or CDC changes from source database."));
            break;
          case "ORACLE_VALIDATE_CONNECTIVITY":
            connectivityIssues.add(new Problem("Oracle Connectivity Failure",
              String.format("Issue : %s found when %s", message, description), "Check your Oracle database settings.",
              "Cannot replicate any snapshot or CDC changes from source database."));
            break;
          case "ORACLE_VALIDATE_LOG_MODE":
            missingFeatures.add(
              new Problem("Incorrect Oracle Settings", String.format("Issue : %s found when %s", message, description),
                "Check your Oracle database settings.", "Cannot replicate CDC changes from source database."));
            break;
          case "ORACLE_VALIDATE_SUPPLEMENTAL_LOGGING":
            missingFeatures.add(
              new Problem("Incorrect Oracle Settings", String.format("Issue : %s found when %s", message, description),
                "Check your Oracle database settings.",
                "Cannot replicate CDC changes of certain tables from source database."));
            break;
          case "GCS_VALIDATE_PERMISSIONS":
            missingFeatures.add(
              new Problem("GCS Permission Issue", String.format("Issue : %s found when %s", message, description),
                "Check your GCS permissions.", "Cannot replicate any snapshot or CDC changes from source database."));
            break;
          default:
            LOGGER
              .warn("Unknown validation failure : {} with description {} and message {}.", code, description, message);
            missingFeatures.add(
              new Problem("General Issue", String.format("Issue : %s found when %s", message, description), "N/A",
                "Unknown"));
        }
      }
    }
    return new Assessment(missingFeatures, connectivityIssues);
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public TableAssessment assess(TableDetail tableDetail) {
    List<ColumnAssessment> columnAssessments = new ArrayList<>();
    for (ColumnDetail columnDetail : tableDetail.getColumns()) {
      columnAssessments.add(evaluateColumn(columnDetail).getAssessment());
    }
    return new TableAssessment(columnAssessments, tableDetail.getFeatures());
  }
}
