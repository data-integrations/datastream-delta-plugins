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


import com.google.api.services.datastream.v1alpha1.DataStream;
import com.google.api.services.datastream.v1alpha1.model.Operation;
import com.google.api.services.datastream.v1alpha1.model.OracleSchema;
import com.google.api.services.datastream.v1alpha1.model.Stream;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
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

import static io.cdap.delta.datastream.util.Utils.buildOracleConnectionProfile;
import static io.cdap.delta.datastream.util.Utils.buildStreamPath;
import static io.cdap.delta.datastream.util.Utils.waitUntilComplete;

/**
 * Datastream table assessor.
 */
public class DatastreamTableAssessor implements TableAssessor<TableDetail> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatastreamTableAssessor.class);
  private static final Gson GSON = new Gson();
  static final String PRECISION = "PRECISION";
  static final String SCALE = "SCALE";
  private final DatastreamConfig conf;
  private final DataStream datastream;
  private final Storage storage;
  private final List<SourceTable> tables;

  DatastreamTableAssessor(DatastreamConfig conf, DataStream datastream, Storage storage, List<SourceTable> tables) {
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
      case BINARY_DOUBLE:
      case DOUBLE_PRECISION:
        schema = Schema.of(Type.DOUBLE);
        break;
      case REAL:
        schema = Schema.of(Type.FLOAT);
        break;
      case FLOAT:
      case BINARY_FLOAT:
        if (parseInt(oracleDataType, "precision", precision) <= 23) {
          schema = Schema.of(Type.FLOAT);
        } else {
          schema = Schema.of(Type.DOUBLE);
        }
        break;
      case RAW:
        schema = Schema.of(Type.BYTES);
        break;
      case DATE:
      case TIMESTAMP:
        schema = Schema.of(LogicalType.TIMESTAMP_MICROS);
        break;
      case DECIMAL:
        schema = Schema.decimalOf(parseInt(oracleDataType, "precision", precision),
                                  parseInt(oracleDataType, "scale", scale));
        break;
      case INTEGER:
      case SMALLINT:
        schema = Schema.of(Type.INT);
        break;
      case NUMBER:
        if (precision == null || precision.isEmpty()) {
          schema = Schema.of(Type.STRING);
        } else {
          if (scale == null || scale.isEmpty() || parseInt(oracleDataType, "scale", scale) == 0) {
            schema = Schema.of(Type.LONG);
          } else {
            schema = Schema.decimalOf(parseInt(oracleDataType, "precision", precision),
                                      parseInt(oracleDataType, "scale", scale));
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

    Schema.Field field = schema == null ? null : Schema.Field.of(detail.getName(),
                                                                 detail.isNullable() ? Schema.nullableOf(schema) :
                                                                   schema);
    ColumnAssessment assessment = ColumnAssessment.builder(detail.getName(), detail.getType().getName()).setSupport(
      support).setSuggestion(suggestion).build();
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
    Operation streamValidationOperation;
    if (conf.isUsingExistingStream()) {
      String streamPath = Utils.buildStreamPath(parentPath, conf.getStreamId());
      Stream stream = null;
      try {
        stream = datastream.projects().locations().streams().get(streamPath).execute();
      } catch (IOException e) {
        throw new RuntimeException(
          String.format("Fail to assess replicator pipeline due to failure of getting existing stream %s ", streamPath),
          e);
      }

      List<OracleSchema> allowList = new ArrayList<>(
        stream.getSourceConfig().getOracleSourceConfig().getAllowlist().getOracleSchemas());
      Utils.addToAllowList(stream, new HashSet<>(tables));
      //TODO set validate only to true when datastream supports validate only correctly
      try {
        DataStream.Projects.Locations.Streams.Patch update = datastream.projects().locations().streams().patch(
          streamPath, stream);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Validating stream when through update stream API, request : {} ", GSON.toJson(update));
        }
        streamValidationOperation = update.execute();
      } catch (IOException e) {
        throw new RuntimeException(String.format(
          "Fail to assess replicator pipeline due to failure of updating existing stream %s ", streamPath), e);
      }
      waitUntilComplete(datastream, streamValidationOperation, LOGGER, true);

      // rollback changes of the update which was for validation
      //TODO below rollback logic can be removed once datastream supports validate only correctly
      stream.getSourceConfig().getOracleSourceConfig().getAllowlist().setOracleSchemas(allowList);
      try {
        Operation operation = datastream.projects().locations().streams().patch(streamPath, stream).execute();
        waitUntilComplete(datastream, operation, LOGGER, true);
      } catch (IOException e) {
        LOGGER.error(String.format("Fail to rollback the update of existing stream %s after validating", streamPath),
                     e);
      }

    } else {
      // creating new stream
      String uuid = UUID.randomUUID().toString() + System.currentTimeMillis();
      String streamName = Utils.buildStreamName(uuid);

      String oracleProfileName = Utils.buildOracleProfileName(uuid);
      // crete the oraclprofile
      Operation operation = null;
      try {
        operation = datastream.projects().locations().connectionProfiles().create(parentPath,
                                                                                  buildOracleConnectionProfile(
                                                                                    oracleProfileName, conf))
          .setConnectionProfileId(oracleProfileName).execute();
      } catch (IOException e) {
        throw new RuntimeException(
          "Fail to assess replicator pipeline due to failure of creating source connection profile.", e);
      }
      waitUntilComplete(datastream, operation, LOGGER);

      String gcsProfileName = Utils.buildGcsProfileName(uuid);
      String bucketName = conf.getGcsBucket();
      if (bucketName == null) {
        bucketName = Utils.buildBucketName(uuid);
      }
      Bucket bucket = storage.get(bucketName);
      boolean newBucketCreated = false;
      if (bucket == null) {
        newBucketCreated = true;
        // create corresponding GCS bucket
        bucket = storage.create(BucketInfo.newBuilder(bucketName).build());
      }
      // crete the gcs connection profile
      try {
        operation = datastream.projects().locations().connectionProfiles().create(parentPath, Utils
          .buildGcsConnectionProfile(parentPath, gcsProfileName, bucketName, conf.getGcsPathPrefix()))
          .setConnectionProfileId(gcsProfileName).execute();
      } catch (IOException e) {
        throw new RuntimeException(
          "Fail to assess replicator pipeline due to failure of creating destination connection profile.", e);
      }
      waitUntilComplete(datastream, operation, LOGGER);

      String gcsProfilePath = Utils.buildConnectionProfilePath(parentPath, gcsProfileName);
      String oracleProfilePath = Utils.buildConnectionProfilePath(parentPath, oracleProfileName);

      // validate stream
      try {
        //TODO set validate only to true when datastream supports validate only correctly
        DataStream.Projects.Locations.Streams.Create create = datastream.projects().locations().streams().create(
          parentPath,
          Utils.buildStreamConfig(parentPath, streamName, oracleProfilePath, gcsProfilePath, new HashSet<>(tables)))
          .setStreamId(streamName);
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Validating stream through create stream API, request : {} ", GSON.toJson(create));
        }
        streamValidationOperation = create.execute();
      } catch (IOException e) {
        throw new RuntimeException("Fail to assess replicator pipeline due to failure of creating stream.", e);
      }
      waitUntilComplete(datastream, streamValidationOperation, LOGGER, true);

      //clear temporary stream
      //TODO below logic for clearing temporary stream can be removed once datastream supports validate only correctly
      Operation streamDeletionOperation = null;
      String streamPath = buildStreamPath(parentPath, streamName);
      try {
        streamDeletionOperation = datastream.projects().locations().connectionProfiles().delete(streamPath).execute();
      } catch (IOException e) {
        LOGGER.warn(String.format("Fail to delete temporary stream : %s", streamPath), e);
      }
      waitUntilComplete(datastream, streamDeletionOperation, LOGGER, true);

      //clear temporary connectionProfile
      Operation sourceProfileDeletionOperation = null;
      try {
        sourceProfileDeletionOperation = datastream.projects().locations().connectionProfiles().delete(
          oracleProfilePath).execute();
      } catch (IOException e) {
        LOGGER.warn(String.format("Fail to delete temporary connection profile : %s", oracleProfilePath), e);
      }
      Operation destinationProfileDeletionOperation = null;
      try {
        destinationProfileDeletionOperation = datastream.projects().locations().connectionProfiles().delete(
          gcsProfilePath).execute();
      } catch (IOException e) {
        LOGGER.warn(String.format("Fail to delete temporary connection profile : %s", gcsProfilePath), e);
      }
      waitUntilComplete(datastream, sourceProfileDeletionOperation, LOGGER, true);
      waitUntilComplete(datastream, destinationProfileDeletionOperation, LOGGER, true);

      //remove temporarily created GCS bucket
      if (newBucketCreated) {
        bucket.delete();
      }
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Stream validation response : {}", GSON.toJson(streamValidationOperation));
    }
    if (streamValidationOperation.getError() == null) {
      return new Assessment(Collections.emptyList(), Collections.emptyList());
    }
    Map<String, Object> metadata = streamValidationOperation.getMetadata();
    List<Problem> missingFeatures = new ArrayList<>();
    List<Problem> connectivityIssues = new ArrayList<>();

    List<Object> validations = (List<Object>) ((Map<String, Object>) metadata.get("validationResult")).get(
      "validations");
    for (Object validation : validations) {
      String status = (String) ((Map<String, Object>) validation).get("status");
      if ("FAILED".equals(status)) {
        String code = (String) ((Map<String, Object>) validation).get("code");
        String description = (String) ((Map<String, Object>) validation).get("description");
        String message = (String) ((Map<String, Object>) ((List<Object>) ((Map<String, Object>) validation).get(
          "message")).get(0)).get("message");
        switch (code) {
          case "ORACLE_VALIDATE_TUNNEL_CONNECTIVITY":
            connectivityIssues.add(new Problem("Oracle Connectivity Failure",
                                               String.format("Issue : %s found when %s", message, description),
                                               "Check your Forward SSH tunnel configurations.",
                                               "Cannot read any snapshot or CDC changes" + "from source database."));
            break;
          case "ORACLE_VALIDATE_CONNECTIVITY":
            connectivityIssues.add(new Problem("Oracle Connectivity Failure",
                                               String.format("Issue : %s found when %s", message, description),
                                               "Check your Oracle database settings.",
                                               "Cannot replicate any snapshot or CDC changes " + "from " +
                                                 "source database."));
            break;
          case "ORACLE_VALIDATE_LOG_MODE":
            missingFeatures.add(
              new Problem("Incorrect Oracle settings.", String.format("Issue : %s found when %s", message, description),
                          "Check your Oracle database settings.",
                          "Cannot replicate CDC changes from source " + "database."));
            break;
          case "ORACLE_VALIDATE_SUPPLEMENTAL_LOGGING":
            missingFeatures.add(
              new Problem("Incorrect Oracle settings.", String.format("Issue : %s found when %s", message, description),
                          "Check your Oracle database settings.",
                          "Cannot replicate CDC changes of certain tables " + "from source database."));
            break;
          case "GCP_VALIDATE_PERMISSIONS":
            missingFeatures.add(
              new Problem("GCS Permission Issue", String.format("Issue : %s found when %s", message, description),
                          "Check your GCS permissions.",
                          "Cannot replicate any snapshot or CDC changes from source database."));
            break;
          default:
            LOGGER.warn("Unknown validation failure : {} with description {} and message {}.", code, description,
                        message);
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
