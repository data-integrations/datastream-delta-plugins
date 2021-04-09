/*
 *
 * Copyright © 2021 Cask Data, Inc.
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
import com.google.cloud.datastream.v1alpha1.OperationMetadata;
import com.google.cloud.datastream.v1alpha1.Validation;
import com.google.cloud.datastream.v1alpha1.ValidationMessage;
import com.google.cloud.datastream.v1alpha1.ValidationResult;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.api.assessment.Assessment;
import io.cdap.delta.api.assessment.ColumnAssessment;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.ColumnSuggestion;
import io.cdap.delta.api.assessment.Problem;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.cdap.delta.api.assessment.ColumnSupport.NO;
import static io.cdap.delta.api.assessment.ColumnSupport.YES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DatastreamAsessorTest extends BaseIntegrationTestCase {
  @Test
  public void testAssess_new() throws Exception {
    testAssess(false);
  }

  @Test
  public void testAssess_existing() throws Exception {
    testAssess(true);
  }

  @Test
  public void testAssessTable_new() throws Exception {
    testAssessTable(false);
  }

  @Test
  public void testAssessTable_existing() throws Exception {
    testAssessTable(true);
  }

  @Test
  public void testBuildAssessment() throws Exception {
    Assessment assessment = DatastreamTableAssessor.buildAssessment(new ApiFuture<OperationMetadata>() {
      @Override
      public void addListener(Runnable runnable, Executor executor) {
      }

      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
      }

      @Override
      public boolean isCancelled() {
        return false;
      }

      @Override
      public boolean isDone() {
        return false;
      }

      @Override
      public OperationMetadata get() throws InterruptedException, ExecutionException {
        Validation.Builder messageBuilder = Validation.newBuilder().setStatus(Validation.Status.FAILED);
        return OperationMetadata.newBuilder().setValidationResult(ValidationResult.newBuilder().addValidations(
          messageBuilder.setCode("ORACLE_VALIDATE_TUNNEL_CONNECTIVITY").setDescription("OVTC-description")
            .clearMessage().addMessage(ValidationMessage.newBuilder().setMessage("OVTC-message1"))
            .addMessage(ValidationMessage.newBuilder().setMessage("OVTC-message2"))).addValidations(
          messageBuilder.setCode("ORACLE_VALIDATE_CONNECTIVITY").setDescription("OVC-description").clearMessage()
            .addMessage(ValidationMessage.newBuilder().setMessage("OVC-message1"))
            .addMessage(ValidationMessage.newBuilder().setMessage("OVC-message2"))).addValidations(
          messageBuilder.setCode("ORACLE_VALIDATE_LOG_MODE").setDescription("OVLM-description").clearMessage()
            .addMessage(ValidationMessage.newBuilder().setMessage("OVLM-message1"))
            .addMessage(ValidationMessage.newBuilder().setMessage("OVLM-message2"))).addValidations(
          messageBuilder.setCode("ORACLE_VALIDATE_SUPPLEMENTAL_LOGGING").setDescription("OVSL-description")
            .clearMessage().addMessage(ValidationMessage.newBuilder().setMessage("OVSL-message1"))
            .addMessage(ValidationMessage.newBuilder().setMessage("OVSL-message2"))).addValidations(
          messageBuilder.setCode("GCS_VALIDATE_PERMISSIONS").setDescription("GVP-description").clearMessage()
            .addMessage(ValidationMessage.newBuilder().setMessage("GVP-message1"))
            .addMessage(ValidationMessage.newBuilder().setMessage("GVP-message2"))).addValidations(
          messageBuilder.setCode("UNKNOWN").setDescription("UNKNOWN-description").clearMessage()
            .addMessage(ValidationMessage.newBuilder().setMessage("UNKNOWN-message1"))
            .addMessage(ValidationMessage.newBuilder().setMessage("UNKNOWN-message2")))).build();
      }

      @Override
      public OperationMetadata get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        return null;
      }
    });

    List<Problem> connectivity = assessment.getConnectivity();
    List<Problem> features = assessment.getFeatures();
    assertEquals(2, connectivity.size());
    assertEquals(4, features.size());
    Problem problem1 = connectivity.get(0);
    assertEquals("Oracle Connectivity Failure", problem1.getName());
    assertEquals("Issue : OVTC-message1\nOVTC-message2 found when OVTC-description", problem1.getDescription());
    assertEquals("Check your Forward SSH tunnel configurations.", problem1.getSuggestion());
    assertEquals("Cannot read any snapshot or CDC changes from source database.", problem1.getImpact());

    Problem problem2 = connectivity.get(1);
    assertEquals("Oracle Connectivity Failure", problem2.getName());
    assertEquals("Issue : OVC-message1\nOVC-message2 found when OVC-description", problem2.getDescription());
    assertEquals("Check your Oracle database settings.", problem2.getSuggestion());
    assertEquals("Cannot replicate any snapshot or CDC changes from source database.", problem2.getImpact());

    Problem problem3 = features.get(0);
    assertEquals("Incorrect Oracle Settings", problem3.getName());
    assertEquals("Issue : OVLM-message1\nOVLM-message2 found when OVLM-description", problem3.getDescription());
    assertEquals("Check your Oracle database settings.", problem3.getSuggestion());
    assertEquals("Cannot replicate CDC changes from source database.", problem3.getImpact());

    Problem problem4 = features.get(1);
    assertEquals("Incorrect Oracle Settings", problem4.getName());
    assertEquals("Issue : OVSL-message1\nOVSL-message2 found when OVSL-description", problem4.getDescription());
    assertEquals("Check your Oracle database settings.", problem4.getSuggestion());
    assertEquals("Cannot replicate CDC changes of certain tables from source database.", problem4.getImpact());

    Problem problem5 = features.get(2);
    assertEquals("GCS Permission Issue", problem5.getName());
    assertEquals("Issue : GVP-message1\nGVP-message2 found when GVP-description", problem5.getDescription());
    assertEquals("Check your GCS permissions.", problem5.getSuggestion());
    assertEquals("Cannot replicate any snapshot or CDC changes from source database.", problem5.getImpact());

    Problem problem6 = features.get(3);
    assertEquals("General Issue", problem6.getName());
    assertEquals("Issue : UNKNOWN-message1\nUNKNOWN-message2 found when UNKNOWN-description",
      problem6.getDescription());
    assertEquals("N/A", problem6.getSuggestion());
    assertEquals("Unknown", problem6.getImpact());

    assertThrows(RuntimeException.class,
      () -> DatastreamTableAssessor.buildAssessment(new ApiFuture<OperationMetadata>() {
        @Override
        public void addListener(Runnable runnable, Executor executor) {

        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
          return false;
        }

        @Override
        public boolean isCancelled() {
          return false;
        }

        @Override
        public boolean isDone() {
          return false;
        }

        @Override
        public OperationMetadata get() throws InterruptedException, ExecutionException {
          throw new RuntimeException();
        }

        @Override
        public OperationMetadata get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
          return null;
        }
      }));

  }

  @Test
  public void testEvaluateColumn() {
    ColumnDetail column = new ColumnDetail("CHAR", OracleDataType.CHAR, false);
    ColumnEvaluation evaluation = DatastreamTableAssessor.evaluateColumn(column);
    Schema.Field field = evaluation.getField();
    assertEquals("CHAR", field.getName());
    assertEquals(Schema.of(Schema.Type.STRING), field.getSchema());
    ColumnAssessment assessment = evaluation.getAssessment();
    assertEquals("CHAR", assessment.getName());
    assertEquals(OracleDataType.CHAR.getName(), assessment.getType());
    assertNull(assessment.getSourceName());
    assertEquals(YES, assessment.getSupport());
    assertNull(assessment.getSuggestion());

    column = new ColumnDetail("DOUBLE_PRECISION", OracleDataType.DOUBLE_PRECISION, false);
    evaluation = DatastreamTableAssessor.evaluateColumn(column);
    field = evaluation.getField();
    assertEquals("DOUBLE_PRECISION", field.getName());
    assertEquals(Schema.of(Schema.Type.DOUBLE), field.getSchema());
    assessment = evaluation.getAssessment();
    assertEquals("DOUBLE_PRECISION", assessment.getName());
    assertEquals(OracleDataType.DOUBLE_PRECISION.getName(), assessment.getType());
    assertNull(assessment.getSourceName());
    assertEquals(YES, assessment.getSupport());
    assertNull(assessment.getSuggestion());

    column = new ColumnDetail("REAL", OracleDataType.REAL, false);
    evaluation = DatastreamTableAssessor.evaluateColumn(column);
    field = evaluation.getField();
    assertEquals("REAL", field.getName());
    assertEquals(Schema.of(Schema.Type.FLOAT), field.getSchema());
    assessment = evaluation.getAssessment();
    assertEquals("REAL", assessment.getName());
    assertEquals(OracleDataType.REAL.getName(), assessment.getType());
    assertNull(assessment.getSourceName());
    assertEquals(YES, assessment.getSupport());
    assertNull(assessment.getSuggestion());

    HashMap<String, String> properties = new HashMap<>();
    properties.put("PRECISION", "23");
    column = new ColumnDetail("FLOAT", OracleDataType.FLOAT, false, properties);
    evaluation = DatastreamTableAssessor.evaluateColumn(column);
    field = evaluation.getField();
    assertEquals("FLOAT", field.getName());
    assertEquals(Schema.of(Schema.Type.FLOAT), field.getSchema());
    assessment = evaluation.getAssessment();
    assertEquals("FLOAT", assessment.getName());
    assertEquals(OracleDataType.FLOAT.getName(), assessment.getType());
    assertNull(assessment.getSourceName());
    assertEquals(YES, assessment.getSupport());
    assertNull(assessment.getSuggestion());

    properties = new HashMap<>();
    properties.put("PRECISION", "24");
    column = new ColumnDetail("BINARY_FLOAT", OracleDataType.BINARY_FLOAT, false, properties);
    evaluation = DatastreamTableAssessor.evaluateColumn(column);
    field = evaluation.getField();
    assertEquals("BINARY_FLOAT", field.getName());
    assertEquals(Schema.of(Schema.Type.DOUBLE), field.getSchema());
    assessment = evaluation.getAssessment();
    assertEquals("BINARY_FLOAT", assessment.getName());
    assertEquals(OracleDataType.BINARY_FLOAT.getName(), assessment.getType());
    assertNull(assessment.getSourceName());
    assertEquals(YES, assessment.getSupport());
    assertNull(assessment.getSuggestion());

    column = new ColumnDetail("RAW", OracleDataType.RAW, false);
    evaluation = DatastreamTableAssessor.evaluateColumn(column);
    field = evaluation.getField();
    assertEquals("RAW", field.getName());
    assertEquals(Schema.of(Schema.Type.BYTES), field.getSchema());
    assessment = evaluation.getAssessment();
    assertEquals("RAW", assessment.getName());
    assertEquals(OracleDataType.RAW.getName(), assessment.getType());
    assertNull(assessment.getSourceName());
    assertEquals(YES, assessment.getSupport());
    assertNull(assessment.getSuggestion());

    column = new ColumnDetail("DATE", OracleDataType.DATE, false);
    evaluation = DatastreamTableAssessor.evaluateColumn(column);
    field = evaluation.getField();
    assertEquals("DATE", field.getName());
    assertEquals(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS), field.getSchema());
    assessment = evaluation.getAssessment();
    assertEquals("DATE", assessment.getName());
    assertEquals(OracleDataType.DATE.getName(), assessment.getType());
    assertNull(assessment.getSourceName());
    assertEquals(YES, assessment.getSupport());
    assertNull(assessment.getSuggestion());

    properties = new HashMap<>();
    properties.put("PRECISION", "5");
    properties.put("SCALE", "3");
    column = new ColumnDetail("DECIMAL", OracleDataType.DECIMAL, false, properties);
    evaluation = DatastreamTableAssessor.evaluateColumn(column);
    field = evaluation.getField();
    assertEquals("DECIMAL", field.getName());
    assertEquals(Schema.decimalOf(5, 3), field.getSchema());
    assessment = evaluation.getAssessment();
    assertEquals("DECIMAL", assessment.getName());
    assertEquals(OracleDataType.DECIMAL.getName(), assessment.getType());
    assertNull(assessment.getSourceName());
    assertEquals(YES, assessment.getSupport());
    assertNull(assessment.getSuggestion());

    column = new ColumnDetail("INTEGER", OracleDataType.INTEGER, false);
    evaluation = DatastreamTableAssessor.evaluateColumn(column);
    field = evaluation.getField();
    assertEquals("INTEGER", field.getName());
    assertEquals(Schema.of(Schema.Type.INT), field.getSchema());
    assessment = evaluation.getAssessment();
    assertEquals("INTEGER", assessment.getName());
    assertEquals(OracleDataType.INTEGER.getName(), assessment.getType());
    assertNull(assessment.getSourceName());
    assertEquals(YES, assessment.getSupport());
    assertNull(assessment.getSuggestion());


    properties = new HashMap<>();
    properties.put("PRECISION", "");
    column = new ColumnDetail("NUMBER_STRING", OracleDataType.NUMBER, false, properties);
    evaluation = DatastreamTableAssessor.evaluateColumn(column);
    field = evaluation.getField();
    assertEquals("NUMBER_STRING", field.getName());
    assertEquals(Schema.of(Schema.Type.STRING), field.getSchema());
    assessment = evaluation.getAssessment();
    assertEquals("NUMBER_STRING", assessment.getName());
    assertEquals(OracleDataType.NUMBER.getName(), assessment.getType());
    assertNull(assessment.getSourceName());
    assertEquals(YES, assessment.getSupport());
    assertNull(assessment.getSuggestion());

    properties = new HashMap<>();
    properties.put("PRECISION", "7");
    column = new ColumnDetail("NUMBER_LONG", OracleDataType.NUMBER, false, properties);
    evaluation = DatastreamTableAssessor.evaluateColumn(column);
    field = evaluation.getField();
    assertEquals("NUMBER_LONG", field.getName());
    assertEquals(Schema.of(Schema.Type.LONG), field.getSchema());
    assessment = evaluation.getAssessment();
    assertEquals("NUMBER_LONG", assessment.getName());
    assertEquals(OracleDataType.NUMBER.getName(), assessment.getType());
    assertNull(assessment.getSourceName());
    assertEquals(YES, assessment.getSupport());
    assertNull(assessment.getSuggestion());

    properties = new HashMap<>();
    properties.put("PRECISION", "5");
    properties.put("SCALE", "3");
    column = new ColumnDetail("NUMBER_DECIMAL", OracleDataType.NUMBER, false, properties);
    evaluation = DatastreamTableAssessor.evaluateColumn(column);
    field = evaluation.getField();
    assertEquals("NUMBER_DECIMAL", field.getName());
    assertEquals(Schema.decimalOf(5, 3), field.getSchema());
    assessment = evaluation.getAssessment();
    assertEquals("NUMBER_DECIMAL", assessment.getName());
    assertEquals(OracleDataType.NUMBER.getName(), assessment.getType());
    assertNull(assessment.getSourceName());
    assertEquals(YES, assessment.getSupport());
    assertNull(assessment.getSuggestion());


    column = new ColumnDetail("TIMESTAMP_WITH_TIME_ZONE", OracleDataType.TIMESTAMP_WITH_TIME_ZONE, false);
    evaluation = DatastreamTableAssessor.evaluateColumn(column);
    field = evaluation.getField();
    assertEquals("TIMESTAMP_WITH_TIME_ZONE", field.getName());
    assertEquals(Schema.recordOf("timestampTz", Schema.Field.of(
      "timestampTz", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("offset", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))), field.getSchema());
    assessment = evaluation.getAssessment();
    assertEquals("TIMESTAMP_WITH_TIME_ZONE", assessment.getName());
    assertEquals(OracleDataType.TIMESTAMP_WITH_TIME_ZONE.getName(), assessment.getType());
    assertNull(assessment.getSourceName());
    assertEquals(YES, assessment.getSupport());
    assertNull(assessment.getSuggestion());

    column = new ColumnDetail("OTHER", OracleDataType.OTHER, false);
    evaluation = DatastreamTableAssessor.evaluateColumn(column);
    assertNull(evaluation.getField());
    assessment = evaluation.getAssessment();
    assertEquals("OTHER", assessment.getName());
    assertEquals(OracleDataType.OTHER.getName(), assessment.getType());
    assertNull(assessment.getSourceName());
    assertEquals(NO, assessment.getSupport());
    assertEquals(new ColumnSuggestion("Unsupported Oracle Data Type: " + OracleDataType.OTHER,
      Collections.emptyList()), assessment.getSuggestion());

  }

  private void testAssessTable(boolean usingExisting) throws Exception {

    DatastreamDeltaSource deltaSource = createDeltaSource(usingExisting);
    List<SourceTable> tables = new ArrayList<>(getSourceTables());
    try (TableAssessor<TableDetail> assessor = deltaSource.createTableAssessor(null, tables);
      TableRegistry registry = deltaSource.createTableRegistry(null)) {
      for (SourceTable srcTable : tables) {
        TableDetail tableDetail =
          registry.describeTable(srcTable.getDatabase(), srcTable.getSchema(), srcTable.getTable());
        TableAssessment assessment = assessor.assess(tableDetail);
        assertEquals(tableDetail.getFeatures(), assessment.getFeatureProblems());
        List<ColumnAssessment> columnAssessments = assessment.getColumns();
        List<ColumnDetail> columnDetails = tableDetail.getColumns();
        assertEquals(columnDetails.size(), columnAssessments.size());
        for (int i = 0; i < columnAssessments.size(); i++) {
          ColumnAssessment columnAssessment = columnAssessments.get(i);
          ColumnDetail columnDetail = columnDetails.get(i);
          assertEquals(columnDetail.getName(), columnAssessment.getName());
          assertNull(columnAssessment.getSourceName());
          assertNull(columnAssessment.getSuggestion());
          assertEquals(YES, columnAssessment.getSupport());
          assertEquals(columnDetail.getType().getName(), columnAssessment.getType());
        }
      }
    }
  }

  private void testAssess(boolean usingExisting) throws Exception {

    DatastreamDeltaSource deltaSource = createDeltaSource(usingExisting);
    List<SourceTable> tables = new ArrayList<>(getSourceTables());
    try (TableAssessor<TableDetail> assessor = deltaSource.createTableAssessor(null, tables)) {
      Assessment assessment = assessor.assess();
      assertTrue(assessment.getConnectivity().isEmpty());
      assertTrue(assessment.getFeatures().isEmpty());
    }
  }


}
