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


import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.LogicalType;
import io.cdap.cdap.api.data.schema.Schema.Type;
import io.cdap.delta.api.assessment.Assessment;
import io.cdap.delta.api.assessment.ColumnAssessment;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.ColumnSuggestion;
import io.cdap.delta.api.assessment.ColumnSupport;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Datastream table assessor.
 */
public class DatastreamTableAssessor implements TableAssessor<TableDetail> {

  static final String PRECISION = "PRECISION";
  static final String SCALE = "SCALE";
  private final DatastreamConfig conf;

  DatastreamTableAssessor(DatastreamConfig conf) {
    this.conf = conf;
  }

  static ColumnEvaluation evaluateColumn(ColumnDetail detail) throws IllegalArgumentException {
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
        if (Integer.parseInt(precision) <= 23) {
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
        schema = Schema.decimalOf(Integer.parseInt(precision), Integer.parseInt(scale));
        break;
      case INTEGER:
      case SMALLINT:
        schema = Schema.of(Type.INT);
        break;
      case NUMBER:
        if (precision == null || precision.isEmpty() || scale == null || scale.isEmpty()) {
          schema = Schema.of(Type.STRING);
        } else {
          if (Integer.parseInt(scale) == 0) {
            schema = Schema.of(Type.LONG);
          } else {
            schema = Schema.decimalOf(Integer.parseInt(precision), Integer.parseInt(scale));
          }
        }
        break;
      case TIMESTAMP_WITH_TIME_ZONE:
        schema = Schema.recordOf("timestampTz", Schema.Field.of("timestampTz",
          Schema.of(LogicalType.TIMESTAMP_MICROS)), Schema.Field.of("offset",
          Schema.of(LogicalType.TIMESTAMP_MILLIS)));
        break;
      default:
        support = ColumnSupport.NO;
        suggestion = new ColumnSuggestion("Unsupported Oracle Data Type: " + detail.getType(),
          Collections.emptyList());
        schema = null;
    }

    Schema.Field field = schema == null ? null :
      Schema.Field.of(detail.getName(), detail.isNullable() ? Schema.nullableOf(schema) : schema);
    ColumnAssessment assessment = ColumnAssessment.builder(detail.getName(), detail.getType().getName())
      .setSupport(support)
      .setSuggestion(suggestion)
      .build();
    return new ColumnEvaluation(field, assessment);
  }

  @Override
  public Assessment assess() {
    return new Assessment(Collections.emptyList(), Collections.emptyList());
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public TableAssessment assess(TableDetail tableDetail) {
    return new TableAssessment(Collections.emptyList(), Collections.emptyList());
  }
}
