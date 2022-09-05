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

import io.cdap.delta.api.SourceTable;
import io.cdap.delta.api.assessment.Assessment;
import io.cdap.delta.api.assessment.ColumnAssessment;
import io.cdap.delta.api.assessment.ColumnDetail;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.cdap.delta.api.assessment.ColumnSupport.YES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DatastreamAsessorTest extends BaseIntegrationTestCase {
  @Test
  public void testAssessNew() throws Exception {
    testAssess(false);
  }

  @Test
  public void testAssessExisting() throws Exception {
    testAssess(true);
  }

  @Test
  public void testAssessTableNew() throws Exception {
    testAssessTable(false);
  }

  @Test
  public void testAssessTableExisting() throws Exception {
    testAssessTable(true);
  }

  @Test
  public void testAssessWithInvalidCredentials() throws Exception {
    String invalidOracleUser = "dummy" + new Random().nextInt();

    DatastreamConfig datastreamConfig = new DatastreamConfig(false, oracleHost, oraclePort,
            invalidOracleUser, oraclePassword, oracleDb, serviceLocation,
            DatastreamConfig.CONNECTIVITY_METHOD_IP_ALLOWLISTING, null, null, null, null, null, null,
            null, gcsBucket, null, serviceAccountKey, serviceAccountKey, streamId, project, null);
    DatastreamDeltaSource deltaSource = createDeltaSource(datastreamConfig);
    List<SourceTable> tables = new ArrayList<>(getSourceTables());
    try (TableAssessor<TableDetail> assessor = deltaSource.createTableAssessor(null, tables)) {
      Assessment assessment = assessor.assess();
      assertEquals(1, assessment.getConnectivity().size());
      assertTrue(assessment.getFeatures().isEmpty());
    }
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
