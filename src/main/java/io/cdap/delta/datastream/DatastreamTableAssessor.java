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

import io.cdap.delta.api.assessment.Assessment;
import io.cdap.delta.api.assessment.TableAssessment;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;

import java.io.IOException;
import java.util.Collections;

/**
 * Datastream table assessor.
 */
public class DatastreamTableAssessor implements TableAssessor<TableDetail> {

  private final DatastreamConfig conf;

  DatastreamTableAssessor(DatastreamConfig conf) {
    this.conf = conf;
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
