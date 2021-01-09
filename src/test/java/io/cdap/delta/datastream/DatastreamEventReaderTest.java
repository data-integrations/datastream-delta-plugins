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

package io.cdap.delta.datastream;

import com.google.gson.Gson;
import io.cdap.delta.api.DDLEvent;
import io.cdap.delta.api.DMLEvent;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.Offset;
import io.cdap.delta.api.SourceTable;
import io.cdap.delta.datastream.util.MockSourceContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DatastreamEventReaderTest extends BaseIntegrationTestCase {

  private static final Gson GSON = new Gson();

  @Test
  public void testStart() throws Exception {

    DatastreamConfig config = buildDatastreamConfig(true);
    DatastreamDeltaSource deltaSource = new DatastreamDeltaSource(config);
    DeltaSourceContext context =  new MockSourceContext();
    deltaSource.initialize(context);
    EventEmitter emitter = createEmitter();
    DatastreamEventReader reader = deltaSource.createReader(buildDefinition(), context, emitter);
    reader.start(createOffset());
    TimeUnit.MINUTES.sleep(30);
    reader.stop();
  }

  private EventReaderDefinition buildDefinition() {
    return new EventReaderDefinition(getSourceTables(), Collections.emptySet(), Collections.emptySet());
  }

  private Set<SourceTable> getSourceTables() {
    return oracleTables.stream().map(table -> new SourceTable(oracleDb, table.substring(table.indexOf(".") + 1),
      table.substring(0, table.indexOf(".")), Collections.emptySet(), Collections.emptySet(), Collections.emptySet()))
      .collect(Collectors.toSet());
  }

  private EventEmitter createEmitter() {
    return new EventEmitter() {

      private long dmleventsNum;
      private long ddleventsNum;

      @Override
      public void emit(DDLEvent ddlEvent) throws InterruptedException {
        System.out.println("DDLEvent:" + ddleventsNum++ + "-" + GSON.toJson(ddlEvent));
      }

      @Override
      public void emit(DMLEvent dmlEvent) throws InterruptedException {
        System.out.println("DDLEvent:" + dmleventsNum++ + "-" + GSON.toJson(dmlEvent));
      }
    };
  }

  private Offset createOffset() {
    return new Offset();
  }
}
