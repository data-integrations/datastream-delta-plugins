/*
 * Copyright © 2023 Cask Data, Inc.
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

import com.google.cloud.datastream.v1.DatastreamClient;
import com.google.cloud.datastream.v1.DiscoverConnectionProfileRequest;
import com.google.cloud.datastream.v1.DiscoverConnectionProfileResponse;
import com.google.cloud.datastream.v1.OracleRdbms;
import com.google.cloud.datastream.v1.OracleSchema;
import com.google.cloud.datastream.v1.OracleTable;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableSummary;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DatastreamTableRegistryTest {
  @Test
  public void testListTable() throws Exception {
    DatastreamConfig datastreamConfig = Mockito.mock(DatastreamConfig.class);
    Mockito.when(datastreamConfig.isUsingExistingStream()).thenReturn(false);
    Mockito.when(datastreamConfig.getSid()).thenReturn("sid");
    Mockito.when(datastreamConfig.getHost()).thenReturn("host");
    Mockito.when(datastreamConfig.getUser()).thenReturn("user");
    Mockito.when(datastreamConfig.getPassword()).thenReturn("password");
    Mockito.when(datastreamConfig.getConnectivityMethod()).thenReturn("ip-allowlisting");

    DiscoverConnectionProfileResponse response = Mockito.mock(DiscoverConnectionProfileResponse.class);

    OracleTable table = Mockito.mock(OracleTable.class);
    Mockito.when(table.getTable()).thenReturn("table");
    Mockito.when(table.getOracleColumnsCount()).thenReturn(2);

    OracleSchema schema = Mockito.mock(OracleSchema.class);
    Mockito.when(schema.getSchema()).thenReturn("schema");
    Mockito.when(schema.getOracleTablesList()).thenReturn(Arrays.asList(table));

    OracleRdbms oracleRdbms = Mockito.mock(OracleRdbms.class);
    Mockito.when(response.getOracleRdbms()).thenReturn(oracleRdbms);
    Mockito.when(oracleRdbms.getOracleSchemasList()).thenReturn(Arrays.asList(schema));

    DatastreamClient datastreamClient = Mockito.mock(DatastreamClient.class);
    Mockito.when(datastreamClient.discoverConnectionProfile(Mockito.any())).thenReturn(response);

    DatastreamTableRegistry datastreamTableRegistry = new DatastreamTableRegistry(datastreamConfig, datastreamClient);
    TableList tableList = datastreamTableRegistry.listTables();

    assertNotNull(tableList);
    List<TableSummary> tables = tableList.getTables();
    assertNotNull(tables);
    assertFalse(tables.isEmpty());
    System.out.println("table counts:" + tables.size());

    //verify hierarchy depth set to 2
    ArgumentCaptor<DiscoverConnectionProfileRequest> captor = ArgumentCaptor.forClass(
      DiscoverConnectionProfileRequest.class);
    Mockito.verify(datastreamClient).discoverConnectionProfile(captor.capture());
    assertEquals(captor.getValue().getHierarchyDepth(), 2);
  }
}
