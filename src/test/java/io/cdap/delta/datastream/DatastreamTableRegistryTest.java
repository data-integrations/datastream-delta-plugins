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

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.FailedPreconditionException;
import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.cloud.datastream.v1.DatastreamClient;
import com.google.cloud.datastream.v1.DiscoverConnectionProfileRequest;
import com.google.cloud.datastream.v1.DiscoverConnectionProfileResponse;
import com.google.cloud.datastream.v1.OracleRdbms;
import com.google.cloud.datastream.v1.OracleSchema;
import com.google.cloud.datastream.v1.OracleTable;
import io.cdap.delta.api.assessment.TableList;
import io.cdap.delta.api.assessment.TableNotFoundException;
import io.cdap.delta.api.assessment.TableSummary;
import io.cdap.delta.datastream.util.DatastreamDeltaSourceException;
import io.grpc.Status;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DatastreamTableRegistryTest {

  private static final String TABLE = "table";
  private static final String SCHEMA = "schema";
  private static final String DATABASE = "sid";
  private DatastreamConfig datastreamConfig;
  private DatastreamClient datastreamClient;
  private DatastreamTableRegistry datastreamTableRegistry;

  @BeforeEach
  public void setupTest() {
    datastreamConfig = Mockito.mock(DatastreamConfig.class);
    Mockito.when(datastreamConfig.isUsingExistingStream()).thenReturn(false);
    Mockito.when(datastreamConfig.getSid()).thenReturn(DATABASE);
    Mockito.when(datastreamConfig.getHost()).thenReturn("host");
    Mockito.when(datastreamConfig.getUser()).thenReturn("user");
    Mockito.when(datastreamConfig.getPassword()).thenReturn("password");
    Mockito.when(datastreamConfig.getConnectivityMethod()).thenReturn("ip-allowlisting");

    datastreamClient = Mockito.mock(DatastreamClient.class);
    datastreamTableRegistry = new DatastreamTableRegistry(datastreamConfig, datastreamClient);
  }

  @Test
  void testListTable() throws Exception {

    DiscoverConnectionProfileResponse response = Mockito.mock(DiscoverConnectionProfileResponse.class);

    OracleTable table = Mockito.mock(OracleTable.class);
    Mockito.when(table.getTable()).thenReturn(TABLE);
    Mockito.when(table.getOracleColumnsCount()).thenReturn(2);

    OracleSchema schema = Mockito.mock(OracleSchema.class);
    Mockito.when(schema.getSchema()).thenReturn(SCHEMA);
    Mockito.when(schema.getOracleTablesList()).thenReturn(Arrays.asList(table));

    OracleRdbms oracleRdbms = Mockito.mock(OracleRdbms.class);
    Mockito.when(response.getOracleRdbms()).thenReturn(oracleRdbms);
    Mockito.when(oracleRdbms.getOracleSchemasList()).thenReturn(Arrays.asList(schema));

    Mockito.when(datastreamClient.discoverConnectionProfile(Mockito.any())).thenReturn(response);

    TableList tableList = datastreamTableRegistry.listTables();

    assertNotNull(tableList);
    List<TableSummary> tables = tableList.getTables();
    assertNotNull(tables);
    assertEquals(1, tables.size());
    TableSummary table1 = tables.get(0);
    assertEquals(SCHEMA, table1.getSchema());
    assertEquals(TABLE, table1.getTable());
    
    //verify hierarchy depth set to 2
    ArgumentCaptor<DiscoverConnectionProfileRequest> captor = ArgumentCaptor.forClass(
      DiscoverConnectionProfileRequest.class);
    Mockito.verify(datastreamClient).discoverConnectionProfile(captor.capture());
    assertEquals(2, captor.getValue().getHierarchyDepth());
  }

  @Test
  void testDescribeTableFailsForPermanentErrors() {
    List<Throwable> exceptions = new ArrayList<>();
    exceptions.add(new FailedPreconditionException("error", new Exception("error"),
                                                   GrpcStatusCode.of(Status.Code.FAILED_PRECONDITION), false));
    exceptions.add(new AlreadyExistsException("error", new Exception("error"),
                                              GrpcStatusCode.of(Status.Code.ALREADY_EXISTS), false));
    exceptions.add(new InvalidArgumentException("error", new Exception("error"),
                                                GrpcStatusCode.of(Status.Code.INVALID_ARGUMENT), false));
    exceptions.add(new IllegalArgumentException("error", new Exception("error")));
    exceptions.add(new PermissionDeniedException("error", new Exception("error"),
                                                 GrpcStatusCode.of(Status.Code.PERMISSION_DENIED),  false));

    Map<Class<? extends Throwable>, Class<? extends Throwable>> mappedException = new HashMap<>();
    mappedException.put(FailedPreconditionException.class, TableNotFoundException.class);
    mappedException.put(InvalidArgumentException.class, TableNotFoundException.class);

    ArrayList<Throwable> nestedExceptions = new ArrayList<>();
    for (Throwable exception : exceptions) {
      nestedExceptions.add(new DatastreamDeltaSourceException("error", new ExecutionException("error", exception)));
    }
    exceptions.addAll(nestedExceptions);

    for (Throwable exception : exceptions) {
      Mockito.reset(datastreamClient);
      Mockito.when(datastreamClient.discoverConnectionProfile(Mockito.any()))
        .thenThrow(exception);

      Assertions.assertThrows(mappedException.getOrDefault(exception.getClass(), exception.getClass()),
                              () -> datastreamTableRegistry.describeTable(DATABASE, SCHEMA, TABLE));

      ArgumentCaptor<DiscoverConnectionProfileRequest> captor = ArgumentCaptor.forClass(
        DiscoverConnectionProfileRequest.class);
      //Verify no retries
      Mockito.verify(datastreamClient, Mockito.times(1))
        .discoverConnectionProfile(captor.capture());

      //Verify request details
      OracleRdbms oracleRdbmsRequest = captor.getValue().getOracleRdbms();
      Assertions.assertEquals(1, oracleRdbmsRequest.getOracleSchemasCount());
      OracleSchema oracleSchemasRequest = oracleRdbmsRequest.getOracleSchemas(0);
      Assertions.assertEquals(SCHEMA, oracleSchemasRequest.getSchema());
      Assertions.assertEquals(1, oracleSchemasRequest.getOracleTablesCount());
      Assertions.assertEquals(TABLE, oracleSchemasRequest.getOracleTables(0).getTable());
    }
  }
}
