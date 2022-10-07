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

import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.ReplicationError;
import io.cdap.delta.api.SourceTable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.cdap.delta.datastream.DatastreamDeltaSource.BUCKET_CREATED_BY_CDF;

public class MockSourceContext implements DeltaSourceContext {
  private String runId;
  private String namespace;
  private String appName;
  private long generation;
  private Set<String> oracleTables;
  private String oracleDb;
  private Map<String, byte[]> mockState;

  public MockSourceContext(String namespace, String appName, long generation, String runId, Set<String> oracleTables,
    String oracleDb) {
    this.runId = runId;
    this.namespace = namespace;
    this.appName = appName;
    this.generation = generation;
    this.oracleTables = oracleTables;
    this.oracleDb = oracleDb;
    this.mockState = new HashMap<>();
  }

  public MockSourceContext() {
  }

  @Override
  public void setError(ReplicationError replicationError) throws IOException {

  }

  @Override
  public void setOK() throws IOException {

  }

  @Override
  public String getApplicationName() {
    return null;
  }

  @Override
  public String getRunId() {
    return runId;
  }

  @Override
  public Metrics getMetrics() {
    return null;
  }

  @Override
  public Map<String, String> getRuntimeArguments() {
    return null;
  }

  @Override
  public int getInstanceId() {
    return 0;
  }

  @Override
  public int getMaxRetrySeconds() {
    return 0;
  }

  @Override
  public byte[] getState(String s) throws IOException {
    if (s.equals(BUCKET_CREATED_BY_CDF)) {
      return mockState.get(s);
    }
    return new byte[0];
  }

  @Override
  public void putState(String s, byte[] bytes) throws IOException {
    if (s.equals(BUCKET_CREATED_BY_CDF)) {
      mockState.put(s, bytes);
    }
  }

  @Override
  public DeltaPipelineId getPipelineId() {
    return new DeltaPipelineId(namespace, appName, generation);
  }

  @Override
  public Set<SourceTable> getAllTables() {
    if (oracleTables == null) {
      return Collections.emptySet();
    }
    return oracleTables.stream().map(table -> new SourceTable(oracleDb, table.substring(table.indexOf(".") + 1),
      table.substring(0, table.indexOf(".")), Collections.emptySet(), Collections.emptySet(),
      Collections.emptySet())).collect(Collectors.toSet());
  }

  @Override
  public PluginProperties getPluginProperties(String s) {
    return null;
  }

  @Override
  public PluginProperties getPluginProperties(String s, MacroEvaluator macroEvaluator)
    throws InvalidMacroException {
    return null;
  }

  @Override
  public <T> Class<T> loadPluginClass(String s) {
    return null;
  }

  @Override
  public <T> T newPluginInstance(String s) throws InstantiationException {
    return null;
  }

  @Override
  public <T> T newPluginInstance(String s, MacroEvaluator macroEvaluator)
    throws InstantiationException, InvalidMacroException {
    return null;
  }

  @Override
  public void notifyFailed(Throwable throwable) {

  }
}
