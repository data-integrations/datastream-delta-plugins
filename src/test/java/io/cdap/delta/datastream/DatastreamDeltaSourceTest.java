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

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.datastream.v1alpha1.DataStream;
import com.google.api.services.datastream.v1alpha1.model.ConnectionProfile;
import com.google.api.services.datastream.v1alpha1.model.DestinationConfig;
import com.google.api.services.datastream.v1alpha1.model.GcsDestinationConfig;
import com.google.api.services.datastream.v1alpha1.model.GcsProfile;
import com.google.api.services.datastream.v1alpha1.model.Operation;
import com.google.api.services.datastream.v1alpha1.model.OracleProfile;
import com.google.api.services.datastream.v1alpha1.model.OracleRdbms;
import com.google.api.services.datastream.v1alpha1.model.OracleSourceConfig;
import com.google.api.services.datastream.v1alpha1.model.SourceConfig;
import com.google.api.services.datastream.v1alpha1.model.Stream;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.delta.api.DeltaPipelineId;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.ReplicationError;
import io.cdap.delta.api.SourceTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class DatastreamDeltaSourceTest {
  private static String serviceLocation;
  private static String oracleHost;
  private static String oracleUser;
  private static String oraclePassword;
  private static String oracleDb;
  private static Set<String> oracleTables;
  private static int oraclePort;
  private static GoogleCredentials credentials;
  private static DataStream datastream;
  private static String parentPath;
  private static String gcsBucket;
  private static Storage storage;
  private static String servcieAccountKey;

  @BeforeAll
  public static void setupTestClass() throws Exception {
    // Certain properties need to be configured otherwise the whole tests will be skipped.
    // Check README for how to configure the properties below.

    String messageTemplate = "%s is not configured, please refer to README for details.";

    String project = System.getProperty("project.id");
    if (project == null) {
      project = System.getProperty("GOOGLE_CLOUD_PROJECT");
    }
    if (project == null) {
      project = System.getProperty("GCLOUD_PROJECT");
    }
    assumeFalse(project == null, String.format(messageTemplate, "project id"));
    System.setProperty("GCLOUD_PROJECT", project);

    String serviceAccountFilePath = System.getProperty("service.account.file");
    assumeFalse(serviceAccountFilePath == null, String.format(messageTemplate, "service account key file"));

    serviceLocation = System.getProperty("service.location");
    if (serviceLocation == null) {
      serviceLocation = "us-central1";
    }

    String port = System.getProperty("oracle.port");
    if (port == null) {
      oraclePort = 1521;
    } else {
      oraclePort = Integer.parseInt(port);
    }

    oracleHost = System.getProperty("oracle.host");
    assumeFalse(oracleHost == null, String.format(messageTemplate, "oracle host"));

    oracleUser = System.getProperty("oracle.user");
    assumeFalse(oracleUser == null, String.format(messageTemplate, "oracle user"));

    oraclePassword = System.getProperty("oracle.password", String.format(messageTemplate, "oracle password"));
    assumeFalse(oraclePassword == null);

    oracleDb = System.getProperty("oracle.database");
    assumeFalse(oracleDb == null, String.format(messageTemplate, "oracle  sid"));

    String tables = System.getProperty("oracle.tables");
    assumeFalse(tables == null, String.format(messageTemplate, "oracle  tables"));
    oracleTables = new HashSet<>(Arrays.asList(tables.split(",")));

    File serviceAccountFile = new File(serviceAccountFilePath);
    try (InputStream is = new FileInputStream(serviceAccountFile)) {
      credentials = GoogleCredentials.fromStream(is).createScoped("https://www.googleapis.com/auth/cloud-platform");
    }
    datastream = createDatastreamClient();
    storage = StorageOptions.newBuilder().setCredentials(credentials).setProjectId(ServiceOptions.getDefaultProjectId())
      .build().getService();

    setEnv("GOOGLE_APPLICATION_CREDENTIALS", serviceAccountFilePath);
    servcieAccountKey = new String(Files.readAllBytes(Paths.get(new File(serviceAccountFilePath).getAbsolutePath())),
      StandardCharsets.UTF_8);

    gcsBucket = System.getProperty("gcs.bucket");
    parentPath = String.format("projects/%s/locations/%s", project, serviceLocation);

  }

  @Test
  public void testInitialize() throws Exception {
    String namspace = "default";
    String appName = "datastream-ut";
    String runId = "1234567890";
    long generation = 0;

    DatastreamConfig config = buildDatastreamConfig();
    DatastreamDeltaSource deltaSource = new DatastreamDeltaSource(config);
    DeltaSourceContext context = createContext(namspace, appName, generation, runId);
    if (gcsBucket == null) {
      gcsBucket = "df-rds-" + runId;
    }
    deltaSource.initialize(context);
    String replicatorId = String.format("%s-%s-%d", namspace, appName, generation);
    checkStream(replicatorId);
    // call twice should not have impact
    deltaSource.initialize(context);
    checkStream(replicatorId);
    clearStream(replicatorId);
    deltaSource.initialize(context);
    checkStream(replicatorId);
    clearStream(replicatorId);
  }

  private void clearStream(String replicatorId) throws InterruptedException, IOException {
    Operation operation = datastream.projects().locations().connectionProfiles().delete(String.format("%s" +
      "/connectionProfiles/DF-ORA-%s", parentPath, replicatorId)).execute();
    operation = waitUntilComplete(operation);
    assertNull(operation.getError());

    operation = datastream.projects().locations().connectionProfiles()
      .delete(String.format("%s/connectionProfiles/DF-GCS-%s", parentPath, replicatorId)).execute();
    operation = waitUntilComplete(operation);
    assertNull(operation.getError());

    operation = datastream.projects().locations().streams()
      .delete(String.format("%s/streams/DF-Stream-%s", parentPath, replicatorId)).execute();
    operation = waitUntilComplete(operation);
    assertNull(operation.getError());

    storage.delete(gcsBucket);
  }


  private Operation waitUntilComplete(Operation operation) throws InterruptedException, IOException {
    while (!operation.getDone()) {
      TimeUnit.MILLISECONDS.sleep(200L);
      operation = datastream.projects().locations().operations().get(operation.getName()).execute();
    }
    return operation;
  }
  private void checkStream(String replicatorId) throws IOException {
    // Check source connection profile
    String srcProfileName = String.format("DF-ORA-%s", replicatorId);
    String srcProfilePath = String.format("%s/connectionProfiles/%s", parentPath, srcProfileName);
    ConnectionProfile srcProfile =
      datastream.projects().locations().connectionProfiles().get(srcProfilePath).execute();
    assertEquals(srcProfileName, srcProfile.getDisplayName());
    assertEquals(srcProfilePath, srcProfile.getName());
    assertNotNull(srcProfile.getStaticServiceIpConnectivity());
    assertNull(srcProfile.getForwardSshConnectivity());
    assertNull(srcProfile.getNoConnectivity());
    assertNull(srcProfile.getPrivateConnectivity());
    OracleProfile oracleProfile = srcProfile.getOracleProfile();
    assertEquals(oracleHost, oracleProfile.getHostname());
    assertEquals(oraclePort, oracleProfile.getPort());
    assertEquals(oracleUser, oracleProfile.getUsername());
    assertEquals(oracleDb, oracleProfile.getDatabaseService());

    // Check destination connection profile
    String desProfileName = String.format("DF-GCS-%s", replicatorId);
    String desProfilePath = String.format("%s/connectionProfiles/%s", parentPath, desProfileName);
    ConnectionProfile desProfile =
      datastream.projects().locations().connectionProfiles().get(desProfilePath).execute();
    assertEquals(desProfileName, desProfile.getDisplayName());
    assertEquals(desProfilePath, desProfile.getName());
    GcsProfile gcsProfile = desProfile.getGcsProfile();
    assertEquals(gcsBucket, gcsProfile.getBucketName());
    assertEquals("/", gcsProfile.getRootPath());

    // Check stream
    String streamName = String.format("DF-Stream-%s", replicatorId);
    String streamPath = String.format("%s/streams/%s", parentPath, streamName);
    Stream stream = datastream.projects().locations().streams().get(streamPath).execute();
    assertEquals(streamName, stream.getDisplayName());
    assertEquals(streamPath, stream.getName());

    SourceConfig sourceConfig = stream.getSourceConfig();
    String sourceConnectionProfileName = sourceConfig.getSourceConnectionProfileName();
    assertEquals(srcProfilePath.substring(srcProfilePath.indexOf("/locations")),
      sourceConnectionProfileName.substring(sourceConnectionProfileName.indexOf("/locations")));
    OracleSourceConfig oracleSourceConfig = sourceConfig.getOracleSourceConfig();
    OracleRdbms allowlist = oracleSourceConfig.getAllowlist();
    allowlist.getOracleSchemas().forEach(schema -> schema.getOracleTables().forEach(table -> {
      assertTrue(oracleTables.contains(String.format("%s.%s", schema.getSchemaName(), table.getTableName())));
    }));

    DestinationConfig desConfig = stream.getDestinationConfig();
    String destinationConnectionProfileName = desConfig.getDestinationConnectionProfileName();
    assertEquals(desProfilePath.substring(desProfilePath.indexOf("/locations")),
      destinationConnectionProfileName.substring(destinationConnectionProfileName.indexOf("/locations")));
    GcsDestinationConfig gcsConfig = desConfig.getGcsDestinationConfig();
    assertNotNull(gcsConfig.getAvroFileFormat());
    assertNull(gcsConfig.getJsonFileFormat());
    assertEquals("15s", gcsConfig.getFileRotationInterval());
    assertEquals(1, gcsConfig.getFileRotationMb());
    assertEquals("/" + streamName, gcsConfig.getPath());
  }

  private DeltaSourceContext createContext(String namespace, String appName, long generation, String runId) {
    return new DeltaSourceContext() {
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
        return new byte[0];
      }

      @Override
      public void putState(String s, byte[] bytes) throws IOException {

      }

      @Override
      public DeltaPipelineId getPipelineId() {
        return new DeltaPipelineId(namespace, appName, generation);
      }

      @Override
      public Set<SourceTable> getAllTables() {
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
    };
  }

  private static DataStream createDatastreamClient() {
    return new DataStream(new NetHttpTransport(), new JacksonFactory(), new HttpCredentialsAdapter(credentials));
  }

  private DatastreamConfig buildDatastreamConfig() {
    return new DatastreamConfig(oracleHost, oraclePort, oracleUser, oraclePassword, oracleDb, serviceLocation,
      DatastreamConfig.CONNECTIVITY_METHOD_IP_ALLOWLISTING, null, null, null, null, null, null, gcsBucket, null, null);
  }

  private static void setEnv(String key, String value) throws Exception {
    Map<String, String> newEnv = new HashMap<>(System.getenv());
    newEnv.put(key, value);
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
      env.putAll(newEnv);
      Field theCaseInsensitiveEnvironmentField =
        processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
      cienv.putAll(newEnv);
    } catch (NoSuchFieldException e) {
      Class[] classes = Collections.class.getDeclaredClasses();
      Map<String, String> env = System.getenv();
      for (Class cl : classes) {
        if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
          Field field = cl.getDeclaredField("m");
          field.setAccessible(true);
          Object obj = field.get(env);
          Map<String, String> map = (Map<String, String>) obj;
          map.clear();
          map.putAll(newEnv);
        }
      }
    }
  }

}
