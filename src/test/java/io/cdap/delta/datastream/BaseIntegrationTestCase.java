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
import com.google.api.services.datastream.v1alpha1.model.Operation;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.BeforeAll;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

public class BaseIntegrationTestCase {
  protected static String serviceLocation;
  protected static String oracleHost;
  protected static String oracleUser;
  protected static String oraclePassword;
  protected static String oracleDb;
  protected static Set<String> oracleTables;
  protected static int oraclePort;
  protected static GoogleCredentials credentials;
  protected static DataStream datastream;
  protected static String parentPath;
  protected static String gcsBucket;
  protected static Storage storage;
  protected static String serviceAccountKey;
  protected static String streamId;
  protected static String project;

  @BeforeAll
  public static void setupTestClass() throws Exception {
    // Certain properties need to be configured otherwise the whole tests will be skipped.
    // Check README for how to configure the properties below.

    String messageTemplate = "%s is not configured, please refer to README for details.";

    project = System.getProperty("project.id");
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

    streamId = System.getProperty("stream.id");
    if (streamId == null || streamId.isEmpty()) {
      oracleHost = System.getProperty("oracle.host");
      assumeFalse(oracleHost == null, String.format(messageTemplate, "oracle host"));

      oracleUser = System.getProperty("oracle.user");
      assumeFalse(oracleUser == null, String.format(messageTemplate, "oracle user"));

      oraclePassword = System.getProperty("oracle.password", String.format(messageTemplate, "oracle password"));
      assumeFalse(oraclePassword == null);
    }

    oracleDb = System.getProperty("oracle.database");
    assumeFalse(oracleDb == null, String.format(messageTemplate, "oracle sid"));

    String tables = System.getProperty("oracle.tables");
    oracleTables = tables == null ? Collections.emptySet() : new HashSet<>(Arrays.asList(tables.split(",")));

    File serviceAccountFile = new File(serviceAccountFilePath);
    try (InputStream is = new FileInputStream(serviceAccountFile)) {
      credentials = GoogleCredentials.fromStream(is).createScoped("https://www.googleapis.com/auth/cloud-platform");
    }
    datastream = createDatastreamClient();
    storage = StorageOptions.newBuilder().setCredentials(credentials).setProjectId(project)
      .build().getService();

    serviceAccountKey = new String(Files.readAllBytes(Paths.get(new File(serviceAccountFilePath).getAbsolutePath())),
      StandardCharsets.UTF_8);

    gcsBucket = System.getProperty("gcs.bucket");
    parentPath = String.format("projects/%s/locations/%s", project, serviceLocation);

  }

  private static DataStream createDatastreamClient() {
    return new DataStream(new NetHttpTransport(), new JacksonFactory(), new HttpCredentialsAdapter(credentials));
  }

  protected DatastreamConfig buildDatastreamConfig(boolean usingExisting) {
    return new DatastreamConfig(usingExisting, oracleHost, oraclePort, oracleUser, oraclePassword, oracleDb,
      serviceLocation, DatastreamConfig.CONNECTIVITY_METHOD_IP_ALLOWLISTING, null, null, null, null, null, null,
      gcsBucket, null, serviceAccountKey, serviceAccountKey, streamId, project);
  }

  protected Operation waitUntilComplete(Operation operation) throws InterruptedException, IOException {
    while (!operation.getDone()) {
      TimeUnit.MILLISECONDS.sleep(200L);
      operation = datastream.projects().locations().operations().get(operation.getName()).execute();
    }
    return operation;
  }
}

