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

import com.google.cloud.datastream.v1.ConnectionProfile;
import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.GcsDestinationConfig;
import com.google.cloud.datastream.v1.GcsProfile;
import com.google.cloud.datastream.v1.OracleProfile;
import com.google.cloud.datastream.v1.OracleRdbms;
import com.google.cloud.datastream.v1.OracleSourceConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.datastream.util.MockSourceContext;
import io.cdap.delta.datastream.util.Utils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static io.cdap.delta.datastream.DatastreamDeltaSource.BUCKET_CREATED_BY_CDF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatastreamDeltaSourceTest extends BaseIntegrationTestCase {

  @Test
  public void testInitializeExistingStream() throws Exception {
    DatastreamConfig config = buildDatastreamConfig(true);
    DatastreamDeltaSource deltaSource = new DatastreamDeltaSource(config);
    DeltaSourceContext context = new MockSourceContext(null, null, 0L, null, oracleTables, oracleDb);
    deltaSource.initialize(context);
    String streamPath = String.format("%s/streams/%s", parentPath, streamId);
    Stream stream = datastream.getStream(streamPath);
    OracleRdbms allowlist = stream.getSourceConfig().getOracleSourceConfig().getIncludeObjects();
    assertTrue(allowlist.getOracleSchemasList().stream().flatMap(schema -> schema.getOracleTablesList().stream()
      .map(table -> String.format("%s.%s", schema.getSchema(), table.getTable()))).collect(Collectors.toSet())
      .containsAll(oracleTables));
  }

  @Test
  public void testGCSBucketCreation() throws Exception {
    // Check if lifecycle rules are set on bucket created by CDF
    String namespace = "default";
    String appName = "datastream-ut";
    String runId = System.currentTimeMillis() + "";
    long generation = System.currentTimeMillis();
    String bucketName = "test-bucket-" + UUID.randomUUID();

    DeltaSourceContext context = new MockSourceContext(namespace, appName, generation, runId, oracleTables, oracleDb);
    String replicatorId = Utils.buildReplicatorId(context);

    try {
      DatastreamConfig config = new DatastreamConfig(false, oracleHost, oraclePort, oracleUser, oraclePassword,
         oracleDb, serviceLocation, DatastreamConfig.CONNECTIVITY_METHOD_IP_ALLOWLISTING, null, null, null, null, null,
         null, null, bucketName, null, serviceAccountKey, serviceAccountKey, null, project, null);
      DatastreamDeltaSource deltaSource = new DatastreamDeltaSource(config);
      deltaSource.initialize(context);
      byte[] bucketCreated = context.getState(BUCKET_CREATED_BY_CDF);
      assertTrue(bucketCreated != null && Bytes.toBoolean(bucketCreated));
      Bucket bucket = storage.get(bucketName, Storage.BucketGetOption.fields(Storage.BucketField.values()));
      assertEquals(1, bucket.getLifecycleRules().size());
    } finally {
      datastream
        .deleteConnectionProfileAsync(String.format("%s" + "/connectionProfiles/DF-ORA-%s", parentPath, replicatorId))
        .get();
      datastream
        .deleteConnectionProfileAsync(String.format("%s/connectionProfiles/DF-GCS-%s", parentPath, replicatorId))
        .get();
      datastream.deleteStreamAsync(String.format("%s/streams/DF-Stream-%s", parentPath, replicatorId)).get();
      storage.delete(bucketName);
    }
  }

  @Test
  public void testInitializeNewStream() throws Exception {
    String namespace = "default";
    String appName = "datastream-ut";
    String runId = "1234567890";
    long generation = 0;
    DatastreamConfig config = buildDatastreamConfig(false);
    DatastreamDeltaSource deltaSource = new DatastreamDeltaSource(config);
    DeltaSourceContext context = new MockSourceContext(namespace, appName, generation, runId, oracleTables, oracleDb);
    if (gcsBucket == null) {
      gcsBucket = "df-rds-" + runId;
    }
    deltaSource.initialize(context);
    String replicatorId = String.format("%s-%s-%d", namespace, appName, generation);
    checkStream(replicatorId);
    // call twice should not have impact
    deltaSource.initialize(context);
    checkStream(replicatorId);
    clearStream(replicatorId);
    deltaSource.initialize(context);
    checkStream(replicatorId);
    clearStream(replicatorId);
  }

  @Test
  public void testGCSBucketCreation() throws Exception {
    // Check if lifecycle rules are set on bucket created by CDF
    DatastreamConfig config = buildDatastreamConfig(false);
    DatastreamDeltaSource deltaSource = new DatastreamDeltaSource(config);
    DeltaSourceContext context = new MockSourceContext(namespace, appName, generation, runId, oracleTables, oracleDb);
    String originalBucket = gcsBucket;

    gcsBucket = null;
    deltaSource.initialize(context);
    byte[] bucketCreated = context.getState(BUCKET_CREATED_BY_CDF);
    assertTrue(bucketCreated != null && Bytes.toBoolean(bucketCreated));
    gcsBucket = Utils.buildBucketName(context.getRunId());
    Bucket bucket = storage.get(gcsBucket, Storage.BucketGetOption.fields(Storage.BucketField.values()));
    assertEquals(1, bucket.getLifecycleRules().size());
    gcsBucket = originalBucket;
  }

  private void clearStream(String replicatorId) throws InterruptedException, IOException, ExecutionException {
    datastream
      .deleteConnectionProfileAsync(String.format("%s" + "/connectionProfiles/DF-ORA-%s", parentPath, replicatorId))
      .get();
    datastream.deleteConnectionProfileAsync(String.format("%s/connectionProfiles/DF-GCS-%s", parentPath, replicatorId))
      .get();
    datastream.deleteStreamAsync(String.format("%s/streams/DF-Stream-%s", parentPath, replicatorId)).get();
    storage.delete(gcsBucket);
  }
  
  private void checkStream(String replicatorId) throws IOException {
    // Check source connection profile
    String srcProfileName = String.format("DF-ORA-%s", replicatorId);
    String srcProfilePath = String.format("%s/connectionProfiles/%s", parentPath, srcProfileName);
    ConnectionProfile srcProfile = datastream.getConnectionProfile(srcProfilePath);
    assertEquals(srcProfileName, srcProfile.getDisplayName());
    assertEquals(srcProfilePath, srcProfile.getName());
    assertTrue(srcProfile.hasStaticServiceIpConnectivity());
    assertFalse(srcProfile.hasForwardSshConnectivity());
    assertFalse(srcProfile.hasPrivateConnectivity());
    OracleProfile oracleProfile = srcProfile.getOracleProfile();
    assertEquals(oracleHost, oracleProfile.getHostname());
    assertEquals(oraclePort, oracleProfile.getPort());
    assertEquals(oracleUser, oracleProfile.getUsername());
    assertEquals(oracleDb, oracleProfile.getDatabaseService());

    // Check destination connection profile
    String desProfileName = String.format("DF-GCS-%s", replicatorId);
    String desProfilePath = String.format("%s/connectionProfiles/%s", parentPath, desProfileName);
    ConnectionProfile desProfile = datastream.getConnectionProfile(desProfilePath);
    assertEquals(desProfileName, desProfile.getDisplayName());
    assertEquals(desProfilePath, desProfile.getName());
    GcsProfile gcsProfile = desProfile.getGcsProfile();
    assertEquals(gcsBucket, gcsProfile.getBucket());
    assertEquals("/", gcsProfile.getRootPath());

    // Check stream
    String streamName = String.format("DF-Stream-%s", replicatorId);
    String streamPath = String.format("%s/streams/%s", parentPath, streamName);
    Stream stream = datastream.getStream(streamPath);
    assertEquals(streamName, stream.getDisplayName());
    assertEquals(streamPath, stream.getName());

    SourceConfig sourceConfig = stream.getSourceConfig();
    String sourceConnectionProfileName = sourceConfig.getSourceConnectionProfile();
    assertEquals(srcProfilePath.substring(srcProfilePath.indexOf("/locations")),
      sourceConnectionProfileName.substring(sourceConnectionProfileName.indexOf("/locations")));
    OracleSourceConfig oracleSourceConfig = sourceConfig.getOracleSourceConfig();
    OracleRdbms allowlist = oracleSourceConfig.getIncludeObjects();
    allowlist.getOracleSchemasList().forEach(schema -> schema.getOracleTablesList().forEach(table -> {
      assertTrue(oracleTables.contains(String.format("%s.%s", schema.getSchema(), table.getTable())));
    }));

    DestinationConfig desConfig = stream.getDestinationConfig();
    String destinationConnectionProfileName = desConfig.getDestinationConnectionProfile();
    assertEquals(desProfilePath.substring(desProfilePath.indexOf("/locations")),
      destinationConnectionProfileName.substring(destinationConnectionProfileName.indexOf("/locations")));
    GcsDestinationConfig gcsConfig = desConfig.getGcsDestinationConfig();
    assertTrue(gcsConfig.hasAvroFileFormat());
    assertFalse(gcsConfig.hasJsonFileFormat());
    assertEquals(15, gcsConfig.getFileRotationInterval().getSeconds());
    assertEquals(1, gcsConfig.getFileRotationMb());
    assertEquals("/" + streamName, gcsConfig.getPath());
  }
}
