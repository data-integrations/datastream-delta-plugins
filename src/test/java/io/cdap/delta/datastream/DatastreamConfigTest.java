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

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


class DatastreamConfigTest {

  @Test
  void testDefaultValues() {
    DatastreamConfig config =
      new DatastreamConfig(false, "hostname", null, "user", "password", null, null, null, null, null, null,
                           null, null, null, null, null, null, null, null, null, null, null);

    assertFalse(config.isUsingExistingStream());
    assertEquals("hostname", config.getHost());
    assertEquals(DatastreamConfig.DEFAULT_PORT, config.getPort());
    assertEquals("user", config.getUser());
    assertEquals("password", config.getPassword());
    assertEquals(DatastreamConfig.DEFAULT_SID, config.getSid());
    assertEquals(DatastreamConfig.DEFAULT_REGION, config.getRegion());
    assertEquals(DatastreamConfig.CONNECTIVITY_METHOD_IP_ALLOWLISTING,
                 config.getConnectivityMethod());
    assertNull(config.getSshAuthenticationMethod());
    assertNull(config.getStreamId());
    assertNull(config.getGcsBucket());
    assertNull(config.getGcsBucketLocation());
    assertEquals("/", config.getGcsPathPrefix());
    assertNotNull(config.getDatastreamCredentials());
    assertNotNull(config.getGcsCredentials());
    assertNotNull(config.getProject());
    assertTrue(config.shouldReplicateExistingData());

    // forward ssh tunnel as connectivity method
    config = new DatastreamConfig(false, "hostname", null, "user", "password", null, null,
                                  DatastreamConfig.CONNECTIVITY_METHOD_FORWARD_SSH_TUNNEL,
                                  "sshHost", null, "sshUser", null, null, "sshPrivateKey", "us", "bucket",
                                  "prefix", "gcs-key", "datastream-key", null, "project", null);
    assertEquals("sshHost", config.getSshHost());
    assertEquals(DatastreamConfig.DEFAULT_SSH_PORT, config.getSshPort());
    assertEquals("sshUser", config.getSshUser());
    assertEquals(DatastreamConfig.AUTHENTICATION_METHOD_PRIVATE_PUBLIC_KEY,
                 config.getSshAuthenticationMethod());
    assertEquals("sshPrivateKey", config.getSshPrivateKey());
    assertNull(config.getSshPassword());
    assertNull(config.getPrivateConnectionName());
    assertEquals("bucket", config.getGcsBucket());
    assertEquals("us", config.getGcsBucketLocation());
    assertEquals("/prefix", config.getGcsPathPrefix());
    DatastreamConfig conf = config;
    assertThrows(RuntimeException.class, () -> conf.getDatastreamCredentials());
    assertThrows(RuntimeException.class, () -> conf.getGcsCredentials());
    assertEquals("project", config.getProject());
  }

  @Test
  void testValidate() throws NoSuchFieldException, IllegalAccessException {

    // sshHost, sshUser , sshPassword and sshPrivateKey can be null if IP allowlisting is selected
    // as connectivity method.
    new DatastreamConfig(false, "hostname", null, "user", "password", null, null,
                         DatastreamConfig.CONNECTIVITY_METHOD_IP_ALLOWLISTING, null, null, null,
                         null, null, null, null, null, null, null, null, null, null, null).validate();

    // sshPassowrd can be null if forward ssh tunnel is selected as connectivity method
    // and private/public key pair is selected as authentication method
    DatastreamConfig config = new DatastreamConfig(false, "hostname", null, "user", "password", null, null,
                                                   DatastreamConfig.CONNECTIVITY_METHOD_FORWARD_SSH_TUNNEL,
                                                   "sshHost", null, "sshUser",
                                                   DatastreamConfig.AUTHENTICATION_METHOD_PRIVATE_PUBLIC_KEY,
                                                   null, "sshPrivateKey", null, null, null, null,
                                                   null, null, null, null);
    config.validate();
    // host should not be null
    Field field = DatastreamConfig.class.getDeclaredField("host");
    field.setAccessible(true);
    field.set(config, null);
    assertThrows(IllegalArgumentException.class, () -> config.validate());
    field.set(config, "hostname");

    // username should not be null
    field = DatastreamConfig.class.getDeclaredField("user");
    field.setAccessible(true);
    field.set(config, null);
    assertThrows(IllegalArgumentException.class, () -> config.validate());
    field.set(config, "user");

    // password should not be null
    field = DatastreamConfig.class.getDeclaredField("password");
    field.setAccessible(true);
    field.set(config, null);
    assertThrows(IllegalArgumentException.class, () -> config.validate());
    field.set(config, "password");

    // sshHost should not be null if forward ssh tunnel is selected as connectivity method
    field = DatastreamConfig.class.getDeclaredField("sshHost");
    field.setAccessible(true);
    field.set(config, null);
    assertThrows(IllegalArgumentException.class, () -> config.validate());
    field.set(config, "sshHost");

    // sshUser should not be null if forward ssh tunnel is selected as connectivity method
    field = DatastreamConfig.class.getDeclaredField("sshUser");
    field.setAccessible(true);
    field.set(config, null);
    assertThrows(IllegalArgumentException.class, () -> config.validate());
    field.set(config, "sshUser");

    // sshPrivateKey should not be null if forward ssh tunnel is selected as connectivity method
    // and private/public key pair is selected as authentication method
    field = DatastreamConfig.class.getDeclaredField("sshPrivateKey");
    field.setAccessible(true);
    field.set(config, null);
    assertThrows(IllegalArgumentException.class, () -> config.validate());


    // sshPassword should not be null if forward ssh tunnel is selected as connectivity method
    // and password is selected as authentication method
    field = DatastreamConfig.class.getDeclaredField("sshAuthenticationMethod");
    field.setAccessible(true);
    field.set(config, DatastreamConfig.AUTHENTICATION_METHOD_PASSWORD);
    assertThrows(IllegalArgumentException.class, () -> config.validate());

    // sshPrivateKey can be null if forward ssh tunnel is selected as connectivity method
    // and password is selected as authentication method
    field = DatastreamConfig.class.getDeclaredField("sshPassword");
    field.setAccessible(true);
    // set value for sshPassword while sshPrivateKey is still null
    field.set(config, "sshPassword");
    config.validate();

    //privateConnectionName should not be null if private connectivity is selected as connectivity method
    field = DatastreamConfig.class.getDeclaredField("connectivityMethod");
    field.setAccessible(true);
    field.set(config, DatastreamConfig.CONNECTIVITY_METHOD_PRIVATE_CONNECTIVITY);
    assertThrows(IllegalArgumentException.class, () -> config.validate());

    field = DatastreamConfig.class.getDeclaredField("privateConnectionName");
    field.setAccessible(true);
    field.set(config, "vpc-peering-name");
    config.validate();

    field = DatastreamConfig.class.getDeclaredField("usingExistingStream");
    field.setAccessible(true);
    field.set(config, true);
    assertThrows(IllegalArgumentException.class, () -> config.validate());

  }
}
