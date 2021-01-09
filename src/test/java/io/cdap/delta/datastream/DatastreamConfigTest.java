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
import static org.junit.jupiter.api.Assertions.assertThrows;


class DatastreamConfigTest {

  @Test
  void testDefaultValues() {
    DatastreamConfig config =
      new DatastreamConfig(false, "hostname", null, "user", "password", null, null, null, null, null, null,
                           null, null, null, null, null, null, null);

    assertEquals(DatastreamConfig.DEFAULT_PORT, config.getPort());
    assertEquals(DatastreamConfig.DEFAULT_SID, config.getSid());
    assertEquals(DatastreamConfig.DEFAULT_REGION, config.getRegion());
    assertEquals(DatastreamConfig.CONNECTIVITY_METHOD_IP_ALLOWLISTING,
                 config.getConnectivityMethod());

    // forward ssh tunnel as connectivity method
    config = new DatastreamConfig(false, "hostname", null, "user", "password", null, null,
                                  DatastreamConfig.CONNECTIVITY_METHOD_FORWARD_SSH_TUNNEL,
                                  "sshHost", null, "sshUser", null, null, "sshPrivateKey", null,
                                  null, null, null);
    assertEquals(DatastreamConfig.DEFAULT_SSH_PORT, config.getSshPort());
    assertEquals(DatastreamConfig.AUTHENTICATION_METHOD_PRIVATE_PUBLIC_KEY,
                 config.getSshAuthenticationMethod());

  }

  @Test
  void testValidate() throws NoSuchFieldException, IllegalAccessException {

    // sshHost, sshUser , sshPassword and sshPrivateKey can be null if IP allowlisting is selected
    // as connectivity method.
    new DatastreamConfig(false, "hostname", null, "user", "password", null, null,
                         DatastreamConfig.CONNECTIVITY_METHOD_IP_ALLOWLISTING, null, null, null,
                         null, null, null, null, null, null, null).validate();

    // sshPassowrd can be null if forward ssh tunnel is selected as connectivity method
    // and private/public key pair is selected as authentication method
    DatastreamConfig config = new DatastreamConfig(false, "hostname", null, "user", "password", null, null,
                                                   DatastreamConfig.CONNECTIVITY_METHOD_FORWARD_SSH_TUNNEL,
                                                   "sshHost", null, "sshUser",
                                                   DatastreamConfig.AUTHENTICATION_METHOD_PRIVATE_PUBLIC_KEY,
                                                   null, "sshPrivateKey", null, null, null, null);
    config.validate();

    // sshHost should not be null if forward ssh tunnel is selected as connectivity method
    Field field = DatastreamConfig.class.getDeclaredField("sshHost");
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


  }
}
