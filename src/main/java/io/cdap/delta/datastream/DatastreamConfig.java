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

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.plugin.PluginConfig;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import javax.annotation.Nullable;


/**
 * Plugin configuration for the Datastream origin.
 */
public class DatastreamConfig extends PluginConfig {

  public static final String DEFAULT_REGION = "us-central1";
  public static final String CONNECTIVITY_METHOD_IP_ALLOWLISTING = "ip-allowlisting";
  public static final String CONNECTIVITY_METHOD_FORWARD_SSH_TUNNEL = "forward-ssh-tunnel";
  public static final String AUTHENTICATION_METHOD_PRIVATE_PUBLIC_KEY = "private-public-key";
  public static final String AUTHENTICATION_METHOD_PASSWORD = "password";
  public static final String DEFAULT_SID = "ORCL";

  public static final int DEFAULT_PORT = 1521;
  public static final int DEFAULT_SSH_PORT = 22;


  @Description("Whether to use an existing Datastream stream.")
  private boolean usingExistingStream;

  @Nullable
  @Description("Hostname of the Oracle server to read from. This information is required when you choose to create a " +
    "new Datastream stream.")
  private String host;

  @Nullable
  @Description("Port to use to connect to the Oracle server. This information is required when you choose to create a" +
    " new Datastream stream. By default \"" + DEFAULT_PORT + "\" will be used.")
  private Integer port;

  @Nullable
  @Description("Username to use to connect to the Oracle server. This information is required when you choose to " +
    "create a new Datastream stream.")
  private String user;

  @Nullable
  @Macro
  @Description("Password to use to connect to the Oracle server.  This information is required when you choose to " +
    "create a new Datastream stream.")
  private String password;

  @Nullable
  @Description("Oracle system identifier of the database to replicate changes from.  This information is required " +
    "when you choose to create a new Datastream stream. By default \"" + DEFAULT_SID + "\" will be used.")
  private String sid;

  @Nullable
  @Description(
    "Region of the existing Datastream stream or a new stream to be created. By default \"" + DEFAULT_REGION +
      "\" will be used.")
  private String region;

  @Nullable
  @Description("The way DataStream will connect to Oracle. See \"Documentation\" tab for details.")
  private String connectivityMethod;

  @Nullable
  @Description("Hostname of the SSH Server to connect to.")
  // only required when connectivity method is  "Forward SSH Tunnel"
  private String sshHost;

  @Nullable
  @Description("Port of the SSH Server to connect to. By default \"" + DEFAULT_SSH_PORT + "\" will be used.")
  // Cannot make sshPort an int, because UI will take this property as required and thus cannot hide
  // this property when IP allowlisting is selected as connectivity method
  // only required when connectivity method is  "Forward SSH Tunnel"
  private Integer sshPort;

  @Nullable
  @Description("Username to login the SSH Server.")
  // only required when connectivity method is  "Forward SSH Tunnel"
  private String sshUser;

  @Nullable
  @Description("How the SSH server authenticates the login.")
  // only required when connectivity method is  "Forward SSH Tunnel"
  private String sshAuthenticationMethod;

  @Macro
  @Nullable
  @Description("Password of the login on the SSH Server.")
  // only required when connectivity method is  "Forward SSH Tunnel" and authentication method is
  // "Password"
  private String sshPassword;

  @Macro
  @Nullable
  @Description("Private key of the login on the SSH Server.")
  // only required when connectivity method is  "Forward SSH Tunnel" and authentication method is
  // "Private/Public Key Pair"
  private String sshPrivateKey;

  @Nullable
  @Description("The GCS bucket that DataStream can write its output to. By default replicator " +
    "application will create one for you. See \"Documentation\" tab for details")
  private String gcsBucket;

  @Nullable
  @Description("The optional path prefix of the path where DataStream will write its output to.")
  private String gcsPathPrefix;


  @Macro
  @Nullable
  @Description("The service account key of the service account that will be used to read " +
    "DataStream results from GCS Bucket. By default Dataproc service account will be used.")
  private String gcsServiceAccountKey;

  @Nullable
  @Description("The id of an existing Datastream stream that will be used to read CDC changes from.  This information" +
    " is required when you choose to use an existing Datastream stream.")
  private String streamId;

  @Macro
  @Nullable
  @Description("The service account key of the service account that will be used to create or query DataStream stream" +
    ". By default Cloud Data Fusion service account will be used when you create a replicator and Dataproc service " +
    "account will be used when replicator pipeline is running.")
  private String dsServiceAccountKey;

  @Nullable
  @Description("Project of the Datastream stream. When running on a Google Cloud Data Fusion, this can be set to "
    + "'auto-detect', which will use the project of the Google Cloud Data Fusion.")
  private String project;

  public boolean isUsingExistingStream() {
    return usingExistingStream;
  }

  @Nullable
  public String getHost() {
    return host;
  }

  public int getPort() {
    return port == null ? DEFAULT_PORT : port;
  }

  @Nullable
  public String getUser() {
    return user;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

  public String getSid() {
    return sid == null || sid.isEmpty() ? DEFAULT_SID : sid;
  }

  public String getRegion() {
    return region == null || region.isEmpty() ? DEFAULT_REGION : region;
  }

  @Nullable
  public String getStreamId() {
    return streamId;
  }

  public String getConnectivityMethod() {
    return connectivityMethod == null || connectivityMethod
      .isEmpty() ? CONNECTIVITY_METHOD_IP_ALLOWLISTING : connectivityMethod;
  }

  @Nullable
  public String getSshHost() {
    return sshHost;
  }

  public Integer getSshPort() {
    return sshPort == null ? DEFAULT_SSH_PORT : sshPort;
  }

  @Nullable
  public String getSshUser() {
    return sshUser;
  }

  @Nullable
  public String getSshAuthenticationMethod() {
    if (CONNECTIVITY_METHOD_IP_ALLOWLISTING.equals(connectivityMethod)) {
      return null;
    }
    //if connectivity method is forward ssh tunnel, return the default value if it's not set.
    return sshAuthenticationMethod == null || sshAuthenticationMethod.isEmpty() ?
      AUTHENTICATION_METHOD_PRIVATE_PUBLIC_KEY : sshAuthenticationMethod;
  }

  @Nullable
  public String getSshPassword() {
    return sshPassword;
  }

  @Nullable
  public String getSshPrivateKey() {
    return sshPrivateKey;
  }

  @Nullable
  public String getGcsBucket() {
    return gcsBucket == null ? null : gcsBucket.toLowerCase();
  }

  public String getGcsPathPrefix() {
    if (gcsPathPrefix == null) {
      return "/";
    }
    return gcsPathPrefix.startsWith("/") ? gcsPathPrefix : "/" + gcsPathPrefix;
  }

  public Credentials getGcsCredentials() {
    return getCredentials(gcsServiceAccountKey);
  }
  public Credentials getDatastreamCredentials() {
    return getCredentials(dsServiceAccountKey);
  }

  public String getProject() {
    if (project == null || project.trim().isEmpty() || "auto-detect".equalsIgnoreCase(project)) {
      return ServiceOptions.getDefaultProjectId();
    }
    return project;
  }

  private Credentials getCredentials(String serviceAccountKey) {
    if (serviceAccountKey == null || "auto-detect".equalsIgnoreCase(serviceAccountKey)) {
      try {
        return GoogleCredentials.getApplicationDefault();
      } catch (IOException e) {
        throw new RuntimeException("Fail to get application default credentials!", e);
      }
    }

    try (InputStream is = new ByteArrayInputStream(serviceAccountKey.getBytes(StandardCharsets.UTF_8))) {
      return GoogleCredentials.fromStream(is)
        .createScoped(Collections.singleton("https://www.googleapis.com/auth/cloud-platform"));
    } catch (IOException e) {
      throw new RuntimeException("Fail to read service account key!", e);
    }
  }

  public DatastreamConfig(boolean usingExistingStream, @Nullable String host, @Nullable Integer port,
    @Nullable String user, @Nullable String password, @Nullable String sid, @Nullable String region,
    @Nullable String connectivityMethod, @Nullable String sshHost, @Nullable Integer sshPort, @Nullable String sshUser,
    @Nullable String sshAuthenticationMethod, @Nullable String sshPassword, @Nullable String sshPrivateKey,
    @Nullable String gcsBucket, @Nullable String gcsPathPrefix, @Nullable String gcsServiceAccountKey,
    @Nullable String dsServiceAccountKey, @Nullable String streamId, @Nullable String project) {
    this.usingExistingStream = usingExistingStream;
    this.host = host;
    this.port = port;
    this.user = user;
    this.password = password;
    this.sid = sid;
    this.region = region;
    this.connectivityMethod = connectivityMethod;
    this.sshHost = sshHost;
    this.sshPort = sshPort;
    this.sshUser = sshUser;
    this.sshAuthenticationMethod = sshAuthenticationMethod;
    this.sshPassword = sshPassword;
    this.sshPrivateKey = sshPrivateKey;
    this.gcsBucket = gcsBucket;
    this.gcsPathPrefix = gcsPathPrefix;
    this.gcsServiceAccountKey = gcsServiceAccountKey;
    this.dsServiceAccountKey = dsServiceAccountKey;
    this.streamId = streamId;
    this.project = project;
    validate();
  }

  /**
   * Validate whether the DatastreamConfig has valid fields values. Currently UI cannot hide a
   * property that is not annotated as nullable. So need to validate them at backend.
   */
  public void validate() {

    if (usingExistingStream) {
      if (streamId == null || streamId.isEmpty()) {
        throw new IllegalArgumentException("Id of the existing Datastream stream is missing!");
      }
    } else {
      if (host == null || host.isEmpty()) {
        throw new IllegalArgumentException("Host of the database is missing!");
      }
      if (user == null || user.isEmpty()) {
        throw new IllegalArgumentException("Username of the database is missing!");
      }
      if (password == null || password.isEmpty()) {
        throw new IllegalArgumentException("Password of the database is missing!");
      }
      if (CONNECTIVITY_METHOD_FORWARD_SSH_TUNNEL.equals(connectivityMethod)) {
        // have to annotate sshHost as nullable otherwise we cannot hide it when
        // IP allowlisting is selected as connectivity method
        if (sshHost == null || sshHost.isEmpty()) {
          throw new IllegalArgumentException("Hostname of SSH Server is missing!");
        }

        if (sshUser == null || sshUser.isEmpty()) {
          throw new IllegalArgumentException("Username of SSH server is missing!");
        }

        if (AUTHENTICATION_METHOD_PASSWORD.equals(sshAuthenticationMethod)) {
          if (sshPassword == null || sshPassword.isEmpty()) {
            throw new IllegalArgumentException("Password of SSH server login is missing!");
          }
        } else {
          // take it as the default value  -- private/public key pair
          if (sshPrivateKey == null || sshPrivateKey.isEmpty()) {
            throw new IllegalArgumentException("Private key of SSH server login is missing!");
          }
        }
      }
    }
  }

}
