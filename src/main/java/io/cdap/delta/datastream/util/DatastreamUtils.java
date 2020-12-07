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

import com.google.cloud.datastream.v1alpha1.ConnectionProfile;
import com.google.cloud.datastream.v1alpha1.ForwardSshTunnelConnectivity;
import com.google.cloud.datastream.v1alpha1.OracleProfile;
import com.google.cloud.datastream.v1alpha1.StaticServiceIpConnectivity;
import io.cdap.delta.datastream.DatastreamConfig;
import io.cdap.delta.datastream.OracleDataType;

import java.sql.SQLType;

import static io.cdap.delta.datastream.DatastreamConfig.AUTHENTICATION_METHOD_PASSWORD;
import static io.cdap.delta.datastream.DatastreamConfig.AUTHENTICATION_METHOD_PRIVATE_PUBLIC_KEY;
import static io.cdap.delta.datastream.DatastreamConfig.CONNECTIVITY_METHOD_FORWARD_SSH_TUNNEL;
import static io.cdap.delta.datastream.DatastreamConfig.CONNECTIVITY_METHOD_IP_ALLOWLISTING;

/**
 *  Common Utils for Datastream source plugins
 */
public final class DatastreamUtils {

  private DatastreamUtils() { };
  /**
   * Convert the string oracle data type returned by Datastream to SQLType
   *
   * @param oracleDataType Oracle data type in form of string
   * @return
   */
  public static SQLType convertStringDataTypetoSQLType(String oracleDataType) {

    oracleDataType = oracleDataType.toUpperCase();
    if (oracleDataType.startsWith("BINARY FLOAT")) {
      return OracleDataType.BINARY_FLOAT;
    }
    if (oracleDataType.startsWith("DECIMAL")) {
      return OracleDataType.DECIMAL;
    }
    if (oracleDataType.startsWith("FLOAT")) {
      return OracleDataType.FLOAT;
    }
    if (oracleDataType.startsWith("NUMBER")) {
      return OracleDataType.NUMBER;
    }
    switch (oracleDataType) {
      case "ANYDATA":
        return OracleDataType.ANYDATA;
      case "BFILE":
        return OracleDataType.BFILE;
      case "BINARY DOUBLE":
        return OracleDataType.BINARY_DOUBLE;
      case "BLOB":
        return OracleDataType.BLOB;
      case "CHAR":
        return OracleDataType.CHAR;
      case "CLOB":
        return OracleDataType.CLOB;
      case "DATE":
        return OracleDataType.DATE;
      case "DOUBLE PRECISION":
        return OracleDataType.DOUBLE_PRECISION;
      case "INTEGER":
        return OracleDataType.INTEGER;
      case "INTERVAL DAY TO SECOND":
        return OracleDataType.INTERVAL_DAY_TO_SECOND;
      case "INTERVAL DAY TO MONTH":
        return OracleDataType.INTERVAL_YEAR_TO_MONTH;
      case "LONG_RAW":
        return OracleDataType.LONG_RAW;
      case "NCHAR":
        return OracleDataType.NCHAR;
      case "NCLOB":
        return OracleDataType.NCLOB;
      case "NVARCHAR2":
        return OracleDataType.NVARCHAR2;
      case "RAW":
        return OracleDataType.RAW;
      case "REAL":
        return OracleDataType.REAL;
      case "ROWID":
        return OracleDataType.ROWID;
      case "SMALLINT":
        return OracleDataType.SMALLINT;
      case "TIMESTAMP":
        return OracleDataType.TIMESTAMP;
      case "TIMESTAMP WITH TIME ZONE":
        return OracleDataType.TIMESTAMP_WITH_TIME_ZONE;
      case "UDT":
        return OracleDataType.UDT;
      case "VARCHAR":
        return OracleDataType.VARCHAR;
      case "VARCHAR2":
        return OracleDataType.VARCHAR2;
      case "XMLTYPE":
        return OracleDataType.XMLTYPE;
      default:
        return OracleDataType.OTHER;
    }

  }

  public static ConnectionProfile.Builder buildOracleConnectionProfile(DatastreamConfig config) {
    ConnectionProfile.Builder profileBuilder = ConnectionProfile.newBuilder().setOracleProfile(
      OracleProfile.newBuilder().setHostname(config.getHost()).setUsername(config.getUser())
        .setPassword(config.getPassword()).setDatabaseService(config.getSid()).setPort(config.getPort()));
    switch (config.getConnectivityMethod()) {
      case CONNECTIVITY_METHOD_FORWARD_SSH_TUNNEL:
        ForwardSshTunnelConnectivity.Builder forwardSSHTunnelConnectivityBuilder =
          ForwardSshTunnelConnectivity.newBuilder().setHostname(config.getSshHost())
            .setPassword(config.getSshPassword()).setPort(config.getSshPort())
            .setUsername(config.getSshUser());
        switch (config.getSshAuthenticationMethod()) {
          case AUTHENTICATION_METHOD_PASSWORD:
            forwardSSHTunnelConnectivityBuilder.setPassword(config.getSshPassword());
            break;
          case AUTHENTICATION_METHOD_PRIVATE_PUBLIC_KEY:
            forwardSSHTunnelConnectivityBuilder.setPrivateKey(config.getSshPrivateKey());
            break;
          default:
            throw new IllegalArgumentException(
              "Unsupported authentication method: " + config.getSshAuthenticationMethod());
        }
        return profileBuilder.setForwardSshConnectivity(forwardSSHTunnelConnectivityBuilder);
      case CONNECTIVITY_METHOD_IP_ALLOWLISTING:
        return profileBuilder
          .setStaticServiceIpConnectivity(StaticServiceIpConnectivity.getDefaultInstance());
      default:
        throw new IllegalArgumentException(
          "Unsupported connectivity method: " + config.getConnectivityMethod());
    }
  }
}
