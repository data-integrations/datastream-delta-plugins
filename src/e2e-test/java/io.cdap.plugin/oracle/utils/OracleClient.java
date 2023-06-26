/*
 * Copyright (c) 2023.
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

package io.cdap.plugin.oracle.utils;

import io.cdap.e2e.utils.PluginPropertyUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 *  Oracle client.
 */
public class OracleClient {
    private static Connection getOracleConnection() throws SQLException, ClassNotFoundException {
        TimeZone timezone = TimeZone.getTimeZone("UTC");
        TimeZone.setDefault(timezone);
        Class.forName("oracle.jdbc.driver.OracleDriver");
        String databaseName = PluginPropertyUtils.pluginProp("dataset");
        String host = PluginPropertyUtils.pluginProp("host");
        String port = PluginPropertyUtils.pluginProp("port");
        String username = PluginPropertyUtils.pluginProp("username");
        String password = PluginPropertyUtils.pluginProp("password");

        return DriverManager.getConnection("jdbc:oracle:thin:@//" + host
                        + ":" + port + "/" + databaseName,
                username, password);
    }

    public static void createTable(String table, String schema, String datatypeColumns)
            throws SQLException, ClassNotFoundException {
        try (Connection connect = getOracleConnection(); Statement statement = connect.createStatement()) {
            String createTableQuery = "CREATE TABLE " + schema + "." + table + datatypeColumns;
            statement.executeUpdate(createTableQuery);
            // Insert row1 data.
            String datatypesValues = PluginPropertyUtils.pluginProp("oracleDatatypeValuesRow1");
            String datatypesColumnsList = PluginPropertyUtils.pluginProp("oracleDatatypesColumnsList");
            statement.executeUpdate("INSERT INTO " + schema + "." + table + " " + datatypesColumnsList + " " +
                                      datatypesValues);
            // Insert row2 data.
            String datatypesValues2 = PluginPropertyUtils.pluginProp("oracleDatatypeValuesRow2");
            String datatypesColumnsList2 = PluginPropertyUtils.pluginProp("oracleDatatypesColumnsList");
            statement.executeUpdate("INSERT INTO " + schema + "." + table + " " + datatypesColumnsList2 + " " +
                                      datatypesValues2);
        }
    }

    public static void forceFlushCDC() throws SQLException, ClassNotFoundException {
        try (Connection connect = getOracleConnection(); Statement statement = connect.createStatement()) {
            /*
            Oracle doesn't immediately flush CDC events, it can automatically happen based
            on time/log file size or can be forced
             */
            statement.executeUpdate("ALTER SYSTEM SWITCH LOGFILE");
        }
    }

    public static void insertRow (String table, String schema, String datatypeValues) throws
            SQLException, ClassNotFoundException {
        try (Connection connect = getOracleConnection(); Statement statement = connect.createStatement()) {
            // Insert dummy data.
            statement.executeUpdate("INSERT INTO " + schema + "." + table + " " +
                    " VALUES " + datatypeValues);
        }
    }
    public static void deleteRow(String table, String schema, String deleteCondition) throws SQLException,
            ClassNotFoundException {
        try (Connection connect = getOracleConnection(); Statement statement = connect.createStatement()) {
            // Insert dummy data.
            statement.executeUpdate("DELETE FROM  " + schema + "." + table + " WHERE " + deleteCondition);
        }
    }
    public static void updateRow(String table, String schema, String updateCondition, String updatedValue) throws
            SQLException, ClassNotFoundException {
        try (Connection connect = getOracleConnection(); Statement statement = connect.createStatement()) {
            // Insert dummy data.
            statement.executeUpdate("UPDATE " + schema + "." + table + " SET " + updatedValue +
                    " WHERE " + updateCondition);
        }
    }

    public static List<Map<String, Object>> getOracleRecordsAsMap(String table, String schema) throws SQLException,
            ClassNotFoundException {
        try (Connection connect = getOracleConnection(); Statement statement = connect.createStatement()) {
            // Insert dummy data.
            List<Map<String, Object>> oracleRecords = new ArrayList<>();
            String query = "select * from " + schema + "." + table;
            ResultSet result = statement.executeQuery(query);

            ResultSetMetaData rsmd = result.getMetaData();
            int numberOfColumns = rsmd.getColumnCount();
            List<String> columns = new ArrayList<>();
            columns.add("");
            for (int colIndex = 1; colIndex <= numberOfColumns; colIndex++) {
                columns.add(rsmd.getColumnName(colIndex) + "#" + rsmd.getColumnType(colIndex));
            }
            while (result.next()) {
                Map<String, Object> record = new HashMap<>();
                for (int colIndex = 1; colIndex <= numberOfColumns; colIndex++) {
                    String columnName = columns.get(colIndex).split("#")[0];
                    int type = Integer.parseInt(columns.get(colIndex).split("#")[1]);
                    Object value;
                    switch (type) {
                        case Types.TIMESTAMP:
                            Instant instant = result.getTimestamp(colIndex).toInstant();
                            //Rounding off as BQ supports till microseconds
                            value = TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) +
                                    TimeUnit.NANOSECONDS.toMicros(instant.getNano());
                            break;
                        default:
                            /*
                            Convert all data types toString as we convert certain data types to string
                            to preserve precision and scale
                             */
                            value = result.getString(colIndex);
                    }
                    record.put(columnName, value);
                }
                oracleRecords.add(record);
            }
            return oracleRecords;
        }
    }

    public static void deleteTable(String schema, String table)
            throws SQLException, ClassNotFoundException {
        try (Connection connect = getOracleConnection(); Statement statement = connect.createStatement()) {
            String dropTableQuery = "DROP TABLE " + schema + "." + table;
            statement.execute(dropTableQuery);
        }
    }
}
