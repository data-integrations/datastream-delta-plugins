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

package io.cdap.plugin.mysql.utils;

import io.cdap.e2e.utils.PluginPropertyUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *  MySQL client.
 */
public class MysqlClient {

        private static final String database = PluginPropertyUtils.pluginProp("mysqlDatabaseName");
        private static Connection getMysqlConnection() throws SQLException, ClassNotFoundException {
            Class.forName("com.mysql.cj.jdbc.Driver");
            return DriverManager.getConnection("jdbc:mysql://" + System.getenv("MYSQL_HOST") + ":" +
                                                 System.getenv("MYSQL_PORT") + "/" + database,
                                         System.getenv("MYSQL_USERNAME"), System.getenv("MYSQL_PASSWORD"));
    }

    public static void createTable(String sourceTable, String datatypeColumns)
      throws SQLException, ClassNotFoundException {
        try (Connection connect = getMysqlConnection();
             Statement statement = connect.createStatement()) {
            String createSourceTableQuery = "CREATE TABLE " + sourceTable + " " + datatypeColumns;
            statement.executeUpdate(createSourceTableQuery);
            // Insert row1 data.
            String datatypesValues = PluginPropertyUtils.pluginProp("mysqlDatatypeValuesRow1");
            String datatypesColumnsList = PluginPropertyUtils.pluginProp("mysqlDatatypesColumnsList");
            statement.executeUpdate("INSERT INTO " + sourceTable + " " + datatypesColumnsList + " " +
                                      datatypesValues);
            // Insert row2 data.
            String datatypesValues2 = PluginPropertyUtils.pluginProp("mysqlDatatypeValuesRow2");
            String datatypesColumnsList2 = PluginPropertyUtils.pluginProp("mysqlDatatypesColumnsList");
            statement.executeUpdate("INSERT INTO " + sourceTable + " " + datatypesColumnsList2 + " " +
                                      datatypesValues2);
        }
    }

    public static void insertRow (String table, String datatypeValues) throws
            SQLException, ClassNotFoundException {
        try (Connection connect = getMysqlConnection(); Statement statement = connect.createStatement()) {
            // Insert dummy data.
            statement.executeUpdate("INSERT INTO " + table + " " + " VALUES " + datatypeValues);
        }
    }
    public static void deleteRow(String table, String deleteCondition) throws SQLException,
            ClassNotFoundException {
        try (Connection connect = getMysqlConnection(); Statement statement = connect.createStatement()) {
            // Delete dummy data.
            statement.executeUpdate("DELETE FROM " + table + " WHERE " + deleteCondition);
        }
    }
    public static void updateRow(String table, String updateCondition, String updatedValue) throws
            SQLException, ClassNotFoundException {
        try (Connection connect = getMysqlConnection(); Statement statement = connect.createStatement()) {
            // Update dummy data.
            statement.executeUpdate("UPDATE " + table + " SET " + updatedValue +
                    " WHERE " + updateCondition);
        }
    }

    public static void deleteTable(String table) throws SQLException, ClassNotFoundException {
        try (Connection connect = getMysqlConnection();
             Statement statement = connect.createStatement()) {
            {
                String dropTableQuery = "DROP TABLE " + table;
                statement.executeUpdate(dropTableQuery);
            }
        }
    }

    public static List<Map<String, Object>> getMySqlRecordsAsMap(String table) throws SQLException,
            ClassNotFoundException {
        try (Connection connect = getMysqlConnection(); Statement statement = connect.createStatement()) {
            // Insert dummy data.
            List<Map<String, Object>> mySqlRecord = new ArrayList<>();
            String query = "select * from " + table;
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
                        case Types.BLOB:
                            value = result.getBlob(colIndex);
                            break;
                        case Types.VARCHAR:
                            value = result.getByte(colIndex);
                            break;
                        case Types.NUMERIC:
                            value = result.getInt(colIndex);
                            break;
                        case Types.DATE:
                            value = result.getDate(colIndex);
                            break;
                        case Types.FLOAT:
                            value = result.getFloat(colIndex);
                            break;
                        case Types.DOUBLE:
                            value = result.getDouble(colIndex);
                            break;
                        case Types.TIME:
                            value = result.getTime(colIndex);
                            break;

                        default:
            /*
            Convert all data types toString as we convert certain data types to string
            to preserve precision and scale
            */

                            value = result.getString(colIndex);
                            break;
                    }
                    record.put(columnName, value);
                }
                mySqlRecord.add(record);
            }
            return mySqlRecord;
        }
    }
}
