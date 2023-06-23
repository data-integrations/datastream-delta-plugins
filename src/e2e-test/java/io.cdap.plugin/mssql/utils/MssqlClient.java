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

package io.cdap.plugin.mssql.utils;

import io.cdap.e2e.utils.PluginPropertyUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.TimeZone;

/**
 *  Mssql client.
 */
public class MssqlClient {

    private static Connection getMssqlConnection() throws SQLException, ClassNotFoundException {
        TimeZone timezone = TimeZone.getTimeZone("UTC");
        TimeZone.setDefault(timezone);
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        String databaseName = PluginPropertyUtils.pluginProp("databaseName");
        return DriverManager.getConnection("jdbc:sqlserver://" + System.getenv("MSSQL_HOST")
                                             + ":" + System.getenv("MSSQL_PORT") + ";databaseName=" + databaseName,
                                           System.getenv("MSSQL_USERNAME"), System.getenv("MSSQL_PASSWORD"));

    }

}
