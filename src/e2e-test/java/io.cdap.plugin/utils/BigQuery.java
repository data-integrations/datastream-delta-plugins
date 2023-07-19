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

package io.cdap.plugin.utils;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import io.cdap.e2e.utils.BigQueryClient;
import org.apache.http.ParseException;
import io.cdap.e2e.utils.PluginPropertyUtils;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
/**
 * Contains bq helper methods used in e2e tests.
 */
public class BigQuery {

    public static List<Map<String, Object>> getBigQueryRecordsAsMap(String projectId, String database, String tableName)
            throws IOException, InterruptedException {
        String query = "SELECT TO_JSON(t) EXCEPT ( _row_id, _source_timestamp, _sort) FROM `" + projectId
                + "." + database + "." + tableName + "`";
        List<Map<String, Object>> bqRecords = new ArrayList<>();
        TableResult results = BigQueryClient.getQueryResult(query);
        List<String> columns = new ArrayList<>();
        for (Field field : results.getSchema().getFields()) {
            columns.add(field.getName() + "#" + field.getType());
        }
        for (FieldValueList row : results.getValues()) {
            Map<String, Object> record = new HashMap<>();
            int index = 0;
            for (FieldValue fieldValue : row) {
                String columnName = columns.get(index).split("#")[0];
                String dataType = columns.get(index).split("#")[1];
                Object value;
                switch (dataType) {
                    case "TIMESTAMP":
                        value = fieldValue.getTimestampValue();
                        break;
                    case "BOOLEAN":
                        value = fieldValue.getBooleanValue();
                        break;
                    case "INTEGER":
                        value = fieldValue.getLongValue();
                        break;
                    case "FLOAT":
                        value = fieldValue.getDoubleValue();
                        break;
                    case "BLOB":
                        value = fieldValue.getBytesValue();
                        break;
                    case "DATE":
                        value = parseDate(fieldValue.getStringValue(), "yyyy-MM-dd");
                        break;
                    case "DATETIME":
                        value = parseDate(fieldValue.getStringValue(), "yyyy-MM-dd HH:mm:ss");
                        break;
                    case "TIME":
                        value = parseTime(fieldValue.getStringValue(), "HH:mm:ss");
                        break;
                    default:
                        value = fieldValue.getValue();
                        value = value != null ? value.toString() : null;
                        break;
                }
                record.put(columnName, value);
                index++;
            }
            bqRecords.add(record);
        }
        return bqRecords;
    }

    private static Date parseDate(String dateString, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        try {
            return dateFormat.parse(dateString);
        } catch (ParseException | java.text.ParseException e) {
            e.printStackTrace();
            return null;
        }
    }
    private static Time parseTime(String timeString, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        try {
            Date parsedDate = dateFormat.parse(timeString);
            return new Time(parsedDate.getTime());
        } catch (ParseException | java.text.ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void waitForFlush() throws InterruptedException {
        int flushInterval = Integer.parseInt(PluginPropertyUtils.pluginProp("loadInterval"));
        TimeUnit time = TimeUnit.SECONDS;
        time.sleep(2 * flushInterval + 60);
    }

    public static void deleteTable(String tableName) throws IOException, InterruptedException {
        try {
            BigQueryClient.dropBqQuery(tableName);
            BeforeActions.scenario.write("BQ Target table - " + tableName + " deleted successfully");
        } catch (BigQueryException e) {
            if (e.getMessage().contains("Not found: Table")) {
                BeforeActions.scenario.write("BQ Target Table does not exist");
            }
            Assert.fail(e.getMessage());
        }
    }
}
