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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import org.junit.Assert;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ValidationHelper {

    public static List<Map<String, Object>> getBigQueryRecordsAsMap(String projectId, String database, String tableName)
            throws IOException, InterruptedException {
        String query = "SELECT * EXCEPT ( _row_id, _source_timestamp, _sort) FROM `" + projectId
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
                if(dataType.equalsIgnoreCase("TIMESTAMP")){
                    value = fieldValue.getTimestampValue();
                } else {
                    value = fieldValue.getValue();
                    value = value != null ? value.toString() : null;
                }
                record.put(columnName, value);
                index++;
            }
            bqRecords.add(record);
        }
        return bqRecords;
    }

    public static void validateRecords(List<Map<String, Object>> sourceOracleRecords,
                                       List<Map<String, Object>> targetBigQueryRecords) {

        String uniqueField = PluginPropertyUtils.pluginProp("primaryKey");
        // Logic to maintain the order of both lists and validate records based on that order.
        Map bqUniqueIdMap = (Map)targetBigQueryRecords.stream()
                .filter(t -> t.get("_is_deleted")==null)
                .collect(Collectors.toMap(
                        t -> t.get(uniqueField),
                        t -> t,
                        //This logic is to handle duplication scenario in bq target
                        (x,y) -> {
                            Long xSeqNum = Long.parseLong(x.get("_sequence_num").toString());
                            Long ySeqNum = Long.parseLong(y.get("_sequence_num").toString());
                            if(xSeqNum > ySeqNum){
                                return x;
                            }
                            return y;
                        }));

        for (int record = 0; record < sourceOracleRecords.size(); record++) {
            Map<String, Object> oracleRecord = sourceOracleRecords.get(record);
            Object uniqueId = oracleRecord.get(uniqueField);
            Map<String, Object> bqRow = (Map<String, Object>) bqUniqueIdMap.get(uniqueId);
            if (bqRow != null) {
                bqRow.remove("_is_deleted");
                bqRow.remove("_sequence_num");
            }
            compareRecords(bqRow, oracleRecord);
        }
    }

    public static void compareRecords(Map<String, Object> targetBigQueryRecords,
                                      Map<String, Object> sourceOracleRecord) {
        Set<String> bigQueryKeySet = targetBigQueryRecords.keySet();
        for (String field : bigQueryKeySet) {
            Object bigQueryFieldValue = targetBigQueryRecords.get(field);
            Object oracleFieldValue = sourceOracleRecord.get(field);
            Assert.assertEquals(String.format("Field %s is not equal: expected %s but got %s", field,
                            oracleFieldValue, bigQueryFieldValue),
                    oracleFieldValue, bigQueryFieldValue);
        }
    }

}
