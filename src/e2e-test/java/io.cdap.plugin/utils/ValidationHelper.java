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

import io.cdap.e2e.utils.PluginPropertyUtils;
import org.junit.Assert;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
/**
 * Contains validation method from oracle to bq used in e2e tests.
 */
public class ValidationHelper {
    public static void validateRecords(List<Map<String, Object>> sourceOracleRecords,
                                       List<Map<String, Object>> targetBigQueryRecords) {

        String uniqueField = PluginPropertyUtils.pluginProp("primaryKey");
        // Logic to maintain the order of both lists and validate records based on that order.
        Map bqUniqueIdMap = null;
        try {
            bqUniqueIdMap = (Map) targetBigQueryRecords.stream()
                    .filter(t -> t.get("_is_deleted") == null)
                    .collect(Collectors.toMap(
                            t -> t.get(uniqueField),
                            t -> t));
        } catch (IllegalStateException e) {
            Assert.fail("Duplication found in Big Query target table");
        }

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
