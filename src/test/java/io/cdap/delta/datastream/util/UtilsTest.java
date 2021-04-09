/*
 *
 * Copyright © 2021 Cask Data, Inc.
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

import io.cdap.delta.datastream.OracleDataType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UtilsTest {
  @Test
  public void testConvertStringDataTypeToSQLType() {
    assertEquals(OracleDataType.BINARY_FLOAT, Utils.convertStringDataTypeToSQLType("BINARY FLOAT"));
    assertEquals(OracleDataType.DECIMAL, Utils.convertStringDataTypeToSQLType("DECIMAL"));
    assertEquals(OracleDataType.FLOAT, Utils.convertStringDataTypeToSQLType("FLOAT"));
    assertEquals(OracleDataType.NUMBER, Utils.convertStringDataTypeToSQLType("NUMBER"));
    assertEquals(OracleDataType.TIMESTAMP, Utils.convertStringDataTypeToSQLType("TIMESTAMP"));
    assertEquals(OracleDataType.TIMESTAMP_WITH_TIME_ZONE,
      Utils.convertStringDataTypeToSQLType("TIMESTAMP WITH TIME ZONE"));
    assertEquals(OracleDataType.ANYDATA, Utils.convertStringDataTypeToSQLType("ANYDATA"));
    assertEquals(OracleDataType.BFILE, Utils.convertStringDataTypeToSQLType("BFILE"));
    assertEquals(OracleDataType.BINARY_DOUBLE, Utils.convertStringDataTypeToSQLType("BINARY DOUBLE"));
    assertEquals(OracleDataType.BLOB, Utils.convertStringDataTypeToSQLType("BLOB"));
    assertEquals(OracleDataType.CHAR, Utils.convertStringDataTypeToSQLType("CHAR"));
    assertEquals(OracleDataType.CLOB, Utils.convertStringDataTypeToSQLType("CLOB"));
    assertEquals(OracleDataType.DATE, Utils.convertStringDataTypeToSQLType("DATE"));
    assertEquals(OracleDataType.DOUBLE_PRECISION, Utils.convertStringDataTypeToSQLType("DOUBLE PRECISION"));
    assertEquals(OracleDataType.INTEGER, Utils.convertStringDataTypeToSQLType("INTEGER"));
    assertEquals(OracleDataType.INTERVAL_DAY_TO_SECOND, Utils.convertStringDataTypeToSQLType("INTERVAL DAY TO SECOND"));
    assertEquals(OracleDataType.INTERVAL_YEAR_TO_MONTH, Utils.convertStringDataTypeToSQLType("INTERVAL YEAR TO MONTH"));
    assertEquals(OracleDataType.LONG, Utils.convertStringDataTypeToSQLType("LONG"));
    assertEquals(OracleDataType.LONG_RAW, Utils.convertStringDataTypeToSQLType("LONG RAW"));
    assertEquals(OracleDataType.NCHAR, Utils.convertStringDataTypeToSQLType("NCHAR"));
    assertEquals(OracleDataType.NCLOB, Utils.convertStringDataTypeToSQLType("NCLOB"));
    assertEquals(OracleDataType.NVARCHAR2, Utils.convertStringDataTypeToSQLType("NVARCHAR2"));
    assertEquals(OracleDataType.RAW, Utils.convertStringDataTypeToSQLType("RAW"));
    assertEquals(OracleDataType.REAL, Utils.convertStringDataTypeToSQLType("REAL"));
    assertEquals(OracleDataType.ROWID, Utils.convertStringDataTypeToSQLType("ROWID"));
    assertEquals(OracleDataType.SMALLINT, Utils.convertStringDataTypeToSQLType("SMALLINT"));
    assertEquals(OracleDataType.UDT, Utils.convertStringDataTypeToSQLType("UDT"));
    assertEquals(OracleDataType.VARCHAR, Utils.convertStringDataTypeToSQLType("VARCHAR"));
    assertEquals(OracleDataType.VARCHAR2, Utils.convertStringDataTypeToSQLType("VARCHAR2"));
    assertEquals(OracleDataType.XMLTYPE, Utils.convertStringDataTypeToSQLType("XMLTYPE"));
    assertEquals(OracleDataType.OTHER, Utils.convertStringDataTypeToSQLType("OTHER"));
  }

  @Test
  public void testOracleDataType() {
    assertEquals("INTEGER", OracleDataType.INTEGER.getName());
    assertEquals("Datastream", OracleDataType.INTEGER.getVendor());
    assertEquals(4, OracleDataType.INTEGER.getVendorTypeNumber());
  }
}
