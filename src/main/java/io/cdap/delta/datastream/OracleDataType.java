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

import java.sql.SQLType;

/**
 * OracleDataType represents the Oracle data types supported by Datastream
 * oracle data type supported by Datastream can be found here:
 * https://cloud.google.com/datastream/docs/sourcedb/unitypes?hl=en
 */
public enum OracleDataType implements SQLType {

  ANYDATA("ANYDATA", 2007),
  BFILE("BFILE", -13),
  BINARY_DOUBLE("BINARY DOUBLE", 101),
  BINARY_FLOAT("BINARY FLOAT", 100),
  BLOB("BLOB", 2004),
  CHAR("CHAR", 1),
  CLOB("CLOB", 2005),
  DATE("DATE", 91),
  DECIMAL("DECIMAL", 3),
  DOUBLE_PRECISION("DOUBLE_PRECISION", 8),
  FLOAT("FLOAT", 6),
  INTEGER("INTEGER", 4),
  INTERVAL_DAY_TO_SECOND("INTERVAL DAY TO SECOND", -104),
  INTERVAL_YEAR_TO_MONTH("INTERVAL YEAR TO MONTH", -103),
  LONG_RAW("LONG RAW", -4),
  NCHAR("NCHAR", -15),
  NCLOB("NCLOB", 2011),
  NUMBER("NUMBER", 2),
  NVARCHAR2("NVARCHAR2", -9),
  OTHER("OTHER", 1111),
  RAW("RAW", -2),
  REAL("REAL", 7),
  ROWID("ROWID", -8),
  SMALLINT("SMALLINT", 5),
  TIMESTAMP("TIMESTAMP", 93),
  TIMESTAMP_WITH_TIME_ZONE("TIMESTAMP WITH TIME ZONE", -101),
  UDT("UDT", -2147483648),
  UROWID("UROWID", -2147483648),
  VARCHAR("VARCHAR", 12),
  VARCHAR2("VARCHAR2", 12),
  XMLTYPE("XMLTYPE", 2009);

  private final String typeName;
  private final int code;

  OracleDataType(String typeName, int code) {
    this.typeName = typeName;
    this.code = code;
  }

  public String getName() {
    return this.typeName;
  }

  public String getVendor() {
    return "Datastream";
  }

  public Integer getVendorTypeNumber() {
    return this.code;
  }
}
