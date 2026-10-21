/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.probe

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

/**
 * Validation probe, injected by the fork workflow only (not part of any commit).
 *
 * Phase 1 runs on the BASE commit, i.e. code that gives the BLOB reference fields the ids 0..3 like releases
 * 1.2.0 / 1.2.1: it creates BLOB tables with schema-on-read and ALTERs them, which persists an InternalSchema
 * history with the legacy ids. Phase 2 (TestProbeLegacyBlobPhase2) reopens the same tables with the fix.
 */
class TestProbeLegacyBlobPhase1 extends HoodieSparkSqlTestBase {

  private val root = sys.env.getOrElse("TABLE_ROOT", "/tmp/legacy-blob-table")

  private def blobLiteral(externalPath: String, offset: Long, length: Long): String =
    s"""named_struct(
       |  'type', 'OUT_OF_LINE',
       |  'data', cast(null as binary),
       |  'reference', named_struct(
       |    'external_path', '$externalPath',
       |    'offset', cast($offset as bigint),
       |    'length', cast($length as bigint),
       |    'managed', false
       |  )
       |)""".stripMargin

  Seq("cow", "mor").foreach { tableType =>
    test(s"Probe phase 1: create a BLOB table with schema on read and ALTER it with the base code ($tableType)") {
      withSQLConf(
        "hoodie.schema.on.read.enable" -> "true",
        "hoodie.datasource.write.schema.allow.auto.evolution.column.drop" -> "true",
        "hoodie.metadata.index.column.stats.enable" -> "false") {
        val tableName = s"legacy_blob_$tableType"
        spark.sql(
          s"""
             |create table $tableName (
             |  id bigint,
             |  name string,
             |  qty int,
             |  payload BLOB,
             |  ts bigint
             |) using hudi
             | location '$root/$tableType'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  orderingFields = 'ts'
             | )
           """.stripMargin)
        spark.sql(
          s"""
             |insert into $tableName values
             |  (1, 'a1', 5, ${blobLiteral("blobs/a", 0, 10)}, 1000),
             |  (2, 'a2', 6, ${blobLiteral("blobs/b", 10, 20)}, 1000)
           """.stripMargin)
        // the first ALTER converts the table's HoodieSchema into an InternalSchema and persists it
        spark.sql(s"alter table $tableName add columns (extra string)")
      }
    }
  }
}
