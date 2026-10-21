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
 * Phase 2 runs on the FIX and reopens the tables written and ALTERed by TestProbeLegacyBlobPhase1 (base code, so
 * their persisted InternalSchema carries the legacy reference ids 0..3).
 */
class TestProbeLegacyBlobPhase2 extends HoodieSparkSqlTestBase {

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
    test(s"Probe phase 2: read, update, insert and ALTER a table persisted with legacy BLOB ids ($tableType)") {
      withSQLConf(
        "hoodie.schema.on.read.enable" -> "true",
        "hoodie.datasource.write.schema.allow.auto.evolution.column.drop" -> "true",
        "hoodie.metadata.index.column.stats.enable" -> "false") {
        val tableName = s"legacy_blob_$tableType"
        spark.sql(s"create table $tableName using hudi location '$root/$tableType'")

        checkAnswer(s"select id, name, qty, extra, payload.reference.external_path from $tableName order by id")(
          Seq(1L, "a1", 5, null, "blobs/a"),
          Seq(2L, "a2", 6, null, "blobs/b"))

        spark.sql(s"update $tableName set qty = 7 where id = 1")
        spark.sql(s"insert into $tableName values (3, 'a3', 8, ${blobLiteral("blobs/c", 20, 30)}, 2000, 'x')")

        checkAnswer(s"select id, name, qty, extra, payload.reference.external_path from $tableName order by id")(
          Seq(1L, "a1", 7, null, "blobs/a"),
          Seq(2L, "a2", 6, null, "blobs/b"),
          Seq(3L, "a3", 8, "x", "blobs/c"))

        // evolve once more with the fixed code, then write again
        spark.sql(s"alter table $tableName add columns (extra2 string)")
        spark.sql(s"update $tableName set extra2 = 'e' where id = 2")

        checkAnswer(s"select id, qty, extra, extra2, payload.reference.length from $tableName order by id")(
          Seq(1L, 7, null, null, 10L),
          Seq(2L, 6, null, "e", 20L),
          Seq(3L, 8, "x", null, 30L))
        checkAnswer(s"select _hoodie_record_key from $tableName order by _hoodie_record_key")(
          Seq("1"), Seq("2"), Seq("3"))
      }
    }
  }
}
