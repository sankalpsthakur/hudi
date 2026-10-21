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
 * One step of a mixed-version scenario, selected by the environment variables STEP (create | update), NAME (the
 * table directory under TABLE_ROOT) and run with whichever hudi-common the job installed last. The scenario uses
 * schema-on-read together with reconcile-schema and never ALTERs the table, so it is the case where a writer has
 * to keep the persisted InternalSchema up to date on every commit.
 */
class TestProbeDowngradeStep extends HoodieSparkSqlTestBase {

  private val root = sys.env.getOrElse("TABLE_ROOT", "/tmp/downgrade-blob-table")
  private val step = sys.env.getOrElse("STEP", "create")
  private val name = sys.env.getOrElse("NAME", "t")

  private val conf: Seq[(String, String)] = Seq(
    "hoodie.schema.on.read.enable" -> "true",
    "hoodie.datasource.write.reconcile.schema" -> "true",
    "hoodie.datasource.write.schema.allow.auto.evolution.column.drop" -> "true",
    "hoodie.metadata.index.column.stats.enable" -> "false")

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

  test(s"Probe downgrade scenario: step=$step table=$name") {
    withSQLConf(conf: _*) {
      val tableName = s"dg_$name"
      val location = s"$root/$name"
      if (step == "create") {
        spark.sql(
          s"""
             |create table $tableName (
             |  id bigint,
             |  name string,
             |  qty int,
             |  payload BLOB,
             |  ts bigint
             |) using hudi
             | location '$location'
             | tblproperties (
             |  type = 'cow',
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
        spark.sql(s"update $tableName set qty = 7 where id = 1")
        checkAnswer(s"select id, name, qty, payload.reference.external_path from $tableName order by id")(
          Seq(1L, "a1", 7, "blobs/a"),
          Seq(2L, "a2", 6, "blobs/b"))
      } else {
        spark.sql(s"create table $tableName using hudi location '$location'")
        val before = spark.sql(s"select qty from $tableName where id = 2").head.getInt(0)
        spark.sql(s"update $tableName set qty = qty + 1 where id = 2")
        val newId = 100L + (System.currentTimeMillis() % 100000L)
        spark.sql(
          s"""
             |insert into $tableName values
             |  ($newId, 'n$newId', 1, ${blobLiteral("blobs/n", 30, 40)}, 3000)
           """.stripMargin)
        assert(spark.sql(s"select qty from $tableName where id = 2").head.getInt(0) == before + 1)
        assert(spark.sql(s"select id from $tableName where id = $newId").count() == 1)
        checkAnswer(s"select count(*) from $tableName where payload.reference.external_path is null")(Seq(0L))
      }
    }
  }
}
