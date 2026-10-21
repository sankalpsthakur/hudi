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
 * Several BLOB columns in one table share the same sentinel ids for their nested fields (-10..-16), and a BLOB can
 * sit inside a struct. The fix keeps that sharing (it only moves the four reference fields off the ids 0..3); this
 * probe checks that schema-on-read ALTER TABLE / update / insert / select still work for both shapes.
 */
class TestProbeTwoBlobColumns extends HoodieSparkSqlTestBase {

  private val evolutionConf: Seq[(String, String)] = Seq(
    "hoodie.schema.on.read.enable" -> "true",
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

  Seq("cow", "mor").foreach { tableType =>
    test(s"Probe: two BLOB columns, add a column and write again with schema on read ($tableType)") {
      withSQLConf(evolutionConf: _*) {
        withTempDir { tmp =>
          val tableName = generateTableName
          spark.sql(
            s"""
               |create table $tableName (
               |  id bigint,
               |  name string,
               |  thumb BLOB,
               |  payload BLOB,
               |  ts bigint
               |) using hudi
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  orderingFields = 'ts'
               | )
             """.stripMargin)
          spark.sql(
            s"""
               |insert into $tableName values
               |  (1, 'a1', ${blobLiteral("thumbs/a", 0, 1)}, ${blobLiteral("blobs/a", 0, 10)}, 1000),
               |  (2, 'a2', ${blobLiteral("thumbs/b", 1, 2)}, ${blobLiteral("blobs/b", 10, 20)}, 1000)
             """.stripMargin)

          spark.sql(s"alter table $tableName add columns (extra string)")
          spark.sql(s"update $tableName set name = 'z1' where id = 1")
          spark.sql(
            s"""
               |insert into $tableName values
               |  (3, 'a3', ${blobLiteral("thumbs/c", 2, 3)}, ${blobLiteral("blobs/c", 20, 30)}, 2000, 'x')
             """.stripMargin)

          checkAnswer(
            s"select id, name, extra, thumb.reference.external_path, payload.reference.external_path from $tableName order by id")(
            Seq(1L, "z1", null, "thumbs/a", "blobs/a"),
            Seq(2L, "a2", null, "thumbs/b", "blobs/b"),
            Seq(3L, "a3", "x", "thumbs/c", "blobs/c"))
          checkAnswer(s"select _hoodie_record_key from $tableName order by _hoodie_record_key")(
            Seq("1"), Seq("2"), Seq("3"))
        }
      }
    }
  }

  test("Probe: a BLOB nested in a struct, add a column and write again with schema on read (cow)") {
    withSQLConf(evolutionConf: _*) {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id bigint,
             |  name string,
             |  media STRUCT<title: STRING, content: BLOB>,
             |  ts bigint
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             |  type = 'cow',
             |  primaryKey = 'id',
             |  orderingFields = 'ts'
             | )
           """.stripMargin)
        spark.sql(
          s"""
             |insert into $tableName values
             |  (1, 'a1', named_struct('title', 't1', 'content', ${blobLiteral("blobs/a", 0, 10)}), 1000),
             |  (2, 'a2', named_struct('title', 't2', 'content', ${blobLiteral("blobs/b", 10, 20)}), 1000)
           """.stripMargin)

        spark.sql(s"alter table $tableName add columns (extra string)")
        spark.sql(s"update $tableName set name = 'z1' where id = 1")
        spark.sql(
          s"""
             |insert into $tableName values
             |  (3, 'a3', named_struct('title', 't3', 'content', ${blobLiteral("blobs/c", 20, 30)}), 2000, 'x')
           """.stripMargin)

        checkAnswer(s"select id, name, extra, media.title, media.content.reference.external_path from $tableName order by id")(
          Seq(1L, "z1", null, "t1", "blobs/a"),
          Seq(2L, "a2", null, "t2", "blobs/b"),
          Seq(3L, "a3", "x", "t3", "blobs/c"))
      }
    }
  }
}
