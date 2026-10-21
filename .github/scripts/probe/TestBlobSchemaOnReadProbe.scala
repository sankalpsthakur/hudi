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
 * A BLOB column next to ordinary columns, with schema-on-read switched on. The first ALTER TABLE converts the
 * table's HoodieSchema into an InternalSchema (AlterTableCommand), which is where the BLOB reference fields used
 * to get ids that collide with the first columns. Every case then writes again (update + insert, which merges
 * into the existing file group with the evolved schema) and reads everything back.
 */
class TestBlobSchemaOnReadProbe extends HoodieSparkSqlTestBase {

  // The same settings the existing add / rename / drop column tests of TestSpark3DDL use.
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

  private def createBlobTable(tableName: String, location: String, tableType: String): Unit = {
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
  }

  Seq("cow", "mor").foreach { tableType =>
    test(s"Probe: add a column beside a BLOB column with schema on read ($tableType)") {
      withSQLConf(evolutionConf: _*) {
        withTempDir { tmp =>
          val tableName = generateTableName
          createBlobTable(tableName, s"${tmp.getCanonicalPath}/$tableName", tableType)

          spark.sql(s"alter table $tableName add columns (extra string)")
          spark.sql(s"update $tableName set qty = 7 where id = 1")
          spark.sql(s"insert into $tableName values (3, 'a3', 8, ${blobLiteral("blobs/c", 20, 30)}, 2000, 'x')")

          checkAnswer(s"select id, name, qty, extra, payload.reference.external_path from $tableName order by id")(
            Seq(1L, "a1", 7, null, "blobs/a"),
            Seq(2L, "a2", 6, null, "blobs/b"),
            Seq(3L, "a3", 8, "x", "blobs/c"))
          checkAnswer(s"select _hoodie_record_key from $tableName order by _hoodie_record_key")(
            Seq("1"), Seq("2"), Seq("3"))
        }
      }
    }

    test(s"Probe: rename a column beside a BLOB column with schema on read ($tableType)") {
      withSQLConf(evolutionConf: _*) {
        withTempDir { tmp =>
          val tableName = generateTableName
          createBlobTable(tableName, s"${tmp.getCanonicalPath}/$tableName", tableType)

          spark.sql(s"alter table $tableName rename column name to name2")
          spark.sql(s"update $tableName set name2 = 'z1' where id = 1")
          spark.sql(s"insert into $tableName values (3, 'a3', 8, ${blobLiteral("blobs/c", 20, 30)}, 2000)")

          checkAnswer(s"select id, name2, qty, payload.reference.external_path from $tableName order by id")(
            Seq(1L, "z1", 5, "blobs/a"),
            Seq(2L, "a2", 6, "blobs/b"),
            Seq(3L, "a3", 8, "blobs/c"))
          checkAnswer(s"select _hoodie_record_key from $tableName order by _hoodie_record_key")(
            Seq("1"), Seq("2"), Seq("3"))
        }
      }
    }

    test(s"Probe: change a column type beside a BLOB column with schema on read ($tableType)") {
      withSQLConf(evolutionConf: _*) {
        withTempDir { tmp =>
          val tableName = generateTableName
          createBlobTable(tableName, s"${tmp.getCanonicalPath}/$tableName", tableType)

          spark.sql(s"alter table $tableName alter column qty type bigint")
          spark.sql(s"update $tableName set qty = 7 where id = 1")
          spark.sql(s"insert into $tableName values (3, 'a3', 8, ${blobLiteral("blobs/c", 20, 30)}, 2000)")

          checkAnswer(s"select id, name, qty, payload.reference.external_path from $tableName order by id")(
            Seq(1L, "a1", 7L, "blobs/a"),
            Seq(2L, "a2", 6L, "blobs/b"),
            Seq(3L, "a3", 8L, "blobs/c"))
          checkAnswer(s"select _hoodie_record_key from $tableName order by _hoodie_record_key")(
            Seq("1"), Seq("2"), Seq("3"))
        }
      }
    }

    test(s"Probe: drop a column beside a BLOB column with schema on read ($tableType)") {
      withSQLConf(evolutionConf: _*) {
        withTempDir { tmp =>
          val tableName = generateTableName
          createBlobTable(tableName, s"${tmp.getCanonicalPath}/$tableName", tableType)

          spark.sql(s"alter table $tableName drop column name")
          spark.sql(s"update $tableName set qty = 7 where id = 1")
          spark.sql(s"insert into $tableName values (3, 8, ${blobLiteral("blobs/c", 20, 30)}, 2000)")

          checkAnswer(s"select id, qty, payload.reference.external_path from $tableName order by id")(
            Seq(1L, 7, "blobs/a"),
            Seq(2L, 6, "blobs/b"),
            Seq(3L, 8, "blobs/c"))
          checkAnswer(s"select _hoodie_record_key from $tableName order by _hoodie_record_key")(
            Seq("1"), Seq("2"), Seq("3"))
        }
      }
    }
  }
}
