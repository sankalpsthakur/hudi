/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.probe;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.util.HoodieSchemaConverter;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validation probe, injected by the fork workflow only (not part of any commit).
 *
 * <p>Runs the two calls that ClusteringOperator#open makes (convert the Flink row type, then
 * HoodieSchemaUtils.asNullable) on a row type that has a BLOB column next to ordinary columns.
 */
class TestProbeFlinkClusteringBlobSchema {

  private static DataType blobType(boolean notNull) {
    DataType reference = DataTypes.ROW(
        DataTypes.FIELD("external_path", DataTypes.STRING().notNull()),
        DataTypes.FIELD("offset", DataTypes.BIGINT()),
        DataTypes.FIELD("length", DataTypes.BIGINT()),
        DataTypes.FIELD("managed", DataTypes.BOOLEAN().notNull()));
    DataType blob = DataTypes.ROW(
        DataTypes.FIELD("type", DataTypes.STRING().notNull()),
        DataTypes.FIELD("data", DataTypes.BYTES()),
        DataTypes.FIELD("reference", reference));
    return notNull ? blob.notNull() : blob.nullable();
  }

  private static RowType rowType(boolean blobNotNull) {
    return (RowType) DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
        DataTypes.FIELD("name", DataTypes.STRING()),
        DataTypes.FIELD("blob_col", blobType(blobNotNull)),
        DataTypes.FIELD("ts", DataTypes.BIGINT().notNull())).notNull().getLogicalType();
  }

  private static List<String> names(HoodieSchema schema) {
    return schema.getFields().stream().map(HoodieSchemaField::name).collect(Collectors.toList());
  }

  private static HoodieSchemaField field(HoodieSchema schema, String name) {
    return schema.getFields().stream().filter(f -> f.name().equals(name)).findFirst()
        .orElseThrow(() -> new AssertionError("column " + name + " missing from " + names(schema)));
  }

  private static void checkAsNullable(boolean blobNotNull) {
    HoodieSchema schema = HoodieSchemaConverter.convertToSchema(
        rowType(blobNotNull), HoodieSchemaUtils.getRecordQualifiedName("probe_table"), "");
    assertEquals(HoodieSchemaType.BLOB, field(schema, "blob_col").schema().getNonNullType().getType(),
        "the Flink row type must map to the Hoodie BLOB type");
    // ClusteringOperator#open: this.readerSchema = HoodieSchemaUtils.asNullable(schema)
    HoodieSchema readerSchema = HoodieSchemaUtils.asNullable(schema);
    assertEquals(names(schema), names(readerSchema), "asNullable must keep the table columns and their names");
    for (HoodieSchemaField f : readerSchema.getFields()) {
      assertTrue(f.schema().isNullable(), "column " + f.name() + " must be nullable: " + f.schema());
    }
    assertEquals(HoodieSchemaType.BLOB, field(readerSchema, "blob_col").schema().getNonNullType().getType());
    assertEquals(HoodieSchemaType.LONG, field(readerSchema, "id").schema().getNonNullType().getType());
    assertEquals(HoodieSchemaType.STRING, field(readerSchema, "name").schema().getNonNullType().getType());
    assertEquals(HoodieSchemaType.LONG, field(readerSchema, "ts").schema().getNonNullType().getType());
  }

  @Test
  void clusteringReaderSchemaKeepsColumnsBesideNullableBlob() {
    checkAsNullable(false);
  }

  @Test
  void clusteringReaderSchemaMakesRequiredBlobNullable() {
    checkAsNullable(true);
  }
}
