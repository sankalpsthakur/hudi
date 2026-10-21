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

package org.apache.hudi.common.schema.internal.probe;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.schema.internal.Types;
import org.apache.hudi.common.schema.internal.convert.InternalSchemaConverter;
import org.apache.hudi.common.schema.internal.utils.AvroSchemaEvolutionUtils;
import org.apache.hudi.common.schema.internal.utils.SerDeHelper;
import org.apache.hudi.common.util.JsonUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validation probe, injected by the fork workflow only (not part of any commit).
 *
 * <p>Mixed-version behaviour of a table schema that carries a BLOB: the same test runs on the base commit (an
 * "older writer" that only knows the reference ids 0..3) and on the fix (reference ids -13..-16, plus the re-tag
 * of persisted 0..3 on read). The table schema starts with the Hudi meta columns, like a real table, so ids 0..3
 * are the first four meta columns.
 */
class TestProbeBlobIdsMixedVersions {

  private static final int[] LEGACY_IDS = {0, 1, 2, 3};
  private static final int[] CURRENT_IDS = {-13, -14, -15, -16};

  private static HoodieSchema tableSchema(boolean withExtraColumn) {
    List<HoodieSchemaField> fields = new ArrayList<>(Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("name", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.STRING)), null, HoodieSchema.NULL_VALUE),
        HoodieSchemaField.of("payload", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, HoodieSchema.NULL_VALUE)));
    if (withExtraColumn) {
      fields.add(HoodieSchemaField.of("extra", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.STRING)), null, HoodieSchema.NULL_VALUE));
    }
    return HoodieSchemaUtils.addMetadataFields(HoodieSchema.createRecord("rec", null, null, false, fields));
  }

  /** Sets the ids of the four reference fields of every BLOB in an InternalSchema JSON. */
  private static String withReferenceIds(String json, int[] ids) throws Exception {
    JsonNode root = JsonUtils.getObjectMapper().readTree(json);
    rewrite(root, ids);
    return root.toString();
  }

  private static void rewrite(JsonNode node, int[] ids) {
    if (node == null) {
      return;
    }
    if (node.isObject()) {
      JsonNode fields = node.get("fields");
      if (fields != null && fields.isArray() && fields.size() == 4
          && "external_path".equals(fields.get(0).path("name").asText())
          && "managed".equals(fields.get(3).path("name").asText())) {
        for (int i = 0; i < 4; i++) {
          ((ObjectNode) fields.get(i)).put("id", ids[i]);
        }
      }
    }
    node.elements().forEachRemaining(child -> rewrite(child, ids));
  }

  /** Ids of the four reference fields as found by name; Integer.MIN_VALUE for a name the schema cannot resolve. */
  private static int[] referenceIds(InternalSchema schema) {
    String[] names = {"payload.reference.external_path", "payload.reference.offset", "payload.reference.length", "payload.reference.managed"};
    int[] ids = new int[names.length];
    for (int i = 0; i < names.length; i++) {
      Types.Field field = schema.findField(names[i]);
      ids[i] = field == null ? Integer.MIN_VALUE : field.fieldId();
    }
    return ids;
  }

  private static HoodieSchemaType payloadType(InternalSchema schema) {
    HoodieSchema back = InternalSchemaConverter.convert(schema, "rec");
    HoodieSchemaField payload = back.getFields().stream().filter(f -> f.name().equals("payload")).findFirst()
        .orElseThrow(() -> new AssertionError("payload column missing after the round trip"));
    return payload.schema().getNonNullType().getType();
  }

  @Test
  void schemaPersistedWithLegacyIdsMatchesTheCurrentConversionAndSurvivesReconcile() throws Exception {
    HoodieSchema schema = tableSchema(false);
    InternalSchema converted = InternalSchemaConverter.convert(schema);
    InternalSchema persisted = SerDeHelper.fromJson(withReferenceIds(SerDeHelper.toJson(converted), LEGACY_IDS)).get();

    assertTrue(persisted.equalsIgnoringVersion(converted),
        () -> "a schema persisted with the ids 0..3 must equal the one converted by this code; persisted reference ids="
            + Arrays.toString(referenceIds(persisted)) + " converted reference ids=" + Arrays.toString(referenceIds(converted)));
    InternalSchema reconciled = AvroSchemaEvolutionUtils.reconcileSchema(schema, persisted, false, Collections.emptyMap());
    assertTrue(reconciled.equalsIgnoringVersion(persisted), () -> "reconcile against the persisted schema must be a no-op");
    assertEquals(HoodieSchemaType.BLOB, payloadType(persisted));
  }

  @Test
  void schemaPersistedWithCurrentIdsIsToleratedByReconcileAndStillReadsAsBlob() throws Exception {
    HoodieSchema schema = tableSchema(false);
    InternalSchema converted = InternalSchemaConverter.convert(schema);
    InternalSchema persisted = SerDeHelper.fromJson(withReferenceIds(SerDeHelper.toJson(converted), CURRENT_IDS)).get();
    assertNotNull(persisted.findField("payload.reference.external_path"));

    // On the base commit this is the "older writer meets a table written by the fix" case: the incoming schema is
    // converted with the ids 0..3, the persisted one has -13..-16, so the BLOB record differs by ids only.
    InternalSchema reconciled = AvroSchemaEvolutionUtils.reconcileSchema(schema, persisted, false, Collections.emptyMap());
    assertTrue(reconciled.equalsIgnoringVersion(persisted),
        () -> "reconcile must leave the persisted schema alone; reference ids of the result=" + Arrays.toString(referenceIds(reconciled)));
    assertEquals(HoodieSchemaType.BLOB, payloadType(persisted));
  }

  @Test
  void addingAColumnToASchemaPersistedWithCurrentIdsKeepsTheBlob() throws Exception {
    InternalSchema converted = InternalSchemaConverter.convert(tableSchema(false));
    InternalSchema persisted = SerDeHelper.fromJson(withReferenceIds(SerDeHelper.toJson(converted), CURRENT_IDS)).get();

    InternalSchema reconciled = AvroSchemaEvolutionUtils.reconcileSchema(tableSchema(true), persisted, false, Collections.emptyMap());

    assertNotNull(reconciled.findField("extra"), "the new column must be added");
    assertNotNull(reconciled.findField("payload.reference.external_path"));
    assertEquals(HoodieSchemaType.BLOB, payloadType(reconciled));
    for (String column : Arrays.asList("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path",
        "_hoodie_file_name", "id", "name", "payload", "extra")) {
      assertNotNull(reconciled.findField(column), () -> "column " + column + " lost; columns=" + reconciled.getAllColsFullName());
    }
  }

  @Test
  void addingAColumnToASchemaPersistedWithLegacyIdsKeepsTheBlobAndEveryTableColumn() throws Exception {
    InternalSchema converted = InternalSchemaConverter.convert(tableSchema(false));
    InternalSchema persisted = SerDeHelper.fromJson(withReferenceIds(SerDeHelper.toJson(converted), LEGACY_IDS)).get();

    InternalSchema reconciled = AvroSchemaEvolutionUtils.reconcileSchema(tableSchema(true), persisted, false, Collections.emptyMap());

    assertNotNull(reconciled.findField("extra"), "the new column must be added");
    for (String column : Arrays.asList("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path",
        "_hoodie_file_name", "id", "name", "payload", "extra")) {
      assertNotNull(reconciled.findField(column), () -> "column " + column + " lost; columns=" + reconciled.getAllColsFullName());
    }
    assertEquals(HoodieSchemaType.BLOB, payloadType(reconciled));
  }

  /**
   * Not an assertion: prints which names of a table with two BLOB columns still resolve to an id. The nested fields
   * of both columns share the sentinel ids, and the name to id lookup is built by inverting an id to name map, so
   * one of the two columns loses its nested names (on the base commit that already holds for ids -10..-12, and the
   * reference fields were shadowed by the meta columns on top of it).
   */
  @Test
  void twoBlobColumnsSurveyOfResolvableNames() {
    List<HoodieSchemaField> fields = new ArrayList<>(Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("thumb", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, HoodieSchema.NULL_VALUE),
        HoodieSchemaField.of("payload", HoodieSchema.createNullable(HoodieSchema.createBlob()), null, HoodieSchema.NULL_VALUE)));
    InternalSchema schema = InternalSchemaConverter.convert(
        HoodieSchemaUtils.addMetadataFields(HoodieSchema.createRecord("rec", null, null, false, fields)));
    List<String> names = Arrays.asList("id", "_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path",
        "thumb", "thumb.type", "thumb.data", "thumb.reference", "thumb.reference.external_path", "thumb.reference.offset",
        "thumb.reference.length", "thumb.reference.managed",
        "payload", "payload.type", "payload.data", "payload.reference", "payload.reference.external_path", "payload.reference.offset",
        "payload.reference.length", "payload.reference.managed");
    List<String> unresolvable = new ArrayList<>();
    for (String column : names) {
      int id = schema.findIdByName(column);
      System.out.println("SURVEY " + column + " -> " + id);
      if (id == -1) {
        unresolvable.add(column);
      }
    }
    System.out.println("SURVEY unresolvable names: " + unresolvable);
  }
}
