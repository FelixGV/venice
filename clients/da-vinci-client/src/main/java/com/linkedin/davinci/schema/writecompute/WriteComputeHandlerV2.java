package com.linkedin.davinci.schema.writecompute;

import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_NAME;
import static com.linkedin.venice.schema.rmd.RmdConstants.TIMESTAMP_FIELD_POS;

import com.linkedin.davinci.schema.SchemaUtils;
import com.linkedin.davinci.schema.merge.AvroCollectionElementComparator;
import com.linkedin.davinci.schema.merge.CollectionFieldOperationHandler;
import com.linkedin.davinci.schema.merge.MergeRecordHelper;
import com.linkedin.davinci.schema.merge.MergeTimestampUtils;
import com.linkedin.davinci.schema.merge.SortBasedCollectionFieldOpHandler;
import com.linkedin.davinci.schema.merge.UpdateResultStatus;
import com.linkedin.davinci.schema.merge.ValueAndRmd;
import com.linkedin.davinci.utils.IndexedHashMap;
import com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp;
import com.linkedin.venice.schema.writecompute.WriteComputeConstants;
import com.linkedin.venice.schema.writecompute.WriteComputeHandlerV1;
import com.linkedin.venice.schema.writecompute.WriteComputeOperation;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.SparseConcurrentList;
import com.linkedin.venice.utils.collections.BiIntKeyCache;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.Validate;


/**
 * Write compute V2 handles value records with replication metadata.
 */
public class WriteComputeHandlerV2 extends WriteComputeHandlerV1 {
  private final MergeRecordHelper mergeRecordHelper;
  private final CollectionFieldOperationHandler collectionFieldOperationHandler;

  /**
   * This list of lists of list allows us to avoid looking up by field name, which requires doing a hash code of the
   * name string. This hash code was demonstrated to be high overhead in a CPU profiling.
   *
   * 1st ID: incoming value schema ID
   * 2nd ID: incoming update protocol version
   * 3rd ID: current value schema ID
   */
  private final BiIntKeyCache<SparseConcurrentList<FieldMapping[]>> fieldMappingCache =
      new BiIntKeyCache<>((first, second) -> new SparseConcurrentList<>());

  WriteComputeHandlerV2(MergeRecordHelper mergeRecordHelper) {
    Validate.notNull(mergeRecordHelper);
    this.mergeRecordHelper = mergeRecordHelper;
    // TODO: get this variable as a argument passed to this constructor.
    this.collectionFieldOperationHandler =
        new SortBasedCollectionFieldOpHandler(AvroCollectionElementComparator.INSTANCE);
  }

  /**
   * Handle partial update request on a value record that has associated replication metadata.
   */
  public ValueAndRmd<GenericRecord> updateRecordWithRmd(
      @Nonnull Schema currValueSchema,
      @Nonnull ValueAndRmd<GenericRecord> currRecordAndRmd,
      @Nonnull GenericRecord writeComputeRecord,
      int incomingValueSchemaId,
      int incomingUpdateProtocolVersion,
      final long updateOperationTimestamp,
      final int coloID) {
    // For now we always create a record if the current one is null. But there could be a case where the created record
    // does not get updated as a result of this update method. In this case, the current record should stay being null
    // instead
    // of being all record with all fields having their default value. TODO: handle this case.
    GenericRecord currValueRecord = currRecordAndRmd.getValue();
    if (currValueRecord == null) {
      currRecordAndRmd.setValue(AvroSchemaUtils.createGenericRecord(currValueSchema));
    }

    Object timestampObject = currRecordAndRmd.getRmd().get(TIMESTAMP_FIELD_POS);
    if (!(timestampObject instanceof GenericRecord)) {
      throw new IllegalStateException(
          String.format(
              "Expect the %s field to have a generic record. Got replication metadata: %s",
              TIMESTAMP_FIELD_NAME,
              currRecordAndRmd.getRmd()));
    }

    final GenericRecord timestampRecord = (GenericRecord) timestampObject;
    if (!WriteComputeOperation.isPartialUpdateOp(writeComputeRecord)) {
      // This Write Compute record could be a Write Compute Delete request which is not supported and there should be no
      // one using it.
      throw new IllegalStateException(
          "Write Compute only support partial update. Got unexpected Write Compute record: " + writeComputeRecord);
    }
    boolean notUpdated = true;
    final Schema writeComputeSchema = writeComputeRecord.getSchema();
    SparseConcurrentList<FieldMapping[]> fieldMappingsForIncomingUpdate =
        fieldMappingCache.get(incomingValueSchemaId, incomingUpdateProtocolVersion);
    FieldMapping[] fieldMappingsForCurrentValue =
        fieldMappingsForIncomingUpdate.computeIfAbsent(currRecordAndRmd.getValueSchemaId(), currValueSchemaId -> {
          FieldMapping[] result = new FieldMapping[writeComputeSchema.getFields().size()];
          int index = 0;
          Schema valueSchema = currRecordAndRmd.getValue().getSchema();
          for (Schema.Field writeComputeField: writeComputeSchema.getFields()) {
            result[index++] = new FieldMapping(writeComputeField, valueSchema.getField(writeComputeField.name()));
          }
          return result;
        });
    for (FieldMapping fieldMapping: fieldMappingsForCurrentValue) {
      if (fieldMapping.valueField == null) {
        throw new IllegalStateException(
            "Current value record must have a schema that has the same field names as the "
                + "write compute schema because the current value's schema should be the schema that is used to generate "
                + "the write-compute schema. Got missing field: " + fieldMapping.updateField.name());
      }

      Object writeComputeFieldValue = writeComputeRecord.get(fieldMapping.updateField.pos());
      WriteComputeOperation operationType = WriteComputeOperation.getFieldOperationType(writeComputeFieldValue);
      switch (operationType) {
        case NO_OP_ON_FIELD:
          continue; // Do nothing

        case PUT_NEW_FIELD:
          UpdateResultStatus putResult = mergeRecordHelper.putOnField(
              currRecordAndRmd.getValue(),
              timestampRecord,
              fieldMapping.valueField,
              writeComputeFieldValue,
              updateOperationTimestamp,
              coloID);
          notUpdated &= (putResult.equals(UpdateResultStatus.NOT_UPDATED_AT_ALL));
          continue;

        case LIST_OPS:
        case MAP_OPS:
          UpdateResultStatus collectionMergeResult = modifyCollectionField(
              MergeTimestampUtils.getCollectionRmdTimestamp(timestampRecord, fieldMapping.valueField),
              (GenericRecord) writeComputeFieldValue,
              updateOperationTimestamp,
              currRecordAndRmd.getValue(),
              fieldMapping.valueField);
          notUpdated &= (collectionMergeResult.equals(UpdateResultStatus.NOT_UPDATED_AT_ALL));
          continue;
        default:
          throw new IllegalStateException("Unexpected write-compute operation: " + operationType);
      }
    }
    if (notUpdated) {
      currRecordAndRmd.setUpdateIgnored(true);
    }
    return currRecordAndRmd;
  }

  private UpdateResultStatus modifyCollectionField(
      CollectionRmdTimestamp fieldTimestampRecord,
      GenericRecord fieldWriteComputeRecord,
      long modifyTimestamp,
      GenericRecord currValueRecord,
      Schema.Field currentValueField) {
    switch (SchemaUtils.unwrapOptionalUnion(currentValueField.schema())) {
      case ARRAY:
        return collectionFieldOperationHandler.handleModifyList(
            modifyTimestamp,
            fieldTimestampRecord,
            currValueRecord,
            currentValueField,
            (List<Object>) fieldWriteComputeRecord.get(WriteComputeConstants.SET_UNION),
            (List<Object>) fieldWriteComputeRecord.get(WriteComputeConstants.SET_DIFF));
      case MAP:
        Object fieldValue = currValueRecord.get(currentValueField.pos());
        if (fieldValue != null && !(fieldValue instanceof IndexedHashMap)) {
          // if the current map field is not of IndexedHashMap type and is empty then replace this field with an empty
          // IndexedHashMap
          if (((Map) fieldValue).isEmpty()) {
            currValueRecord.put(currentValueField.pos(), new IndexedHashMap<>());
          } else {
            throw new IllegalStateException(
                "Expect value of field " + currentValueField.name() + " to be an IndexedHashMap. Got: "
                    + fieldValue.getClass());
          }
        }
        return collectionFieldOperationHandler.handleModifyMap(
            modifyTimestamp,
            fieldTimestampRecord,
            currValueRecord,
            currentValueField,
            (Map<String, Object>) fieldWriteComputeRecord.get(WriteComputeConstants.MAP_UNION),
            (List<String>) fieldWriteComputeRecord.get(WriteComputeConstants.MAP_DIFF));
      default:
        throw new IllegalArgumentException(
            String.format(
                "Expect value field %s to be either a List or a Map. Got value record: %s",
                currentValueField.name(),
                currValueRecord));
    }
  }

  private static class FieldMapping {
    final Schema.Field updateField;
    final Schema.Field valueField;

    private FieldMapping(Schema.Field updateField, Schema.Field valueField) {
      this.updateField = updateField;
      this.valueField = valueField;
    }
  }
}
