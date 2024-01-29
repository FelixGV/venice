package com.linkedin.davinci.schema.merge;

import com.linkedin.davinci.utils.IndexedHashMap;
import com.linkedin.venice.schema.rmd.v1.CollectionRmdTimestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


public class MergeTimestampUtils {
  private MergeTimestampUtils() {
    // Utility class
  }

  static <T> IndexedHashMap<T, Long> createElementToActiveTsMap(
      List<T> existingElements,
      List<Long> activeTimestamps,
      final long topLevelTimestamp,
      final long minTimestamp, // Any timestamp smaller than or equal to this one will not be included in the result
                               // map.
      final int putOnlyPartLength) {
    IndexedHashMap<T, Long> activeElementToTsMap = new IndexedHashMap<>(existingElements.size());
    int idx = 0;
    if (!existingElements.isEmpty() && activeTimestamps instanceof LinkedList) {
      /**
       * LinkedList is not efficient for get operation
       */
      activeTimestamps = new ArrayList<>(activeTimestamps);
    }
    for (T existingElement: existingElements) {
      final long activeTimestamp;
      if (idx < putOnlyPartLength) {
        activeTimestamp = topLevelTimestamp;
      } else {
        activeTimestamp = activeTimestamps.get(idx - putOnlyPartLength);
      }
      if (activeTimestamp > minTimestamp) {
        activeElementToTsMap.put(existingElement, activeTimestamp);
      }
      idx++;
    }
    return activeElementToTsMap;
  }

  static <T> IndexedHashMap<T, Long> createDeletedElementToTsMap(
      List<T> deletedElements,
      List<Long> deletedTimestamps,
      final long minTimestamp // Any deletion timestamp strictly smaller than this one will not be included in the
                              // result map.
  ) {
    IndexedHashMap<T, Long> elementToTimestampMap = new IndexedHashMap<>();
    int idx = 0;
    if (!deletedTimestamps.isEmpty() && deletedElements instanceof LinkedList) {
      /**
       * LinkedList is not efficient for get operation
       */
      deletedElements = new ArrayList<>(deletedElements);
    }
    for (long deletedTimestamp: deletedTimestamps) {
      if (deletedTimestamp >= minTimestamp) {
        elementToTimestampMap.put(deletedElements.get(idx), deletedTimestamp);
      }
      idx++;
    }
    return elementToTimestampMap;
  }

  public static CollectionRmdTimestamp getCollectionRmdTimestamp(
      GenericRecord timestampRecord,
      Schema.Field valueField) {
    Object objectTimestamp = getObjectTimestamp(timestampRecord, valueField, GenericRecord.class);
    return new CollectionRmdTimestamp((GenericRecord) objectTimestamp);
  }

  static long getLongTimestamp(GenericRecord timestampRecord, Schema.Field valueField) {
    return (long) getObjectTimestamp(timestampRecord, valueField, Long.class);
  }

  private static Object getObjectTimestamp(GenericRecord timestampRecord, Schema.Field valueField, Class expectedType) {
    Schema.Field timestampRecordField = getTimestampField(timestampRecord, valueField);
    Object o = timestampRecord.get(timestampRecordField.pos());
    if (!expectedType.isInstance(o)) {
      throw new IllegalArgumentException(
          String.format(
              "Expected timestamp field %s to be a %s, but instead got: %s",
              valueField.name(),
              expectedType.getSimpleName(),
              o));
    }
    return o;
  }

  public static Schema.Field getTimestampField(GenericRecord timestampRecord, Schema.Field valueField) {
    Schema.Field timestampRecordField = timestampRecord.getSchema().getFields().get(valueField.pos());
    if (!timestampRecordField.name().equals(valueField.name())) {
      // This would happen if the schema of the RMD is mismatched with that of the value, which does not seem possible.
      // Out of an abundance of caution, we introduce this fallback here, though if we can convince ourselves that this
      // cannot happen, then we could eliminate this string comparison and make the function a bit more efficient...
      timestampRecordField = timestampRecord.getSchema().getField(valueField.name());
    }
    return timestampRecordField;
  }
}
