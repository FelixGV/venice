package com.linkedin.venice.serializer.avro;

import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * This class is a factory that creates {@link MapOrderPreservingSerializer} and {@link MapOrderPreservingDeserializer}
 * with given schemas and cache them.
 */
public class MapOrderingPreservingSerDeFactory extends SerializerDeserializerFactory {
  private static final Map<Schema, MapOrderPreservingSerializer<?>> SERIALIZER_MAP = new VeniceConcurrentHashMap<>();
  private static final Map<SchemaPairAndClassContainer, RecordDeserializer<GenericRecord>> DESERIALIZER_MAP =
      new VeniceConcurrentHashMap<>();

  public static <K> MapOrderPreservingSerializer<K> getSerializer(Schema schema) {
    return (MapOrderPreservingSerializer<K>) SERIALIZER_MAP
        .computeIfAbsent(schema, s -> new MapOrderPreservingSerializer<>(s));
  }

  public static RecordDeserializer<GenericRecord> getDeserializer(Schema writerSchema, Schema readerSchema) {
    return DESERIALIZER_MAP
        .computeIfAbsent(new SchemaPairAndClassContainer(writerSchema, readerSchema, Object.class), o -> {
          RecordDeserializer<GenericRecord> recordDeserializer =
              FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(writerSchema, readerSchema);
          if (AvroSchemaUtils.schemaContainsTopLevelCollection(readerSchema)) {
            return new MapOrderPreservingDeserializer(recordDeserializer, readerSchema);
          }
          return recordDeserializer;
        });
  }
}
