package com.linkedin.venice.serializer;

import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.utils.BiIntFunction;
import com.linkedin.venice.utils.SparseConcurrentList;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import org.apache.avro.Schema;


/**
 * Container for the deserializers of a single store.
 */
public class StoreDeserializerCache<T> {
  SparseConcurrentList<SparseConcurrentList<RecordDeserializer<T>>> genericDeserializers = new SparseConcurrentList<>();
  BiIntFunction<RecordDeserializer<T>> deserializerGenerator;

  public StoreDeserializerCache(ReadOnlySchemaRepository schemaRepository, String storeName, boolean fastAvroEnabled) {
    this(
        id -> schemaRepository.getValueSchema(storeName, id).getSchema(),
        fastAvroEnabled
            ? FastSerializerDeserializerFactory::getFastAvroGenericDeserializer
            : SerializerDeserializerFactory::getAvroGenericDeserializer);
  }

  public StoreDeserializerCache(
      IntFunction<Schema> schemaGetter,
      BiFunction<Schema, Schema, RecordDeserializer<T>> deserializerGetter) {
    this((writerId, readerId) -> deserializerGetter.apply(schemaGetter.apply(writerId), schemaGetter.apply(readerId)));
  }

  public StoreDeserializerCache(
      ReadOnlySchemaRepository schemaRepository,
      String storeName,
      Class specificRecordClass) {
    this(
        id -> schemaRepository.getValueSchema(storeName, id).getSchema(),
        (writerSchema, readerSchema) -> FastSerializerDeserializerFactory
            .getFastAvroSpecificDeserializer(writerSchema, specificRecordClass));
  }

  public StoreDeserializerCache(BiIntFunction<RecordDeserializer<T>> deserializerGenerator) {
    this.deserializerGenerator = deserializerGenerator;
  }

  public RecordDeserializer<T> getDeserializer(int writerSchemaId, int readerSchemaId) {
    SparseConcurrentList<RecordDeserializer<T>> innerList =
        genericDeserializers.computeIfAbsent(writerSchemaId, SparseConcurrentList.SUPPLIER);
    RecordDeserializer<T> deserializer = innerList.get(readerSchemaId);
    if (deserializer == null) {
      deserializer = deserializerGenerator.apply(writerSchemaId, readerSchemaId);
      innerList.set(readerSchemaId, deserializer);
    }
    return deserializer;
  }
}
