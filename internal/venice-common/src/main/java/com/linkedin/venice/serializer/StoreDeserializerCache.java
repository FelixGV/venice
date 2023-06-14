package com.linkedin.venice.serializer;

public interface StoreDeserializerCache<T> {
  RecordDeserializer<T> getDeserializer(int writerSchemaId, int readerSchemaId);
}
