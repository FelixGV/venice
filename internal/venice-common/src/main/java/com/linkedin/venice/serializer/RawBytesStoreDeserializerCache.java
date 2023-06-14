package com.linkedin.venice.serializer;

import com.linkedin.venice.serialization.IdentityRecordDeserializer;
import java.nio.ByteBuffer;


public class RawBytesStoreDeserializerCache implements StoreDeserializerCache<ByteBuffer> {
  private static final StoreDeserializerCache INSTANCE = new RawBytesStoreDeserializerCache();

  @Override
  public RecordDeserializer<ByteBuffer> getDeserializer(int writerSchemaId, int readerSchemaId) {
    return IdentityRecordDeserializer.getInstance();
  }

  public static StoreDeserializerCache<ByteBuffer> getInstance() {
    return INSTANCE;
  }
}
