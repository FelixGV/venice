package com.linkedin.venice.serializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;


public interface RecordDeserializer<T> {
  T deserialize(byte[] bytes) throws VeniceSerializationException;

  T deserialize(ByteBuffer byteBuffer) throws VeniceSerializationException;

  T deserialize(T reuse, ByteBuffer byteBuffer, BinaryDecoder reusedDecoder) throws VeniceSerializationException;

  T deserialize(T reuse, byte[] bytes) throws VeniceSerializationException;

  T deserialize(Decoder decoder) throws VeniceSerializationException;

  T deserialize(T reuse, Decoder decoder) throws VeniceSerializationException;

  T deserialize(T reuse, InputStream in, BinaryDecoder reusedDecoder) throws VeniceSerializationException;

  Iterable<T> deserializeObjects(Decoder decoder, DecoderStatus decoderStatus) throws VeniceSerializationException;

  interface DecoderStatus {
    boolean isDone() throws IOException;
  }
}
