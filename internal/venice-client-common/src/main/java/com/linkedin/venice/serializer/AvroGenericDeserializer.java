package com.linkedin.venice.serializer;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;


public class AvroGenericDeserializer<V> implements RecordDeserializer<V> {
  private static boolean BUFFERED_AVRO_DECODER = true;

  /**
   * Legacy config to tune the implementation for deserializing a sequence of records. Kept for now just in case
   * it is still wired in by any application. TODO: Delete completely after auditing usage.
   */
  @Deprecated
  public enum IterableImpl {
    /** Only supported mode. */
    BLOCKING,

    @Deprecated
    LAZY,

    @Deprecated
    LAZY_WITH_REPLAY_SUPPORT;
  }

  private final DatumReader<V> datumReader;

  public AvroGenericDeserializer(Schema writer, Schema reader) {
    this(new GenericDatumReader<>(writer, reader));
  }

  protected AvroGenericDeserializer(DatumReader<V> datumReader) {
    this.datumReader = datumReader;
  }

  @Override
  public V deserialize(byte[] bytes) throws VeniceSerializationException {
    return deserialize(null, bytes);
  }

  @Override
  public V deserialize(ByteBuffer byteBuffer) throws VeniceSerializationException {
    return deserialize(null, byteBuffer, null);
  }

  @Override
  public V deserialize(V reuse, ByteBuffer byteBuffer, BinaryDecoder reusedDecoder)
      throws VeniceSerializationException {
    BinaryDecoder decoder = AvroCompatibilityHelper
        .newBinaryDecoder(byteBuffer.array(), byteBuffer.position(), byteBuffer.remaining(), reusedDecoder);
    return deserialize(reuse, decoder);
  }

  @Override
  public V deserialize(V reuseRecord, byte[] bytes) throws VeniceSerializationException {
    InputStream in = new ByteArrayInputStream(bytes);
    BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(in, BUFFERED_AVRO_DECODER, null);
    return deserialize(reuseRecord, decoder);
  }

  @Override
  public V deserialize(Decoder decoder) throws VeniceSerializationException {
    return deserialize(null, decoder);
  }

  @Override
  public V deserialize(V reuseRecord, Decoder decoder) throws VeniceSerializationException {
    try {
      return datumReader.read(reuseRecord, decoder);
    } catch (Exception e) {
      throw new VeniceSerializationException("Could not deserialize bytes back into Avro object", e);
    }
  }

  @Override
  public V deserialize(V reuseRecord, InputStream in, BinaryDecoder reusedDecoder) throws VeniceSerializationException {
    BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(in, BUFFERED_AVRO_DECODER, reusedDecoder);
    return deserialize(reuseRecord, decoder);
  }

  @Override
  public Iterable<V> deserializeObjects(Decoder decoder, DecoderStatus decoderStatus)
      throws VeniceSerializationException {
    List<V> objects = new ArrayList();
    try {
      while (!decoderStatus.isDone()) {
        objects.add(datumReader.read(null, decoder));
      }
    } catch (Exception e) {
      throw new VeniceSerializationException("Could not deserialize bytes back into Avro objects", e);
    }

    return objects;
  }
}
