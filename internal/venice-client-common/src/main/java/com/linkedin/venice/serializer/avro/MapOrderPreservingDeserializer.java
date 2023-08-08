package com.linkedin.venice.serializer.avro;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.VeniceSerializationException;
import com.linkedin.venice.utils.AvroSchemaUtils;
import com.linkedin.venice.utils.IndexedHashMap;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;


/**
 * {@code MapOrderPreservingDeserializer} is a deserializer which leverages Avro's object reuse in order to
 * inject a {@link IndexedHashMap} instance into all top-level map fields of a record, prior to deserialization.
 */
public class MapOrderPreservingDeserializer implements RecordDeserializer<GenericRecord> {
  private static boolean BUFFERED_AVRO_DECODER = true;
  private final RecordDeserializer<GenericRecord> delegate;
  private final Function<GenericRecord, GenericRecord> recordPrepper;
  private final Supplier<GenericRecord> recordSupplier;

  public MapOrderPreservingDeserializer(RecordDeserializer<GenericRecord> delegate, Schema readerSchema) {
    this.delegate = delegate;
    int numberOfMapFields = 0;
    int numberOfArrayFields = 0;
    for (Schema.Field field: readerSchema.getFields()) {
      if (AvroSchemaUtils.fieldConformsToType(field, Schema.Type.MAP)) {
        numberOfMapFields++;
      } else if (AvroSchemaUtils.fieldConformsToType(field, Schema.Type.ARRAY)) {
        numberOfArrayFields++;
      }
    }
    List<Schema.Field> mapFields = new ArrayList<>(numberOfMapFields);
    List<Schema.Field> arrayFields = new ArrayList<>(numberOfArrayFields);
    for (Schema.Field field: readerSchema.getFields()) {
      if (AvroSchemaUtils.fieldConformsToType(field, Schema.Type.MAP)) {
        mapFields.add(field);
      } else if (AvroSchemaUtils.fieldConformsToType(field, Schema.Type.ARRAY)) {
        arrayFields.add(field);
      }
    }
    this.recordPrepper = record -> {
      for (Schema.Field field: mapFields) {
        if (record.get(field.pos()) instanceof IndexedHashMap) {
          continue;
        }
        putMap(record, field);
      }
      for (Schema.Field field: arrayFields) {
        if (record.get(field.pos()) instanceof LinkedList) {
          continue;
        }
        putArray(record, field);
      }
      return record;
    };
    this.recordSupplier = () -> {
      GenericRecord record = new GenericData.Record(readerSchema);
      for (Schema.Field field: mapFields) {
        putMap(record, field);
      }
      for (Schema.Field field: arrayFields) {
        putArray(record, field);
      }
      return record;
    };
  }

  private static void putMap(GenericRecord record, Schema.Field field) {
    record.put(field.pos(), new IndexedHashMap<>());
  }

  private static void putArray(GenericRecord record, Schema.Field field) {
    record.put(field.pos(), new LinkedList<>());
  }

  @Override
  public GenericRecord deserialize(byte[] bytes) throws VeniceSerializationException {
    return this.delegate.deserialize(this.recordSupplier.get(), bytes);
  }

  @Override
  public GenericRecord deserialize(ByteBuffer byteBuffer) throws VeniceSerializationException {
    return this.delegate.deserialize(
        this.recordSupplier.get(),
        AvroCompatibilityHelper
            .newBinaryDecoder(byteBuffer.array(), byteBuffer.position(), byteBuffer.remaining(), null));
  }

  @Override
  public GenericRecord deserialize(GenericRecord reuse, ByteBuffer byteBuffer, BinaryDecoder reusedDecoder)
      throws VeniceSerializationException {
    return this.delegate.deserialize(this.recordPrepper.apply(reuse), byteBuffer, reusedDecoder);
  }

  @Override
  public GenericRecord deserialize(GenericRecord reuse, byte[] bytes) throws VeniceSerializationException {
    return this.delegate.deserialize(this.recordPrepper.apply(reuse), bytes);
  }

  @Override
  public GenericRecord deserialize(BinaryDecoder binaryDecoder) throws VeniceSerializationException {
    return this.delegate.deserialize(this.recordSupplier.get(), binaryDecoder);
  }

  @Override
  public GenericRecord deserialize(GenericRecord reuse, BinaryDecoder binaryDecoder)
      throws VeniceSerializationException {
    return this.delegate.deserialize(this.recordPrepper.apply(reuse), binaryDecoder);
  }

  @Override
  public GenericRecord deserialize(GenericRecord reuse, InputStream in, BinaryDecoder reusedDecoder)
      throws VeniceSerializationException {
    return this.delegate.deserialize(this.recordPrepper.apply(reuse), in, reusedDecoder);
  }

  @Override
  public Iterable<GenericRecord> deserializeObjects(byte[] bytes) throws VeniceSerializationException {
    InputStream in = new ByteArrayInputStream(bytes);
    BinaryDecoder decoder = AvroCompatibilityHelper.newBinaryDecoder(in, BUFFERED_AVRO_DECODER, null);
    return deserializeObjects(decoder);
  }

  @Override
  public Iterable<GenericRecord> deserializeObjects(BinaryDecoder binaryDecoder) throws VeniceSerializationException {
    List<GenericRecord> objects = new ArrayList();
    try {
      while (!binaryDecoder.isEnd()) {
        objects.add(deserialize(recordSupplier.get(), binaryDecoder));
      }
    } catch (Exception e) {
      throw new VeniceSerializationException("Could not deserialize bytes back into Avro objects", e);
    }

    return objects;
  }
}
