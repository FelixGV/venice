package com.linkedin.avro.netty;

import com.linkedin.venice.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;


public class Helper {
  public static byte[] avroGenericBinaryEncoder(final GenericRecord record) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    final DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(record.getSchema());

    try {
      writer.write(record, encoder);
      encoder.flush();
      out.close();
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
    return out.toByteArray();
  }

  public static byte[] avroGenericByteBufEncoder(final GenericRecord record) {
    final ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer(1024);
    final ByteBufEncoder encoder = new ByteBufEncoder();
    encoder.setBuffer(buffer);
    final DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
    try {
      writer.write(record, encoder);
      return getBytes(encoder.getBuffer());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    } finally {
      buffer.release();
    }
  }

  public static GenericRecord avroGenericByteBufDecoder(final ByteBuf buffer, final Schema schema) {
    final ByteBufDecoder decoder = new ByteBufDecoder();
    decoder.setBuffer(buffer);
    final GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema, schema);
    try {
      return reader.read(null, decoder);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static GenericRecord defaultGeneric() throws IOException {
    final GenericRecord record = getGenericRecord("E_COMPOSITE");
    record.put("f_string", "this is a string");
    record.put("f_int", 12456);
    record.put("f_long", (long) 3);
    record.put("f_float", (float) 12.0);
    record.put("f_double", 25668.0);
    record.put("f_boolean", true);
    record.put("f_bytes", ByteBuffer.wrap(new byte[] { 2, 3, 4, 5, 6, 7, 8 }));

    return record;
  }

  public static GenericRecord genericInt(final int value) throws IOException {
    final GenericRecord record = getGenericRecord("E_INT");
    record.put("f_value", value);
    return record;
  }

  public static GenericRecord genericLong(final long value) throws IOException {
    final GenericRecord record = getGenericRecord("E_LONG");
    record.put("f_value", value);
    return record;
  }

  public static GenericRecord genericFloat(final float value) throws IOException {
    final GenericRecord record = getGenericRecord("E_FLOAT");
    record.put("f_value", value);
    return record;
  }

  public static GenericRecord genericDouble(final double value) throws IOException {
    final GenericRecord record = getGenericRecord("E_DOUBLE");
    record.put("f_value", value);
    return record;
  }

  public static GenericRecord genericBoolean(final boolean value) throws IOException {
    final GenericRecord record = getGenericRecord("E_BOOLEAN");
    record.put("f_value", value);
    return record;
  }

  public static GenericRecord genericBytes(final byte[] value) throws IOException {
    final GenericRecord record = getGenericRecord("E_BYTES");
    record.put("f_value", ByteBuffer.wrap(value));
    return record;
  }

  public static GenericRecord genericString(final String value) throws IOException {
    final GenericRecord record = getGenericRecord("E_STRING");
    record.put("f_value", value);
    return record;
  }

  public static GenericRecord genericNull(final String value) throws IOException {
    final GenericRecord record = getGenericRecord("E_NULL");
    record.put("f_value", value);
    return record;
  }

  public static GenericRecord genericMap(final Map<String, Long> value) throws IOException {
    final GenericRecord record = getGenericRecord("E_MAP");
    record.put("f_value", value);
    return record;
  }

  public static GenericRecord genericList(final List<Long> value) throws IOException {
    final GenericRecord record = getGenericRecord("E_ARRAY");
    record.put("f_value", value);
    return record;
  }

  public static GenericRecord genericEnum(final String value) throws IOException {
    final GenericRecord record = getGenericRecord("E_ENUM");
    record.put("f_value", GenericData.get().createEnum(value, getSchema("CARDS_ENUM")));
    return record;
  }

  public static byte[] getBytes(final ByteBuf buffer) {
    if (buffer.isDirect()) {
      final byte[] blob = new byte[buffer.readableBytes()];
      buffer.getBytes(0, blob);
      return blob;
    } else {
      return buffer.array();
    }
  }

  private static GenericRecord getGenericRecord(String schemaFileName) throws IOException {
    return new GenericData.Record(getSchema(schemaFileName));
  }

  private static Schema getSchema(String schemaFileName) throws IOException {
    return Utils.getSchemaFromResource("avro/AvroNetty/" + schemaFileName + ".avsc");
  }
}
