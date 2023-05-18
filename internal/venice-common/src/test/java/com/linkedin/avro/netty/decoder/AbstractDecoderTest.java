package com.linkedin.avro.netty.decoder;

import static org.testng.Assert.assertEquals;

import com.linkedin.avro.netty.AbstractTest;
import com.linkedin.avro.netty.Helper;
import io.netty.buffer.ByteBuf;
import org.apache.avro.generic.GenericRecord;


public abstract class AbstractDecoderTest extends AbstractTest {
  public ByteBuf encode(final GenericRecord originalValue) {
    final byte[] blob = Helper.avroGenericBinaryEncoder(originalValue);

    return freeLaterBuffer(blob);
  }

  public void test(final GenericRecord originalValue, final GenericRecord decodedValue) {
    assertEquals(originalValue.get("f_value"), decodedValue.get("f_value"));
  }

  public void test(final GenericRecord originalValue, final ByteBuf buffer) {
    assertEquals(
        originalValue.get("f_value"),
        Helper.avroGenericByteBufDecoder(buffer, originalValue.getSchema()).get("f_value"));
  }

  public void test(final GenericRecord originalValue) {
    final ByteBuf buffer = this.encode(originalValue);
    assertEquals(
        originalValue.get("f_value"),
        Helper.avroGenericByteBufDecoder(buffer, originalValue.getSchema()).get("f_value"));
  }

}
