package com.linkedin.avro.netty.decoder;

import static org.testng.Assert.assertEquals;

import com.linkedin.avro.netty.Helper;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class DecodeCompositeTest extends AbstractDecoderTest {
  @Test
  public void test_record() throws IOException {
    final GenericRecord originalValue = Helper.defaultGeneric();
    final ByteBuf buffer = this.encode(originalValue);
    final GenericRecord decodedValue = Helper.avroGenericByteBufDecoder(buffer, originalValue.getSchema());

    assertEquals(originalValue.get("f_string"), decodedValue.get("f_string").toString());
    assertEquals(originalValue.get("f_int"), decodedValue.get("f_int"));
    assertEquals(originalValue.get("f_long"), decodedValue.get("f_long"));
    assertEquals(originalValue.get("f_float"), decodedValue.get("f_float"));
    assertEquals(originalValue.get("f_double"), decodedValue.get("f_double"));
    assertEquals(originalValue.get("f_boolean"), decodedValue.get("f_boolean"));
    assertEquals(originalValue.get("f_bytes"), decodedValue.get("f_bytes"));
  }
}
