package com.linkedin.avro.netty.decoder;

import static org.testng.Assert.assertEquals;

import com.linkedin.avro.netty.Helper;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class DecodeEnumTest extends AbstractDecoderTest {
  @Test
  public void test_value() throws IOException {
    final GenericRecord originalValue = Helper.genericEnum("DIAMONDS");
    final ByteBuf buffer = this.encode(originalValue);
    assertEquals(
        originalValue.get("f_value").toString(),
        Helper.avroGenericByteBufDecoder(buffer, originalValue.getSchema()).get("f_value").toString());
  }
}
