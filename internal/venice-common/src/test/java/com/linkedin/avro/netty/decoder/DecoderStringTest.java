package com.linkedin.avro.netty.decoder;

import static org.testng.Assert.assertEquals;

import com.linkedin.avro.netty.DataHelper;
import com.linkedin.avro.netty.Helper;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class DecoderStringTest extends AbstractDecoderTest {
  @Test(dataProvider = "strings", dataProviderClass = DataHelper.class)
  public void test_plus_value(String value) throws IOException {

    final GenericRecord originalValue = Helper.genericString(value);
    final ByteBuf buffer = this.encode(originalValue);

    assertEquals(
        originalValue.get("f_value"),
        Helper.avroGenericByteBufDecoder(buffer, originalValue.getSchema()).get("f_value").toString());
  }
}
