package com.linkedin.avro.netty.decoder;

import static org.testng.Assert.assertEquals;

import com.linkedin.avro.netty.Helper;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class DecodeMapTest extends AbstractDecoderTest {
  @Test
  public void test_value() throws IOException {
    final Map<String, Long> value = new HashMap<>();
    value.put("aaaaa", Long.valueOf(1));
    value.put("bbbbb", Long.valueOf(2));
    value.put("ccccc", Long.valueOf(3));
    value.put("ddddd", Long.valueOf(4));

    final GenericRecord originalValue = Helper.genericMap(value);
    final ByteBuf buffer = this.encode(originalValue);

    final Map<CharSequence, Long> result =
        (Map<CharSequence, Long>) Helper.avroGenericByteBufDecoder(buffer, originalValue.getSchema()).get("f_value");

    for (Map.Entry<CharSequence, Long> entry: result.entrySet()) {
      assertEquals(value.get(entry.getKey().toString()), entry.getValue().toString());
    }

  }
}
