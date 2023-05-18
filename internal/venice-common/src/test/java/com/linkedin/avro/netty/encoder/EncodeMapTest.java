package com.linkedin.avro.netty.encoder;

import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import com.linkedin.avro.netty.Helper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class EncodeMapTest {
  @Test
  public void test_value() throws IOException {
    final Map<String, Long> value = new HashMap<String, Long>();
    value.put("aaaaa", Long.valueOf(1));
    value.put("bbbbb", Long.valueOf(2));
    value.put("ccccc", Long.valueOf(3));
    value.put("ddddd", Long.valueOf(4));
    final GenericRecord record = Helper.genericMap(value);
    assertArrayEquals(
        "should be equal " + record,
        Helper.avroGenericBinaryEncoder(record),
        Helper.avroGenericByteBufEncoder(record));
  }
}
