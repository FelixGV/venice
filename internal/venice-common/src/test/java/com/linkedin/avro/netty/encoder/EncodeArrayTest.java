package com.linkedin.avro.netty.encoder;

import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import com.linkedin.avro.netty.Helper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class EncodeArrayTest {
  @Test
  public void test_value() throws IOException {
    final List<Long> value = new ArrayList<Long>();
    value.add(Long.valueOf(1));
    value.add(Long.valueOf(2));
    value.add(Long.valueOf(3));
    value.add(Long.valueOf(4));
    final GenericRecord record = Helper.genericList(value);
    assertArrayEquals(
        "should be equal " + record,
        Helper.avroGenericBinaryEncoder(record),
        Helper.avroGenericByteBufEncoder(record));
  }
}
