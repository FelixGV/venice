package com.linkedin.avro.netty.encoder;

import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import com.linkedin.avro.netty.Helper;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class EncoderBooleanTest {
  @Test
  public void test_plus_value() throws IOException {
    final GenericRecord record = Helper.genericBoolean(false);
    assertArrayEquals(
        "should be equal " + record,
        Helper.avroGenericBinaryEncoder(record),
        Helper.avroGenericByteBufEncoder(record));
  }

  @Test
  public void test_minus_value() throws IOException {
    final GenericRecord record = Helper.genericBoolean(false);
    assertArrayEquals(
        "should be equal -" + record,
        Helper.avroGenericBinaryEncoder(record),
        Helper.avroGenericByteBufEncoder(record));
  }
}
