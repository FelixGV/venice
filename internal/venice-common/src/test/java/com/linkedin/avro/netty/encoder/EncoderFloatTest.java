package com.linkedin.avro.netty.encoder;

import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import com.linkedin.avro.netty.DataHelper;
import com.linkedin.avro.netty.Helper;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class EncoderFloatTest {
  @Test(dataProvider = "floats", dataProviderClass = DataHelper.class)
  public void test_plus_value(float value) throws IOException {
    final GenericRecord record = Helper.genericFloat(value);
    assertArrayEquals(
        "should be equal " + record,
        Helper.avroGenericBinaryEncoder(record),
        Helper.avroGenericByteBufEncoder(record));

  }

  @Test(dataProvider = "floats", dataProviderClass = DataHelper.class)
  public void test_minus_value(float value) throws IOException {
    final GenericRecord record = Helper.genericFloat(-value);
    assertArrayEquals(
        "should be equal -" + record,
        Helper.avroGenericBinaryEncoder(record),
        Helper.avroGenericByteBufEncoder(record));
  }
}
