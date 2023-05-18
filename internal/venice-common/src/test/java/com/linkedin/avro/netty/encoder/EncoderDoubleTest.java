package com.linkedin.avro.netty.encoder;

import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import com.linkedin.avro.netty.DataHelper;
import com.linkedin.avro.netty.Helper;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class EncoderDoubleTest {
  @Test(dataProvider = "doubles", dataProviderClass = DataHelper.class)
  public void test_plus_value(double value) throws IOException {
    final GenericRecord record = Helper.genericDouble(value);
    assertArrayEquals(
        "should be equal " + record,
        Helper.avroGenericBinaryEncoder(record),
        Helper.avroGenericByteBufEncoder(record));
  }

  @Test(dataProvider = "doubles", dataProviderClass = DataHelper.class)
  public void test_minus_value(double value) throws IOException {
    final GenericRecord record = Helper.genericDouble(-value);
    assertArrayEquals(
        "should be equal -" + record,
        Helper.avroGenericBinaryEncoder(record),
        Helper.avroGenericByteBufEncoder(record));
  }

}
