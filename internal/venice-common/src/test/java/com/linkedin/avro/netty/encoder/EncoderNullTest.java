package com.linkedin.avro.netty.encoder;

import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

import com.linkedin.avro.netty.Helper;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class EncoderNullTest {
  @DataProvider(name = "nulls")
  public static Object[][] numners() {
    return new Object[][] { { null }, { "bla" } };
  }

  @Test(dataProvider = "nulls")
  public void test_value(String value) throws IOException {
    final GenericRecord record = Helper.genericNull(value);
    assertArrayEquals(
        "should be equal " + record,
        Helper.avroGenericBinaryEncoder(record),
        Helper.avroGenericByteBufEncoder(record));
  }
}
