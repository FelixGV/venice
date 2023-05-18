package com.linkedin.avro.netty.decoder;

import com.linkedin.avro.netty.DataHelper;
import com.linkedin.avro.netty.Helper;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class DecoderLongTest extends AbstractDecoderTest {
  @Test(dataProvider = "longs", dataProviderClass = DataHelper.class)
  public void test_plus_value(long value) throws IOException {
    final GenericRecord originalValue = Helper.genericLong(value);
    this.test(originalValue);
  }

  @Test(dataProvider = "longs", dataProviderClass = DataHelper.class)
  public void test_minus_value(long value) throws IOException {
    final GenericRecord originalValue = Helper.genericLong(-value);
    this.test(originalValue);
  }
}
