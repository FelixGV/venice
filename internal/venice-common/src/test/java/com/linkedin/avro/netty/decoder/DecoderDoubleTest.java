package com.linkedin.avro.netty.decoder;

import com.linkedin.avro.netty.DataHelper;
import com.linkedin.avro.netty.Helper;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class DecoderDoubleTest extends AbstractDecoderTest {
  @Test(dataProvider = "doubles", dataProviderClass = DataHelper.class)
  public void test_plus_value(double value) throws IOException {
    final GenericRecord originalValue = Helper.genericDouble(value);
    this.test(originalValue);
  }

  @Test(dataProvider = "doubles", dataProviderClass = DataHelper.class)
  public void test_minus_value(double value) throws IOException {
    final GenericRecord originalValue = Helper.genericDouble(-value);
    this.test(originalValue);
  }
}
