package com.linkedin.avro.netty.decoder;

import com.linkedin.avro.netty.DataHelper;
import com.linkedin.avro.netty.Helper;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class DecoderFloatTest extends AbstractDecoderTest {
  @Test(dataProvider = "floats", dataProviderClass = DataHelper.class)
  public void test_plus_value(float value) throws IOException {
    final GenericRecord originalValue = Helper.genericFloat(value);
    this.test(originalValue);
  }

  @Test(dataProvider = "floats", dataProviderClass = DataHelper.class)
  public void test_minus_value(float value) throws IOException {
    final GenericRecord originalValue = Helper.genericFloat(-value);
    this.test(originalValue);
  }
}
