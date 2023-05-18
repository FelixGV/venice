package com.linkedin.avro.netty.decoder;

import com.linkedin.avro.netty.DataHelper;
import com.linkedin.avro.netty.Helper;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class DecoderIntTest extends AbstractDecoderTest {
  @Test(dataProvider = "integers", dataProviderClass = DataHelper.class)
  public void test_plus_value(int value) throws IOException {
    final GenericRecord originalValue = Helper.genericInt(value);
    this.test(originalValue);
  }

  @Test(dataProvider = "integers", dataProviderClass = DataHelper.class)
  public void test_minus_value(int value) throws IOException {
    final GenericRecord originalValue = Helper.genericInt(-value);
    this.test(originalValue);
  }
}
