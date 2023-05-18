package com.linkedin.avro.netty.decoder;

import com.linkedin.avro.netty.DataHelper;
import com.linkedin.avro.netty.Helper;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class DecoderBytesTest extends AbstractDecoderTest {
  @Test(dataProvider = "bytes", dataProviderClass = DataHelper.class)
  public void test_value(byte[] value) throws IOException {
    final GenericRecord record = Helper.genericBytes(value);
    this.test(record);
  }
}
