package com.linkedin.avro.netty.decoder;

import com.linkedin.avro.netty.Helper;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class DecoderBooleanTest extends AbstractDecoderTest {
  @Test
  public void test_positive_value() throws IOException {
    final GenericRecord original = Helper.genericBoolean(true);
    this.test(original);
  }

  @Test
  public void test_negative_value() throws IOException {
    final GenericRecord original = Helper.genericBoolean(false);
    this.test(original);
  }

}
