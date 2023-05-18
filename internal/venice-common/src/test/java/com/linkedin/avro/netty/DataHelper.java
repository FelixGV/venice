package com.linkedin.avro.netty;

import org.testng.annotations.DataProvider;


public class DataHelper {
  @DataProvider(name = "integers")
  public static Object[][] integers() {
    return new Object[][] { { 2 }, { 6 }, { 19 }, { 22 }, { 23 }, { 236 }, { 2395 }, { 2346793 }, { 123546879 },
        { Integer.MAX_VALUE } };
  }

  @DataProvider(name = "longs")
  public static Object[][] longs() {
    return new Object[][] { { 2 }, { 6 }, { 19 }, { 22 }, { 23 }, { 236 }, { 2395 }, { 2346793 }, { 123546879 },
        { Integer.MAX_VALUE }, { Long.MAX_VALUE } };
  }

  @DataProvider(name = "floats")
  public static Object[][] floats() {
    return new Object[][] { { (float) 2.0 }, { (float) 6 }, { (float) 19 }, { (float) 22 }, { (float) 23 },
        { (float) 236 }, { (float) 2395 }, { (float) 2346793 }, { 123546879 }, { (float) Integer.MAX_VALUE },
        { (float) Float.MAX_VALUE }, { (float) 24342.234 }, { (float) 12.123 }, { (float) 743.324 } };
  }

  @DataProvider(name = "doubles")
  public static Object[][] doubles() {
    return new Object[][] { { (double) 2.0 }, { (double) 6 }, { (double) 19 }, { (double) 22 }, { (double) 23 },
        { (double) 236 }, { (double) 2395 }, { (double) 2346793 }, { 123546879 }, { (double) Integer.MAX_VALUE },
        { (double) Float.MAX_VALUE }, { (double) Double.MAX_VALUE }, { (double) 24342.234 }, { (double) 12.123 },
        { (double) 743.324 } };
  }

  @DataProvider(name = "strings")
  public static Object[][] strings() {
    return new Object[][] { { "Test 1" }, { "Test fdsaqwe qwe" }, { "Test циспсашпсад" },
        { "асдфлкјасдфл лкдфсј алкдјф " } };
  }

  @DataProvider(name = "bytes")
  public static Object[][] bytes() {
    return new Object[][] { { new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 } }, { new byte[] {} },
        { new byte[] { 1, 2, 3, 4, 5 } } };
  }
}
