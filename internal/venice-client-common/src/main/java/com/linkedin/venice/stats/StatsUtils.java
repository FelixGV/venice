package com.linkedin.venice.stats;

import com.linkedin.venice.utils.DoubleAndBooleanConsumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;


public class StatsUtils {
  public static String convertHostnameToMetricName(String hostName) {
    return hostName.replace('.', '_');
  }

  public static void consumeIntIfAbove(IntConsumer consumer, int value, int threshold) {
    if (value > threshold) {
      consumer.accept(value);
    }
  }

  public static void consumeDoubleIfAbove(DoubleConsumer consumer, double value, double threshold) {
    if (value > threshold) {
      consumer.accept(value);
    }
  }

  public static void consumeDoubleAndBooleanIfAbove(
      DoubleAndBooleanConsumer consumer,
      double value,
      boolean b,
      double threshold) {
    if (value > threshold) {
      consumer.accept(value, b);
    }
  }
}
