package com.linkedin.venice.stats;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.linkedin.venice.utils.DoubleAndBooleanConsumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import org.testng.annotations.Test;


public class StatsUtilsTest {
  @Test
  public void testConsumeFunctions() {
    IntConsumer intConsumer = mock(IntConsumer.class);
    StatsUtils.consumeIntIfAbove(intConsumer, 1, 0);
    verify(intConsumer).accept(1);

    intConsumer = mock(IntConsumer.class);
    StatsUtils.consumeIntIfAbove(intConsumer, 0, 0);
    verify(intConsumer, never()).accept(0);

    DoubleConsumer doubleConsumer = mock(DoubleConsumer.class);
    StatsUtils.consumeDoubleIfAbove(doubleConsumer, 1.0, 0.0);
    verify(doubleConsumer).accept(1.0);

    doubleConsumer = mock(DoubleConsumer.class);
    StatsUtils.consumeDoubleIfAbove(doubleConsumer, 0, 0);
    verify(doubleConsumer, never()).accept(0);

    DoubleAndBooleanConsumer doubleAndBooleanConsumer = mock(DoubleAndBooleanConsumer.class);
    StatsUtils.consumeDoubleAndBooleanIfAbove(doubleAndBooleanConsumer, 1.0, true, 0.0);
    verify(doubleAndBooleanConsumer).accept(1.0, true);

    doubleAndBooleanConsumer = mock(DoubleAndBooleanConsumer.class);
    StatsUtils.consumeDoubleAndBooleanIfAbove(doubleAndBooleanConsumer, 0.0, true, 0.0);
    verify(doubleAndBooleanConsumer, never()).accept(1.0, true);
  }
}
