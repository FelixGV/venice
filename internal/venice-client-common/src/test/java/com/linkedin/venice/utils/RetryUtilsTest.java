package com.linkedin.venice.utils;

import static org.mockito.BDDMockito.doThrow;
import static org.mockito.BDDMockito.mock;
import static org.mockito.BDDMockito.reset;
import static org.mockito.BDDMockito.times;
import static org.mockito.BDDMockito.verify;
import static org.mockito.BDDMockito.when;

import com.linkedin.venice.utils.locks.AutoCloseableLock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RetryUtilsTest {
  /**
   * This internal class is used as a mock to simulate behaviors of throwing exceptions.
   */
  private interface SomeObj {
    void doSomething();

    void doSomethingMightThrowCheckedException() throws CustomizedCheckedException;

    int getAnInteger();

    int getAnIntegerMightThrowCheckedException() throws CustomizedCheckedException;
  }

  private static class CustomizedRuntimeException extends RuntimeException {
  }

  private static class CustomizedCheckedException extends Exception {
  }

  @Test
  public void testFixedDelayRetryOnRunnable() throws CustomizedCheckedException {
    SomeObj obj = mock(SomeObj.class);

    // Case 1: no failure
    RetryUtils.executeWithMaxAttempt(
        obj::doSomething,
        3,
        Duration.ofMillis(1),
        Collections.singletonList(IllegalStateException.class));
    verify(obj, times(1)).doSomething();
    reset(obj);

    // Case 2: succeed on the last attempt
    doThrow(IllegalArgumentException.class).doThrow(IllegalStateException.class).doNothing().when(obj).doSomething();

    RetryUtils.executeWithMaxAttempt(
        obj::doSomething,
        3,
        Duration.ofMillis(1),
        Arrays.asList(IllegalStateException.class, IllegalArgumentException.class));

    verify(obj, times(3)).doSomething();
    reset(obj);

    // Case 3: fail through all attempts
    doThrow(IllegalArgumentException.class).doThrow(IllegalStateException.class)
        .doThrow(CustomizedRuntimeException.class) // This exception should be reported since it is the last one
        .when(obj)
        .doSomething();

    Exception expectedException = null;
    try {
      RetryUtils.executeWithMaxAttempt(
          obj::doSomething,
          3,
          Duration.ofMillis(1),
          Arrays.asList(IllegalStateException.class, IllegalArgumentException.class, CustomizedRuntimeException.class));
    } catch (Exception e) {
      expectedException = e;
    }
    Assert.assertNotNull(expectedException);
    Assert.assertTrue(expectedException instanceof CustomizedRuntimeException);
    verify(obj, times(3)).doSomething();
    reset(obj);

    // Case 4: throw unhandled exception
    doThrow(IllegalArgumentException.class).doThrow(CustomizedRuntimeException.class) // This exception should be
                                                                                      // reported since it is not
                                                                                      // handled
        .when(obj)
        .doSomething();

    expectedException = null;
    try {
      RetryUtils.executeWithMaxAttempt(
          obj::doSomething,
          3,
          Duration.ofMillis(1),
          Collections.singletonList(IllegalArgumentException.class));
    } catch (Exception e) {
      expectedException = e;
    }
    Assert.assertNotNull(expectedException);
    Assert.assertTrue(expectedException instanceof CustomizedRuntimeException);
    verify(obj, times(2)).doSomething();
    reset(obj);

    // Case 5: Handle checked exception and eventually succeed
    doThrow(IllegalArgumentException.class).doThrow(CustomizedCheckedException.class)
        .doNothing()
        .when(obj)
        .doSomethingMightThrowCheckedException();

    RetryUtils.executeWithMaxAttempt(
        obj::doSomethingMightThrowCheckedException,
        3,
        Duration.ofMillis(1),
        Arrays.asList(CustomizedCheckedException.class, IllegalArgumentException.class));

    verify(obj, times(3)).doSomethingMightThrowCheckedException();
    reset(obj);
  }

  @Test
  public void testFixedDelayRetryOnSupplier() throws CustomizedCheckedException {
    SomeObj obj = mock(SomeObj.class);

    // Case 1: no failure
    when(obj.getAnInteger()).thenReturn(1);
    Assert.assertEquals(
        (int) RetryUtils.executeWithMaxAttempt(
            () -> obj.getAnInteger() + 1,
            3,
            Duration.ofMillis(1),
            Collections.singletonList(IllegalStateException.class)),
        2);
    verify(obj, times(1)).getAnInteger();
    reset(obj);

    // Case 2: succeed on the last attempt
    when(obj.getAnInteger()).thenThrow(IllegalArgumentException.class)
        .thenThrow(IllegalStateException.class)
        .thenReturn(2);

    Assert.assertEquals(
        (int) RetryUtils.executeWithMaxAttempt(
            () -> obj.getAnInteger() + 1,
            3,
            Duration.ofMillis(1),
            Arrays.asList(IllegalStateException.class, IllegalArgumentException.class)),
        3);
    verify(obj, times(3)).getAnInteger();
    reset(obj);

    // Case 3: fail through all attempts
    when(obj.getAnInteger()).thenThrow(IllegalArgumentException.class)
        .thenThrow(IllegalStateException.class)
        .thenThrow(CustomizedRuntimeException.class);

    Exception expectedException = null;
    try {
      RetryUtils.executeWithMaxAttempt(
          () -> obj.getAnInteger() + 1,
          3,
          Duration.ofMillis(1),
          Arrays.asList(IllegalStateException.class, IllegalArgumentException.class, CustomizedRuntimeException.class));
    } catch (Exception e) {
      expectedException = e;
    }
    Assert.assertNotNull(expectedException);
    Assert.assertTrue(expectedException instanceof CustomizedRuntimeException);
    verify(obj, times(3)).getAnInteger();
    reset(obj);

    // Case 4: throw unhandled exception
    when(obj.getAnInteger()).thenThrow(IllegalArgumentException.class).thenThrow(CustomizedRuntimeException.class);

    expectedException = null;
    try {
      RetryUtils.executeWithMaxAttempt(
          () -> obj.getAnInteger() + 1,
          3,
          Duration.ofMillis(1),
          Collections.singletonList(IllegalArgumentException.class));
    } catch (Exception e) {
      expectedException = e;
    }
    Assert.assertNotNull(expectedException);
    Assert.assertTrue(expectedException instanceof CustomizedRuntimeException);
    verify(obj, times(2)).getAnInteger();
    reset(obj);

    // Case 5: Handle checked exception and eventually succeed
    when(obj.getAnIntegerMightThrowCheckedException()).thenThrow(IllegalArgumentException.class)
        .thenThrow(CustomizedCheckedException.class)
        .thenReturn(1);

    Assert.assertEquals(
        (int) RetryUtils.executeWithMaxAttempt(
            () -> obj.getAnIntegerMightThrowCheckedException() + 1,
            3,
            Duration.ofMillis(1),
            Arrays.asList(IllegalArgumentException.class, CustomizedCheckedException.class)),
        2);
    verify(obj, times(3)).getAnIntegerMightThrowCheckedException();
    reset(obj);
  }

  @Test
  public void testExponentialBackoffDelayRetryOnRunnable() {
    SomeObj obj = mock(SomeObj.class);

    // Case 1: no failure
    RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
        obj::doSomething,
        3,
        Duration.ofMillis(2),
        Duration.ofMillis(5),
        Duration.ofMillis(100),
        Collections.singletonList(IllegalStateException.class));
    verify(obj, times(1)).doSomething();
    reset(obj);

    // Case 2: succeed on the last attempt
    doThrow(IllegalArgumentException.class).doThrow(IllegalStateException.class).doNothing().when(obj).doSomething();
    RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
        obj::doSomething,
        3,
        Duration.ofMillis(2),
        Duration.ofMillis(5),
        Duration.ofMillis(100),
        Arrays.asList(IllegalArgumentException.class, IllegalStateException.class));
    verify(obj, times(3)).doSomething();
    reset(obj);
  }

  @Test
  public void testExponentialBackoffDelayRetryOnSupplier() {
    SomeObj obj = mock(SomeObj.class);

    // Case 1: no failure
    when(obj.getAnInteger()).thenReturn(1);
    Assert.assertEquals(
        (int) RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
            () -> obj.getAnInteger() + 1,
            3,
            Duration.ofMillis(2),
            Duration.ofMillis(5),
            Duration.ofMillis(100),
            Collections.singletonList(IllegalStateException.class)),
        2);
    verify(obj, times(1)).getAnInteger();
    reset(obj);

    // Case 2: succeed on the last attempt
    when(obj.getAnInteger()).thenThrow(IllegalArgumentException.class)
        .thenThrow(IllegalStateException.class)
        .thenReturn(2);

    Assert.assertEquals(
        (int) RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
            () -> obj.getAnInteger() + 1,
            3,
            Duration.ofMillis(2),
            Duration.ofMillis(5),
            Duration.ofMillis(100),
            Arrays.asList(IllegalStateException.class, IllegalArgumentException.class)),
        3);
    verify(obj, times(3)).getAnInteger();
    reset(obj);
  }

  @Test
  public void testFixAttemptDurationOnSupplier() {
    SomeObj obj = mock(SomeObj.class);

    // Case 1: no failure
    when(obj.getAnInteger()).thenReturn(1);
    Assert.assertEquals(
        (int) RetryUtils.executeWithMaxRetriesAndFixedAttemptDuration(
            () -> obj.getAnInteger() + 1,
            3,
            Duration.ofMillis(100),
            Collections.singletonList(IllegalStateException.class)),
        2);
    verify(obj, times(1)).getAnInteger();
    reset(obj);

    // Case 2: succeed on the last attempt
    when(obj.getAnInteger()).thenThrow(IllegalArgumentException.class)
        .thenThrow(IllegalStateException.class)
        .thenReturn(2);
    long startTime = System.currentTimeMillis();
    Assert.assertEquals((int) RetryUtils.executeWithMaxRetriesAndFixedAttemptDuration(() -> {
      // Give the action some non-trivial time to make sure no precision error.
      Utils.sleep(100);
      return obj.getAnInteger() + 1;
    }, 2, Duration.ofMillis(1000), Arrays.asList(IllegalStateException.class, IllegalArgumentException.class)), 3);
    long timeSpentInMs = (System.currentTimeMillis() - startTime);
    Assert.assertTrue(timeSpentInMs > 2000, "Time spent in attempts " + timeSpentInMs + "ms");
    verify(obj, times(3)).getAnInteger();
    reset(obj);
  }

  @Test
  public void testLockingBehavior() throws ExecutionException, InterruptedException {
    Lock lock = new ReentrantLock();
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    AtomicInteger attemptCounter1 = new AtomicInteger(0);
    AtomicInteger attemptCounter2 = new AtomicInteger(0);
    final int ATTEMPT_COUNT = 5;
    BiFunction<Integer, AtomicInteger, Runnable> runnableSupplier = (threadNumber, attemptCounter) -> () -> {
      try (AutoCloseableLock ignored = AutoCloseableLock.of(lock)) {
        RetryUtils.executeWithMaxAttempt(() -> {
          int attempt = attemptCounter.incrementAndGet();
          if (attempt < ATTEMPT_COUNT) {
            throw new RuntimeException("Thread number " + threadNumber + ", attempt " + attempt + ".");
          } else {
            System.out.println("Thread number " + threadNumber + " succeeded!");
          }
        }, ATTEMPT_COUNT, Duration.ofMillis(1000), List.of(RuntimeException.class));
      }
    };
    Runnable runnable1 = runnableSupplier.apply(1, attemptCounter1);
    Runnable runnable2 = runnableSupplier.apply(2, attemptCounter2);
    long startTime = System.currentTimeMillis();
    Future future1 = executorService.submit(runnable1);
    Future future2 = executorService.submit(runnable2);
    future1.get();
    future2.get();
    long elapsedTime = System.currentTimeMillis() - startTime;
    System.out.println("Elapsed time: " + elapsedTime + " ms.");
  }

  @Test
  public void testBetterLockingBehavior() throws ExecutionException, InterruptedException {
    Lock lock = new ReentrantLock();
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    AtomicInteger attemptCounter1 = new AtomicInteger(0);
    AtomicInteger attemptCounter2 = new AtomicInteger(0);
    final int ATTEMPT_COUNT = 5;
    BiFunction<Integer, AtomicInteger, Runnable> runnableSupplier = (threadNumber, attemptCounter) -> () -> {
      RetryUtils.executeWithMaxAttempt(() -> {
        try (AutoCloseableLock ignored = AutoCloseableLock.of(lock)) {
          int attempt = attemptCounter.incrementAndGet();
          if (attempt < ATTEMPT_COUNT) {
            throw new RuntimeException("Thread number " + threadNumber + ", attempt " + attempt + ".");
          } else {
            System.out.println("Thread number " + threadNumber + " succeeded!");
          }
        }
      }, ATTEMPT_COUNT, Duration.ofMillis(1000), List.of(RuntimeException.class));
    };
    Runnable runnable1 = runnableSupplier.apply(1, attemptCounter1);
    Runnable runnable2 = runnableSupplier.apply(2, attemptCounter2);
    long startTime = System.currentTimeMillis();
    Future future1 = executorService.submit(runnable1);
    Future future2 = executorService.submit(runnable2);
    future1.get();
    future2.get();
    long elapsedTime = System.currentTimeMillis() - startTime;
    System.out.println("Elapsed time: " + elapsedTime + " ms.");
  }

}
