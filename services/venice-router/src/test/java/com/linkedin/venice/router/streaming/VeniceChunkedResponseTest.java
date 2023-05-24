package com.linkedin.venice.router.streaming;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.stats.RouterStats;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class VeniceChunkedResponseTest {
  // @Test
  // public void whetherToSkipMessage() {
  // VeniceChunkedWriteHandler chunkedWriteHandler = new VeniceChunkedWriteHandler();
  // /** The {@link VeniceChunkedResponse} is only instantiated so that it registers its callback into the handler */
  // new VeniceChunkedResponse(
  // "storeName",
  // RequestType.MULTI_GET_STREAMING,
  // mock(ChannelHandlerContext.class),
  // chunkedWriteHandler,
  // mock(RouterStats.class));
  // assertThrows(
  // // This used to throw a NPE as part of a previous regression, we want a better error message
  // VeniceException.class,
  // () -> chunkedWriteHandler.write(mock(ChannelHandlerContext.class), null, mock(ChannelPromise.class)));
  // }
  @DataProvider(name = "raceConditionParams")
  public static Object[][] trueAndFalseProvider() {
    return new Object[][] { { false, 2, 1 }, { false, 4, 1 }, { false, 8, 1 }, { false, 16, 1 }, { false, 32, 1 },
        { false, 64, 1 }, { false, 128, 1 }, { false, 256, 1 }, { true, 2, 1 }, { true, 4, 1 }, { true, 8, 1 },
        { true, 16, 1 }, { true, 32, 1 }, { true, 64, 1 }, { true, 128, 1 }, { true, 256, 1 }, { false, 2, 2 },
        { false, 4, 2 }, { false, 8, 2 }, { false, 16, 2 }, { false, 32, 2 }, { false, 64, 2 }, { false, 128, 2 },
        { false, 256, 2 }, { true, 2, 2 }, { true, 4, 2 }, { true, 8, 2 }, { true, 16, 2 }, { true, 32, 2 },
        { true, 64, 2 }, { true, 128, 2 }, { true, 256, 2 }, { false, 4, 4 }, { false, 8, 4 }, { false, 16, 4 },
        { false, 32, 4 }, { false, 64, 4 }, { false, 128, 4 }, { false, 256, 4 }, { true, 4, 4 }, { true, 8, 4 },
        { true, 16, 4 }, { true, 32, 4 }, { true, 64, 4 }, { true, 128, 4 }, { true, 256, 4 }, { false, 8, 8 },
        { false, 16, 8 }, { false, 32, 8 }, { false, 64, 8 }, { false, 128, 8 }, { false, 256, 8 }, { true, 8, 8 },
        { true, 16, 8 }, { true, 32, 8 }, { true, 64, 8 }, { true, 128, 8 }, { true, 256, 8 } };
  }

  @Test(invocationCount = 10, dataProvider = "raceConditionParams")
  public void test(boolean withExternalSynchronization, int numberOfParallelWriteInvocations, int parallelExecutors)
      throws ExecutionException, InterruptedException {
    VeniceChunkedWriteHandler chunkedWriteHandler = new VeniceChunkedWriteHandler();
    ChannelHandlerContext channelHandlerContext = mock(ChannelHandlerContext.class);
    when(channelHandlerContext.newPromise()).thenReturn(mock(ChannelPromise.class));
    when(channelHandlerContext.channel()).thenReturn(mock(Channel.class));
    when(channelHandlerContext.newProgressivePromise()).thenReturn(mock(ChannelProgressivePromise.class));
    VeniceChunkedResponse response = new VeniceChunkedResponse(
        "storeName",
        RequestType.MULTI_GET_STREAMING,
        channelHandlerContext,
        chunkedWriteHandler,
        mock(RouterStats.class));

    Runnable[] runnables = new Runnable[numberOfParallelWriteInvocations];
    AtomicInteger failures = new AtomicInteger(0);
    AtomicInteger nullPointers = new AtomicInteger(0);
    AtomicInteger successes = new AtomicInteger(0);
    for (int i = 0; i < numberOfParallelWriteInvocations; i++) {
      runnables[i] = () -> {
        try {
          response.write(VeniceChunkedResponse.EMPTY_BYTE_BUF, CompressionStrategy.NO_OP);
          successes.incrementAndGet();
        } catch (NullPointerException e) {
          nullPointers.incrementAndGet();
        } catch (Exception e) {
          failures.incrementAndGet();
        }
      };
    }

    ExecutorService executor = Executors.newFixedThreadPool(parallelExecutors);
    Runnable[] metaRunnables = new Runnable[parallelExecutors];
    int numberOfTasksPerExecutor = numberOfParallelWriteInvocations / parallelExecutors;
    for (int i = 0; i < parallelExecutors; i++) {
      ExecutorService innerExecutor = Executors.newFixedThreadPool(numberOfTasksPerExecutor);
      int firstIndex = i;
      metaRunnables[i] = () -> {
        if (withExternalSynchronization) {
          synchronized (response) {
            for (int j = firstIndex; j < numberOfParallelWriteInvocations; j += parallelExecutors) {
              innerExecutor.submit(runnables[j]);
            }
          }
        } else {
          for (int j = firstIndex; j < numberOfParallelWriteInvocations; j += parallelExecutors) {
            innerExecutor.submit(runnables[j]);
          }
        }
        innerExecutor.shutdown();
        try {
          innerExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      };
    }

    int busyWorkThreads = 8;
    ExecutorService busyWorkExecutor = Executors.newFixedThreadPool(busyWorkThreads);
    for (int i = 0; i < busyWorkThreads; i++) {
      int threadNumber = i;
      // busywork, just to increase the chance of CPU context switching...
      busyWorkExecutor.submit(() -> {
        float computation = 0.1f;
        for (int j = 0; j < 10_000_000; j++) {
          computation = computation * 1.1f;
          if (computation == Float.POSITIVE_INFINITY) {
            computation = 0.1f;
          }
        }
        System.out.println("Done with busy work thread " + threadNumber + ": " + computation);
      });
    }

    for (int i = 0; i < parallelExecutors; i++) {
      executor.submit(metaRunnables[i]);
    }

    // if (withExternalSynchronization) {
    // synchronized (response) {
    // for (int i = 0; i < numberOfParallelWriteInvocations; i++) {
    // executor.submit(runnables[i]);
    // }
    // }
    // } else {
    // for (int i = 0; i < numberOfParallelWriteInvocations; i++) {
    // executor.submit(runnables[i]);
    // }
    // }
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);
    System.out.println("Finished main tasks.");
    assertEquals(nullPointers.get(), 0, "Should not get any null pointers!");
    assertEquals(failures.get(), 0, "Should not get any other exceptions!");
    assertEquals(successes.get(), numberOfParallelWriteInvocations, "All tasks should have finished!");

    busyWorkExecutor.shutdown();
    busyWorkExecutor.awaitTermination(10, TimeUnit.SECONDS);

    // CompletableFuture[] futures = new CompletableFuture[numberOfParallelWriteInvocations];
    // synchronized (response) {
    // for (int i = 0; i < futures.length; i++) {
    // futures[i] = CompletableFuture.runAsync(runnables[i]);
    // }
    // }
    // CompletableFuture.allOf(futures).get();
  }
}
