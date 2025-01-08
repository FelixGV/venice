package com.linkedin.alpini.router.impl;

import com.linkedin.alpini.base.concurrency.AsyncFuture;
import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.alpini.base.registry.ResourceRegistry;
import com.linkedin.alpini.base.registry.ShutdownableResource;
import com.linkedin.alpini.router.api.Netty;
import com.linkedin.alpini.router.api.ResourcePath;
import com.linkedin.alpini.router.api.RouterTimeoutProcessor;
import com.linkedin.alpini.router.api.ScatterGatherHelper;
import com.linkedin.alpini.router.impl.netty4.Router4;
import java.net.SocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public interface Router extends ShutdownableResource {
  AsyncFuture<SocketAddress> start(@Nonnull SocketAddress address);

  AsyncFuture<SocketAddress> getLocalAddress();

  AsyncFuture<Void> setAcceptConnection(boolean enabled);

  int getConnectedCount();

  static <H, P extends ResourcePath<K>, K, R> Builder builder(
      @Nonnull ScatterGatherHelper<H, P, K, R, ?, ?, ?> scatterGatherHelper) {
    if (scatterGatherHelper.dispatcherNettyVersion() != Netty.NETTY_4_1) {
      throw new IllegalStateException("Only Netty 4.1 is supported.");
    }
    return new Router4<>(scatterGatherHelper);
  }

  interface Builder {
    int MINIMUM_MAX_CHUNK_SIZE = 256;
    long MINIMUM_IDLE_TIMEOUT_MILLIS = 1000L;

    Builder name(@Nonnull String name);

    Builder resourceRegistry(@Nonnull ResourceRegistry resourceRegistry);

    Builder threadFactory(@Nonnull ThreadFactory threadFactory);

    Builder serverSocketChannel(@Nonnull Class<?> serverSocketChannel);

    Builder executor(@Nonnull Executor executor);

    Builder timeoutProcessor(@Nonnull RouterTimeoutProcessor timeoutProcessor);

    default Builder timeoutProcessor(@Nonnull TimeoutProcessor timeoutProcessor) {
      return timeoutProcessor(RouterTimeoutProcessor.adapt(timeoutProcessor));
    }

    Builder connectionLimit(@Nonnegative int connectionLimit);

    Builder connectionLimit(@Nonnull IntSupplier connectionLimit);

    Builder maxChunkSize(@Nonnegative int maxChunkSize);

    Builder idleTimeout(@Nonnegative long time, @Nonnull TimeUnit unit);

    /* HTTP/2 Settings */
    default Builder enableInboundHttp2(boolean enableHttp2) {
      // Do Nothing
      return this;
    }

    default Builder http2MaxConcurrentStreams(int maxConcurrentStreams) {
      // Do Nothing
      return this;
    }

    default Builder http2MaxFrameSize(int http2MaxFrameSize) {
      // Do Nothing
      return this;
    }

    default Builder http2InitialWindowSize(int http2InitialWindowSize) {
      // Do Nothing
      return this;
    }

    default Builder http2HeaderTableSize(int http2HeaderTableSize) {
      // Do Nothing
      return this;
    }

    default Builder http2MaxHeaderListSize(int http2MaxHeaderListSize) {
      // Do Nothing
      return this;
    }

    <CHANNEL_PIPELINE> Builder beforeHttpServerCodec(
        @Nonnull Class<CHANNEL_PIPELINE> pipelineClass,
        @Nonnull Consumer<CHANNEL_PIPELINE> consumer);

    <CHANNEL_PIPELINE> Builder beforeHttpRequestHandler(
        @Nonnull Class<CHANNEL_PIPELINE> pipelineClass,
        @Nonnull Consumer<CHANNEL_PIPELINE> consumer);

    <POOL_TYPE> Builder ioWorkerPoolBuilder(
        @Nonnull Class<POOL_TYPE> poolClass,
        @Nonnull Function<Executor, POOL_TYPE> builder);

    <POOL_TYPE> Builder bossPoolBuilder(
        @Nonnull Class<POOL_TYPE> poolClass,
        @Nonnull Function<Executor, POOL_TYPE> builder);

    @CheckReturnValue
    Router build();
  }
}
