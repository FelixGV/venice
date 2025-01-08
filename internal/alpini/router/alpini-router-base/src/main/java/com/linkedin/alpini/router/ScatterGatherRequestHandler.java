package com.linkedin.alpini.router;

import com.linkedin.alpini.base.concurrency.TimeoutProcessor;
import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.router.api.Netty;
import com.linkedin.alpini.router.api.ResourcePath;
import com.linkedin.alpini.router.api.RouterTimeoutProcessor;
import com.linkedin.alpini.router.api.ScatterGatherHelper;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public abstract class ScatterGatherRequestHandler<H, P extends ResourcePath<K>, K, R> {
  protected static final Logger LOG = LogManager.getLogger(ScatterGatherRequestHandler.class);
  protected static final Runnable NOP = () -> {};

  protected final @Nonnull RouterTimeoutProcessor _timeoutProcessor;

  protected ScatterGatherRequestHandler(@Nonnull RouterTimeoutProcessor timeoutProcessor) {
    _timeoutProcessor = Objects.requireNonNull(timeoutProcessor, "timeoutProcessor");
  }

  protected ScatterGatherRequestHandler(@Nonnull TimeoutProcessor timeoutProcessor) {
    this(RouterTimeoutProcessor.adapt(timeoutProcessor));
  }

  @SuppressWarnings("unchecked")
  public static <H, P extends ResourcePath<K>, K, R> ScatterGatherRequestHandler<H, P, K, R> make(
      @Nonnull ScatterGatherHelper<H, P, K, R, BasicFullHttpRequest, FullHttpResponse, HttpResponseStatus> scatterGatherHelper,
      @Nonnull RouterTimeoutProcessor timeoutProcessor) {
    if (scatterGatherHelper.dispatcherNettyVersion() != Netty.NETTY_4_1) {
      throw new IllegalStateException("Only Netty 4.1 is supported.");
    }
    try {
      return new ScatterGatherRequestHandler4<>(scatterGatherHelper, timeoutProcessor);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public abstract @Nonnull ScatterGatherHelper<H, P, K, R, ?, ?, ?> getScatterGatherHelper();

  protected abstract boolean isTooLongFrameException(Throwable cause);
}
