package com.linkedin.alpini.router.impl.netty4;

import com.linkedin.alpini.netty4.handlers.BasicServerChannelInitializer;
import com.linkedin.alpini.netty4.handlers.ConnectionLimitHandler;
import com.linkedin.alpini.netty4.handlers.Http2SettingsFrameLogger;
import com.linkedin.alpini.netty4.http2.Http2PipelineInitializer;
import com.linkedin.alpini.router.ScatterGatherRequestHandler4;
import com.linkedin.alpini.router.api.ResourcePath;
import com.linkedin.alpini.router.impl.RouterPipelineFactory;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http2.ActiveStreamsCountHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import javax.annotation.Nonnull;


/**
 * @author Antony T Curtis {@literal <acurtis@linkedin.com>}
 */
public class Router4PipelineFactory<C extends Channel> extends
    BasicServerChannelInitializer<C, Router4PipelineFactory<C>> implements RouterPipelineFactory<ChannelHandler> {
  private final List<Consumer<ChannelPipeline>> _beforeHttpServerCodec = new ArrayList<>();
  private final List<Consumer<ChannelPipeline>> _beforeHttpRequestHandler = new ArrayList<>();
  private Http2PipelineInitializer.BuilderSupplier _http2PipelineInitializerBuilder =
      Http2PipelineInitializer.DEFAULT_BUILDER;

  public <H, P extends ResourcePath<K>, K, R> Router4PipelineFactory(
      @Nonnull ConnectionLimitHandler connectionLimit,
      @Nonnull ActiveStreamsCountHandler activeStreamsCountHandler,
      @Nonnull Http2SettingsFrameLogger http2SettingsFrameLogger,
      @Nonnull BooleanSupplier shutdownFlag,
      @Nonnull ScatterGatherRequestHandler4<H, P, K, R> scatterGatherRequestHandler) {
    super(
        connectionLimit,
        activeStreamsCountHandler,
        http2SettingsFrameLogger,
        shutdownFlag,
        scatterGatherRequestHandler);
  }

  private Router4PipelineFactory<C> add(
      List<Consumer<ChannelPipeline>> list,
      Consumer<ChannelPipeline> pipelineConsumer) {
    Objects.requireNonNull(pipelineConsumer, "pipelineConsumer");
    list.add(pipelineConsumer);
    return factory();
  }

  public Router4PipelineFactory<C> addBeforeHttpServerCodec(@Nonnull Consumer<ChannelPipeline> pipelineConsumer) {
    return add(_beforeHttpServerCodec, pipelineConsumer);
  }

  public Router4PipelineFactory<C> addBeforeHttpRequestHandler(@Nonnull Consumer<ChannelPipeline> pipelineConsumer) {
    return add(_beforeHttpRequestHandler, pipelineConsumer);
  }

  private static void apply(List<Consumer<ChannelPipeline>> list, ChannelPipeline pipeline) {
    for (Consumer<ChannelPipeline> consumer: list) {
      consumer.accept(pipeline);
    }
  }

  public Router4PipelineFactory<C> setHttp2PipelineInitializer(Http2PipelineInitializer.BuilderSupplier supplier) {
    _http2PipelineInitializerBuilder = Optional.ofNullable(supplier).orElse(Http2PipelineInitializer.DEFAULT_BUILDER);
    return factory();
  }

  @Override
  protected Http2PipelineInitializer.BuilderSupplier getHttp2PipelineInitializerBuilderSupplier() {
    return _http2PipelineInitializerBuilder;
  }

  @Override
  protected void beforeHttpServerCodec(@Nonnull ChannelPipeline pipeline) {
    super.beforeHttpServerCodec(pipeline);
    apply(_beforeHttpServerCodec, pipeline);
  }

  @Override
  protected void beforeHttpRequestHandler(@Nonnull ChannelPipeline pipeline) {
    super.beforeHttpRequestHandler(pipeline);
    apply(_beforeHttpRequestHandler, pipeline);
  }
}
