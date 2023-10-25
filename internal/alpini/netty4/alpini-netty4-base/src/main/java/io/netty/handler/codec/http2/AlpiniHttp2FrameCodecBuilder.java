package io.netty.handler.codec.http2;

import io.netty.channel.Channel;
import java.util.Objects;
import java.util.function.Predicate;


public class AlpiniHttp2FrameCodecBuilder extends Http2FrameCodecBuilder {
  public static final Predicate<Channel> CAN_ALWAYS_CREATE_STREAMS = ch -> true;
  private Predicate<Channel> _canCreateStreams = Channel::isOpen;

  AlpiniHttp2FrameCodecBuilder(boolean server) {
    super(server);
  }

  public AlpiniHttp2FrameCodecBuilder canCreateStreams(Predicate<Channel> canCreateStreams) {
    _canCreateStreams = Objects.requireNonNull(canCreateStreams);
    return this;
  }

  /**
   * Creates a builder for a HTTP/2 server.
   */
  public static AlpiniHttp2FrameCodecBuilder forServer() {
    return new AlpiniHttp2FrameCodecBuilder(true);
  }

  /**
   * Creates a builder for an HTTP/2 client.
   * @return Builder for client
   */
  public static AlpiniHttp2FrameCodecBuilder forClient() {
    return new AlpiniHttp2FrameCodecBuilder(false);
  }

  @Override
  protected Http2FrameCodec build(
      Http2ConnectionDecoder decoder,
      Http2ConnectionEncoder encoder,
      Http2Settings initialSettings) {
    AlpiniHttp2FrameCodec codec =
        new AlpiniHttp2FrameCodec(encoder, decoder, initialSettings, decoupleCloseAndGoAway(), _canCreateStreams);
    codec.gracefulShutdownTimeoutMillis(gracefulShutdownTimeoutMillis());
    return codec;
  }
}
