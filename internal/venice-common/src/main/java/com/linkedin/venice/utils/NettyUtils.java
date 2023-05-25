package com.linkedin.venice.utils;

import static com.linkedin.venice.utils.ExceptionUtils.logClassLoaderContent;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import com.linkedin.venice.HttpConstants;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.ByteBuffer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class NettyUtils {
  private static final Logger LOGGER = LogManager.getLogger(NettyUtils.class);

  public static void setupResponseAndFlush(
      HttpResponseStatus status,
      byte[] body,
      boolean isJson,
      ChannelHandlerContext ctx) {
    FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status, Unpooled.wrappedBuffer(body));
    try {
      if (isJson) {
        response.headers().set(CONTENT_TYPE, HttpConstants.JSON);
      } else {
        response.headers().set(CONTENT_TYPE, HttpConstants.TEXT_PLAIN);
      }
    } catch (NoSuchMethodError e) { // netty version conflict
      LOGGER.warn("NoSuchMethodError, probably from netty version conflict.", e);
      logClassLoaderContent("netty");
      throw e;
    }
    response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
    ctx.writeAndFlush(response);
  }

  /**
   * N.B. there is a bug in {@link UnpooledHeapByteBuf#nioBuffer()} where it calls {@link ByteBuffer#slice()}, which
   * removes the position information. Hence, we do the BB wrapping ourselves to avoid it.
   */
  public static ByteBuffer getNioByteBuffer(ByteBuf nettyByteBuf, int index, int length) {
    return nettyByteBuf.hasArray()
        ? ByteBuffer.wrap(nettyByteBuf.array(), index, length)
        : nettyByteBuf.nioBuffer(index, length);
  }
}
