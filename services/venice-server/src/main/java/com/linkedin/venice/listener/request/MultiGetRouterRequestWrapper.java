package com.linkedin.venice.listener.request;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.read.protocol.request.router.MultiGetRouterRequestKeyV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import java.net.URI;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.OptimizedBinaryDecoderFactory;


/**
 * {@code MultiGetRouterRequestWrapper} encapsulates a POST request to storage/resourcename on the storage node for a multi-get operation.
 */
public class MultiGetRouterRequestWrapper extends MultiKeyRouterRequestWrapper<MultiGetRouterRequestKeyV1> {
  private static final RecordDeserializer<MultiGetRouterRequestKeyV1> DESERIALIZER =
      FastSerializerDeserializerFactory.getAvroSpecificDeserializer(MultiGetRouterRequestKeyV1.class);

  private MultiGetRouterRequestWrapper(
      String resourceName,
      Iterable<MultiGetRouterRequestKeyV1> keys,
      HttpRequest request) {
    super(resourceName, keys, request);
  }

  public static MultiGetRouterRequestWrapper parse(FullHttpRequest httpRequest, URI fullUri, String[] requestParts) {
    int apiVersion = RequestHelper.validateRequestAndGetApiVersion(requestParts, fullUri, httpRequest);
    int expectedApiVersion = ReadAvroProtocolDefinition.MULTI_GET_ROUTER_REQUEST_V1.getProtocolVersion();
    if (apiVersion != expectedApiVersion) {
      throw new VeniceException("Expected API version: " + expectedApiVersion + ", but received: " + apiVersion);
    }
    String resourceName = requestParts[2];

    // N.B.: The Netty ByteBufDecoder doesn't work because it returns DirectByteBuffers, which are harder to use with
    // the
    // RocksDB APIs
    // ByteBufDecoder decoder = new ByteBufDecoder();
    // decoder.setBuffer(httpRequest.content());

    byte[] content = new byte[httpRequest.content().readableBytes()];
    httpRequest.content().readBytes(content);
    BinaryDecoder decoder =
        OptimizedBinaryDecoderFactory.defaultFactory().createOptimizedBinaryDecoder(content, 0, content.length);

    Iterable<MultiGetRouterRequestKeyV1> keys = DESERIALIZER.deserializeObjects(decoder, decoder::isEnd);

    return new MultiGetRouterRequestWrapper(resourceName, keys, httpRequest);
  }

  public String toString() {
    return "MultiGetRouterRequestWrapper(storeName: " + getStoreName() + ", key count: " + keyCount + ")";
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.MULTI_GET;
  }
}
