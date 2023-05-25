package com.linkedin.venice.listener.request;

import static com.linkedin.venice.compute.ComputeRequestWrapper.LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST;

import com.linkedin.avro.netty.ByteBufDecoder;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import java.net.URI;


/**
 * {@code ComputeRouterRequestWrapper} encapsulates a POST request for read-compute from routers.
 */
public class ComputeRouterRequestWrapper extends MultiKeyRouterRequestWrapper<ComputeRouterRequestKeyV1> {
  private static final RecordDeserializer<ComputeRouterRequestKeyV1> DESERIALIZER =
      FastSerializerDeserializerFactory.getAvroSpecificDeserializer(ComputeRouterRequestKeyV1.class);

  private final ComputeRequestWrapper computeRequestWrapper;
  private int valueSchemaId = -1;

  private ComputeRouterRequestWrapper(
      String resourceName,
      ComputeRequestWrapper computeRequestWrapper,
      Iterable<ComputeRouterRequestKeyV1> keys,
      HttpRequest request,
      String schemaId) {
    super(resourceName, keys, request);
    this.computeRequestWrapper = computeRequestWrapper;
    if (schemaId != null) {
      this.valueSchemaId = Integer.parseInt(schemaId);
    }
  }

  public static ComputeRouterRequestWrapper parse(FullHttpRequest httpRequest, URI fullUri, String[] requestParts) {
    int apiVersion = RequestHelper.validateRequestAndGetApiVersion(requestParts, fullUri, httpRequest);
    if (apiVersion <= 0 || apiVersion > LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST) {
      throw new VeniceException(
          "Compute API version " + apiVersion + " is invalid. " + "Latest version is "
              + LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST);
    }
    String resourceName = requestParts[2];

    ByteBufDecoder decoder = new ByteBufDecoder();
    decoder.setBuffer(httpRequest.content());
    ComputeRequestWrapper computeRequestWrapper = new ComputeRequestWrapper(apiVersion);
    computeRequestWrapper.deserialize(decoder);

    Iterable<ComputeRouterRequestKeyV1> keys = DESERIALIZER.deserializeObjects(decoder, decoder::isEnd);
    String schemaId = httpRequest.headers().get(HttpConstants.VENICE_COMPUTE_VALUE_SCHEMA_ID);
    return new ComputeRouterRequestWrapper(resourceName, computeRequestWrapper, keys, httpRequest, schemaId);
  }

  public ComputeRequestWrapper getComputeRequest() {
    return computeRequestWrapper;
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }

  public String toString() {
    return "ComputeRouterRequestWrapper(storeName: " + getStoreName() + ", key count: " + keyCount + ")";
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.COMPUTE;
  }
}
