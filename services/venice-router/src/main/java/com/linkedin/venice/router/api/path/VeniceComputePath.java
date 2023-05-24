package com.linkedin.venice.router.api.path;

import static com.linkedin.venice.HttpConstants.VENICE_COMPUTE_VALUE_SCHEMA_ID;
import static com.linkedin.venice.compute.ComputeRequestWrapper.LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST;
import static com.linkedin.venice.router.api.VenicePathParser.TYPE_COMPUTE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.alpini.router.api.RouterException;
import com.linkedin.avro.netty.ByteBufDecoder;
import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.compute.protocol.request.router.ComputeRouterRequestKeyV1;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.RouterExceptionAndTrackingUtils;
import com.linkedin.venice.router.api.RouterKey;
import com.linkedin.venice.router.api.VenicePartitionFinder;
import com.linkedin.venice.router.api.VenicePathParser;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.NettyUtils;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;


public class VeniceComputePath extends VeniceMultiKeyPath<ComputeRouterRequestKeyV1> {
  private static final RecordDeserializer<ByteBuffer> COMPUTE_REQUEST_CLIENT_KEY_V1_DESERIALIZER =
      FastSerializerDeserializerFactory
          .getAvroGenericDeserializer(ReadAvroProtocolDefinition.COMPUTE_REQUEST_CLIENT_KEY_V1.getSchema());

  private static final RecordSerializer<ComputeRouterRequestKeyV1> COMPUTE_ROUTER_REQUEST_KEY_V1_SERIALIZER =
      FastSerializerDeserializerFactory.getAvroGenericSerializer(ComputeRouterRequestKeyV1.getClassSchema());

  // Compute request is useless for now in router, until we support ranking in the future.
  private final ComputeRequestWrapper computeRequestWrapper;
  private final ByteBuf requestContent;
  private final int computeRequestLengthInBytes;
  private int valueSchemaId;

  private final int computeRequestVersion;

  public VeniceComputePath(
      String storeName,
      int versionNumber,
      String resourceName,
      BasicFullHttpRequest request,
      VenicePartitionFinder partitionFinder,
      int maxKeyCount,
      boolean smartLongTailRetryEnabled,
      int smartLongTailRetryAbortThresholdMs,
      int longTailRetryMaxRouteForMultiKeyReq) throws RouterException {
    super(
        storeName,
        versionNumber,
        resourceName,
        smartLongTailRetryEnabled,
        smartLongTailRetryAbortThresholdMs,
        longTailRetryMaxRouteForMultiKeyReq);

    // Get API version
    this.computeRequestVersion = Integer.parseInt(request.headers().get(HttpConstants.VENICE_API_VERSION));
    CharSequence schemaHeader = request.getRequestHeaders().get(VENICE_COMPUTE_VALUE_SCHEMA_ID);
    this.valueSchemaId = schemaHeader == null ? -1 : Integer.parseInt((String) schemaHeader);

    if (this.computeRequestVersion <= 0 || this.computeRequestVersion > LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST) {
      throw RouterExceptionAndTrackingUtils.newRouterExceptionAndTracking(
          Optional.of(getStoreName()),
          Optional.of(getRequestType()),
          BAD_REQUEST,
          "Compute API version " + this.computeRequestVersion + " is invalid. Latest version is "
              + LATEST_SCHEMA_VERSION_FOR_COMPUTE_REQUEST);
    }

    /**
     * The first part of the request content from client is the ComputeRequest which contains an array of operations
     * and the result schema string for now; deserialize the first part and record the length of the first part
     */
    this.computeRequestWrapper = new ComputeRequestWrapper(computeRequestVersion);
    this.requestContent = request.content();
    ByteBufDecoder decoder = new ByteBufDecoder();
    decoder.setBuffer(this.requestContent);
    int readerIndexBeforeDeserialization = this.requestContent.readerIndex();
    this.computeRequestWrapper.deserialize(decoder);
    this.computeRequestLengthInBytes = requestContent.readerIndex() - readerIndexBeforeDeserialization;

    // deserialize the second part of the request content using the same decoder
    Iterable<ByteBuffer> keys = COMPUTE_REQUEST_CLIENT_KEY_V1_DESERIALIZER.deserializeObjects(decoder, decoder::isEnd);

    initialize(storeName, resourceName, keys, partitionFinder, maxKeyCount, null);
  }

  private VeniceComputePath(
      String storeName,
      int versionNumber,
      String resourceName,
      Map<RouterKey, ComputeRouterRequestKeyV1> routerKeyMap,
      ComputeRequestWrapper computeRequestWrapper,
      ByteBuf requestContent,
      int computeRequestLengthInBytes,
      int computeRequestVersion,
      boolean smartLongTailRetryEnabled,
      int smartLongTailRetryAbortThresholdMs,
      int longTailRetryMaxRouteForMultiKeyReq) {
    super(
        storeName,
        versionNumber,
        resourceName,
        smartLongTailRetryEnabled,
        smartLongTailRetryAbortThresholdMs,
        routerKeyMap,
        longTailRetryMaxRouteForMultiKeyReq);
    this.computeRequestWrapper = computeRequestWrapper;
    this.requestContent = requestContent;
    this.computeRequestLengthInBytes = computeRequestLengthInBytes;
    this.computeRequestVersion = computeRequestVersion;
    setPartitionKeys(routerKeyMap.keySet());
  }

  @Nonnull
  @Override
  public String getLocation() {
    StringBuilder sb = new StringBuilder();
    sb.append(TYPE_COMPUTE).append(VenicePathParser.SEP).append(getResourceName());
    return sb.toString();
  }

  @Override
  public final RequestType getRequestType() {
    return isStreamingRequest() ? getStreamingRequestType() : RequestType.COMPUTE;
  }

  @Override
  public final RequestType getStreamingRequestType() {
    return RequestType.COMPUTE_STREAMING;
  }

  /**
   * If the parent request is a retry request, the sub-request generated by scattering-gathering logic should be retry
   * request as well.
   *
   * @param routerKeyMap
   * @return
   */
  @Override
  protected VeniceComputePath fixRetryRequestForSubPath(Map<RouterKey, ComputeRouterRequestKeyV1> routerKeyMap) {
    VeniceComputePath subPath = new VeniceComputePath(
        storeName,
        versionNumber,
        getResourceName(),
        routerKeyMap,
        this.computeRequestWrapper,
        this.requestContent,
        this.computeRequestLengthInBytes,
        this.computeRequestVersion,
        isSmartLongTailRetryEnabled(),
        getSmartLongTailRetryAbortThresholdMs(),
        getLongTailRetryMaxRouteForMultiKeyReq());
    subPath.setupRetryRelatedInfo(this);
    subPath.setValueSchemaId(this.getValueSchemaId());
    return subPath;
  }

  @Override
  protected ComputeRouterRequestKeyV1 createRouterRequestKey(ByteBuffer key, int keyIdx, int partitionId) {
    ComputeRouterRequestKeyV1 routerRequestKey = new ComputeRouterRequestKeyV1();
    routerRequestKey.keyBytes = key;
    routerRequestKey.keyIndex = keyIdx;
    routerRequestKey.partitionId = partitionId;
    return routerRequestKey;
  }

  @Override
  protected byte[] serializeRouterRequest() {
    return COMPUTE_ROUTER_REQUEST_KEY_V1_SERIALIZER.serializeObjects(
        routerKeyMap.values(),
        NettyUtils.getNioByteBuffer(requestContent, 0, computeRequestLengthInBytes));
  }

  public int getValueSchemaId() {
    return valueSchemaId;
  }

  public void setValueSchemaId(int id) {
    this.valueSchemaId = id;
  }

  @Override
  public void setupVeniceHeaders(BiConsumer<String, String> setupHeaderFunc) {
    super.setupVeniceHeaders(setupHeaderFunc);
    setupHeaderFunc.accept(VENICE_COMPUTE_VALUE_SCHEMA_ID, Integer.toString(getValueSchemaId()));
  }

  @Override
  public String getVeniceApiVersionHeader() {
    return String.valueOf(computeRequestVersion);
  }

  // for testing
  protected ComputeRequestWrapper getComputeRequest() {
    return computeRequestWrapper;
  }

  // for testing
  protected int getComputeRequestLengthInBytes() {
    return computeRequestLengthInBytes;
  }
}
