package com.linkedin.venice.listener.request;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.RequestConstants;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.utils.EncodingUtils;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;


/**
 * {@code GetRouterRequest} encapsulates a GET request to storage/resourcename/partition/key on the storage node for a single-get operation.
 */
public class GetRouterRequest extends RouterRequest {
  private final int partition;
  private final byte[] keyBytes;

  private GetRouterRequest(String resourceName, int partition, byte[] keyBytes, HttpRequest request) {
    super(resourceName, request);

    this.partition = partition;
    this.keyBytes = keyBytes;
  }

  public int getPartition() {
    return partition;
  }

  public byte[] getKeyBytes() {
    return keyBytes;
  }

  @Override
  public RequestType getRequestType() {
    return RequestType.SINGLE_GET;
  }

  @Override
  public int getKeyCount() {
    return 1;
  }

  public static GetRouterRequest parse(HttpRequest request, String[] requestParts, String rawQuery) {
    String uri = request.uri();
    if (requestParts.length == 5) {
      // [0]""/[1]"action"/[2]"store"/[3]"partition"/[4]"key"
      String topicName = requestParts[2];
      int partition = Integer.parseInt(requestParts[3]);
      byte[] keyBytes = getKeyBytesFromUrlKeyString(requestParts[4], uri, rawQuery);
      return new GetRouterRequest(topicName, partition, keyBytes, request);
    } else {
      throw new VeniceException("Not a valid request for a STORAGE action: " + uri);
    }
  }

  public static byte[] getKeyBytesFromUrlKeyString(String keyString, String uri, String rawQuery) {
    // Looking for common cases upfront, before doing a full-blown decoding
    if (rawQuery == null) {
      return keyString.getBytes(StandardCharsets.UTF_8);
    } else if (rawQuery.equals(RequestConstants.B64_FORMAT_KEY_VALUE)) {
      return EncodingUtils.base64DecodeFromString(keyString);
    } else {
      // Someone trying to be smart and injecting a bunch of extra GET query params...
      QueryStringDecoder queryStringParser = new QueryStringDecoder(uri, StandardCharsets.UTF_8);
      List<String> formatKeyParam = queryStringParser.parameters().get(RequestConstants.FORMAT_KEY);
      String format = formatKeyParam == null ? RequestConstants.DEFAULT_FORMAT : formatKeyParam.get(0);
      switch (format) {
        case RequestConstants.B64_FORMAT:
          return EncodingUtils.base64DecodeFromString(keyString);
        default:
          return keyString.getBytes(StandardCharsets.UTF_8);
      }
    }
  }

  /***
   * throws VeniceException if we don't handle the specified api version
   * @param headers
   */
  public static void verifyApiVersion(HttpHeaders headers, String expectedVersion) {
    if (headers.contains(HttpConstants.VENICE_API_VERSION)) { /* if not present, assume latest version */
      String clientApiVersion = headers.get(HttpConstants.VENICE_API_VERSION);
      if (!clientApiVersion.equals(expectedVersion)) {
        throw new VeniceException("Storage node is not compatible with requested API version: " + clientApiVersion);
      }
    }
  }
}
