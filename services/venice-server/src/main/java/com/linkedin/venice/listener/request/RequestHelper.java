package com.linkedin.venice.listener.request;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.exceptions.VeniceException;
import io.netty.handler.codec.http.FullHttpRequest;
import java.net.URI;


public class RequestHelper {
  /**
   * Parses the '/' separated parts of a request uri.
   * @param uri
   * @return String array of the request parts
   */
  public static String[] getRequestParts(String uri) {
    /**
     * Sometimes req.uri() gives a full uri (e.g. https://host:port/path) and sometimes it only gives a path.
     * Generating a URI lets us always take just the path, but we need to add on the query string.
     */
    URI fullUri = URI.create(uri);
    return getRequestParts(fullUri);
  }

  public static String[] getRequestParts(URI uri) {
    String path = uri.getRawQuery() == null ? uri.getRawPath() : uri.getRawPath() + "?" + uri.getRawQuery();
    return path.split("/");
  }

  public static int validateRequestAndGetApiVersion(String[] requestParts, URI fullUri, FullHttpRequest httpRequest) {
    if (requestParts.length != 3) {
      // [0]""/[1]"compute or storage"/[2]"resource name"
      throw new VeniceException("Invalid request: " + fullUri.getRawPath());
    }

    // Validate API version
    String apiVersionStr = httpRequest.headers().get(HttpConstants.VENICE_API_VERSION);
    if (apiVersionStr == null) {
      throw new VeniceException("Header: " + HttpConstants.VENICE_API_VERSION + " is missing");
    }
    return Integer.parseInt(apiVersionStr);
  }
}
