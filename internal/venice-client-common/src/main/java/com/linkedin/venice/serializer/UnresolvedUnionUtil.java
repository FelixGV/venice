package com.linkedin.venice.serializer;

import org.apache.avro.UnresolvedUnionException;
import org.apache.avro.generic.GenericContainer;


/**
 * This class makes use of {@link UnresolvedUnionException}, which is not present in all versions of Avro.
 *
 * Therefore, this class should only be invoked if we know for sure that we are handling an instance of this class,
 * which can be verified via {@link com.linkedin.venice.utils.AvroSchemaUtils#isUnresolvedUnionException(Throwable)}.
 */
public class UnresolvedUnionUtil {
  public static void handleUnresolvedUnion(Throwable t) {
    if (t instanceof UnresolvedUnionException) {
      UnresolvedUnionException serializationException = (UnresolvedUnionException) t;
      String datumDescription = datumDescription(serializationException.getUnresolvedDatum());
      throw new VeniceSerializationException(
          "The following type does not conform to any branch of the union: " + datumDescription,
          serializationException);
    }
    throw new IllegalArgumentException(
        "The Throwable param must be an instance of " + UnresolvedUnionException.class.getSimpleName());
  }

  private static String datumDescription(Object unresolvedDatum) {
    if (unresolvedDatum instanceof GenericContainer) {
      return ((GenericContainer) unresolvedDatum).getSchema().toString();
    } else if (unresolvedDatum == null) {
      return "null";
    } else {
      return unresolvedDatum.getClass().getSimpleName();
    }
  }
}
