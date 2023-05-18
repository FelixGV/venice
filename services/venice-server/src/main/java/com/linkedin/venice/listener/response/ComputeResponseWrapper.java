package com.linkedin.venice.listener.response;

import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordSerializer;


public class ComputeResponseWrapper extends MultiKeyResponseWrapper<ComputeResponseRecordV1> {
  private static final RecordSerializer<ComputeResponseRecordV1> SERIALIZER =
      FastSerializerDeserializerFactory.getAvroGenericSerializer(ComputeResponseRecordV1.getClassSchema());

  public ComputeResponseWrapper(int maxKeyCount) {
    super(maxKeyCount);
  }

  @Override
  protected byte[] serializedResponse() {
    return SERIALIZER.serializeObjects(records);
  }

  @Override
  public int getResponseSchemaIdHeader() {
    return ReadAvroProtocolDefinition.COMPUTE_RESPONSE_V1.getProtocolVersion();
  }
}
