package com.linkedin.venice.serializer;

import static org.testng.Assert.assertThrows;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.ArrayUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AvroSerializerTest {
  private static final String value = "abc";
  private static final Schema schema = AvroCompatibilityHelper.parse("\"string\"");
  private static final RecordSerializer<String> serializer = new AvroSerializer<>(schema);
  String recordSchema = "{\n" //
      + "    \"type\": \"record\",\n" //
      + "    \"namespace\": \"com.linkedin.avro\",\n" //
      + "    \"name\": \"Person\",\n" //
      + "    \"fields\": [\n" //
      + "        {\n" //
      + "            \"name\": \"MapField\",\n" //
      + "            \"type\": {\n" //
      + "                \"type\": \"map\",\n" //
      + "                \"values\": \"string\"\n" //
      + "            }\n" //
      + "        }\n" //
      + "    ]\n" //
      + "}";
  Schema recordSchemaObject = AvroCompatibilityHelper.parse(recordSchema);

  @Test
  public void testSerialize() {
    byte[] serializedValue = serializer.serialize(value);
    Assert.assertTrue(serializedValue.length > value.getBytes().length);
  }

  @Test
  public void testSerializeObjects() {
    List<String> array = Arrays.asList(value, value);
    byte[] serializedValue = serializer.serialize(value);
    byte[] expectedSerializedArray = ArrayUtils.addAll(serializedValue, serializedValue);
    Assert.assertEquals(serializer.serializeObjects(array), expectedSerializedArray);

    byte[] prefixBytes = "prefix".getBytes();
    Assert.assertEquals(
        serializer.serializeObjects(array, ByteBuffer.wrap(prefixBytes)),
        ArrayUtils.addAll(prefixBytes, expectedSerializedArray));
  }

  @Test
  public void testDeterministicallySerializeMapWithDifferentSubclass() {
    AvroSerializer<GenericRecord> serializer = new AvroSerializer<>(recordSchemaObject);
    GenericRecord record = getGenericRecordWithPopulatedMap();

    // Verify no exception is thrown
    serializer.serialize(record);
  }

  @Test
  public void testRecoveryFromCorruptInternalStateOfReusableEncoder() throws IOException {
    AvroSerializerWithWriteFailure serializer = new AvroSerializerWithWriteFailure(recordSchemaObject);
    GenericRecord record = getGenericRecordWithPopulatedMap();

    // First invocation is expected to throw.
    assertThrows(VeniceException.class, () -> serializer.serialize(record));

    // We should be able to serialize twice and get the same result everytime,
    // which would not be the case if the internal state is corrupted by the first exception
    byte[] bytes = serializer.serialize(record);
    Assert.assertEquals(serializer.serialize(record), bytes);
  }

  private static class AvroSerializerWithWriteFailure<T> extends AvroSerializer<T> {
    private boolean firstUse = true;

    public AvroSerializerWithWriteFailure(Schema schema) {
      super(schema);
    }

    @Override
    public void write(T object, Encoder encoder) throws IOException {
      super.write(object, encoder);
      if (firstUse) {
        firstUse = false;
        throw new IOException("Exception on first use only... subsequent invocations will succeed.");
      }
    }
  }

  private GenericRecord getGenericRecordWithPopulatedMap() {
    Map<Object, Object> map = new LinkedHashMap<>();
    map.put("key1", "valueStr");
    map.put(new Utf8("key2"), "valueUtf8");
    map.put(new Utf8("key3"), "valueUtf8_2");
    map.put("key4", "valueStr_2");
    map.put(10L, "valueStr_2");

    GenericRecord record = new GenericData.Record(recordSchemaObject);
    record.put("MapField", map);

    return record;
  }
}
