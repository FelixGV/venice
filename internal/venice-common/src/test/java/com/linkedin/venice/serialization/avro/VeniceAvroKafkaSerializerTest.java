package com.linkedin.venice.serialization.avro;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.serializer.AvroSerializer;
import com.linkedin.venice.serializer.VeniceSerializationException;
import java.io.IOException;
import java.util.Queue;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class VeniceAvroKafkaSerializerTest {
  private static final Logger LOGGER = LogManager.getLogger(VeniceAvroKafkaSerializerTest.class);
  private String topic = "dummyTopic";

  @Test
  public void testSerializeUnionOfEnumField() throws IOException {
    try {
      Schema recordSchema = Schema.parse(
          "{\n" + "  \"fields\": [\n" + "    {\n" + "      \"default\": null,\n" + "      \"name\": \"union\",\n"
              + "      \"type\": [\n" + "        \"null\",\n" + "        {\n" + "          \"name\": \"EnumField1\",\n"
              + "          \"symbols\": [\n" + "            \"A\",\n" + "            \"B\",\n" + "            \"C\"\n"
              + "          ],\n" + "          \"type\": \"enum\"\n" + "        }\n" + "      ]\n" + "    }\n" + "  ],\n"
              + "  \"name\": \"UnionOfEnumRecord\",\n" + "  \"type\": \"record\",\n"
              + "  \"namespace\": \"com.linkedin.venice.serialization.avro\"\n" + "}");
      Schema enumSchema = recordSchema.getField("union").schema().getTypes().get(1);

      VeniceAvroKafkaSerializer serializer = new VeniceAvroKafkaSerializer(recordSchema);

      // test serializing specificRecord
      UnionOfEnumRecord specificRecord = new UnionOfEnumRecord();
      specificRecord.union = EnumField1.B;

      byte[] bytes = serializer.serialize(topic, specificRecord);

      GenericRecord genericRecordResult = (GenericRecord) serializer.deserialize(topic, bytes);
      Assert.assertEquals(genericRecordResult.get("union"), AvroCompatibilityHelper.newEnumSymbol(enumSchema, "B"));

      Decoder decoder = DecoderFactory.defaultFactory().createBinaryDecoder(bytes, null);
      SpecificDatumReader<UnionOfEnumRecord> reader = new SpecificDatumReader<>(UnionOfEnumRecord.class);
      UnionOfEnumRecord result = reader.read(null, decoder);
      Assert.assertEquals(specificRecord, result);

      // test serializing genericRecord
      GenericRecord genericRecord = new GenericData.Record(recordSchema);
      genericRecord.put("union", AvroCompatibilityHelper.newEnumSymbol(enumSchema, "B"));

      Assert.assertEquals(serializer.serialize(topic, specificRecord), bytes);
    } catch (VeniceSerializationException e) {
      LOGGER.error("Caught a VeniceSerializationException! Will print all usage sites of the serializer...");
      int i = 0;
      Queue<Throwable> usageSites = AvroSerializer.REUSABLE_OBJECTS.get().lastUsageSites;
      for (Throwable t: usageSites) {
        LOGGER.error("Usage #" + i + " / " + usageSites.size(), t);
      }
    }
  }
}
