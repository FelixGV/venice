package com.linkedin.venice.duckdb;

import static com.linkedin.venice.sql.AvroToSQL.UnsupportedTypeHandling.SKIP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.sql.AvroToSQL;
import com.linkedin.venice.sql.AvroToSQLTest;
import com.linkedin.venice.utils.ByteUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.duckdb.DuckDBResultSet;
import org.testng.annotations.Test;


public class DuckDBAvroToSQLTest {
  @Test
  public void test() throws SQLException, IOException {
    List<Schema.Field> fields = AvroToSQLTest.getAllValidFields();
    Schema avroSchema = Schema.createRecord("MyRecord", "", "", false, fields);
    Set<String> primaryKeyFields = new HashSet<>();
    primaryKeyFields.add("intField");

    // N.B.: Multiple primary/unique keys don't work so far... TODO: debug why
    // primaryKeyFields.add("longField");
    try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
        Statement stmt = connection.createStatement()) {
      // create a table
      String tableName = "MyRecord_v1";
      String createTableStatement = AvroToSQL.createTableStatement(tableName, avroSchema, primaryKeyFields, SKIP);
      System.out.println(createTableStatement);
      stmt.execute(createTableStatement);

      String upsertStatement = AvroToSQL.upsertStatement(tableName, avroSchema, primaryKeyFields);
      System.out.println(upsertStatement);
      BiConsumer<GenericRecord, PreparedStatement> upsertProcessor = AvroToSQL.upsertProcessor(tableName, avroSchema);
      for (int rewriteIteration = 0; rewriteIteration < 3; rewriteIteration++) {
        List<GenericRecord> records = generateRecords(avroSchema);
        GenericRecord record;
        try (PreparedStatement preparedStatement = connection.prepareStatement(upsertStatement)) {
          for (int i = 0; i < records.size(); i++) {
            record = records.get(i);
            upsertProcessor.accept(record, preparedStatement);
          }
        }

        int recordCount = records.size();
        try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
          for (int j = 0; j < recordCount; j++) {
            assertTrue(
                rs.next(),
                "Rewrite iteration " + rewriteIteration + ". Expected a row at index " + j
                    + " after having inserted up to row " + recordCount);

            record = records.get(j);
            for (Schema.Field field: avroSchema.getFields()) {
              Object result = rs.getObject(field.name());
              if (result instanceof DuckDBResultSet.DuckDBBlobResult) {
                DuckDBResultSet.DuckDBBlobResult duckDBBlobResult = (DuckDBResultSet.DuckDBBlobResult) result;
                byte[] actual = duckDBBlobResult.getBinaryStream().readAllBytes();
                byte[] expected = ((ByteBuffer) record.get(field.pos())).array();
                assertEquals(
                    actual,
                    expected,
                    "Rewrite iteration " + rewriteIteration + ". Bytes not equals at row " + j + "! actual: "
                        + ByteUtils.toHexString(actual) + ", wanted: " + ByteUtils.toHexString(expected));
                // System.out.println("Rewrite iteration " + rewriteIteration + ". Row: " + j + ", field: " +
                // field.name() + ", value: " + ByteUtils.toHexString(actual));
              } else {
                assertEquals(
                    result,
                    record.get(field.name()),
                    "Rewrite iteration " + rewriteIteration + ". Field '" + field.name() + "' is not correct at row "
                        + j + "!");
                // System.out.println("Rewrite iteration " + rewriteIteration + ". Row: " + j + ", field: " +
                // field.name() + ", value: " + result);
              }
            }

            System.out.println("Rewrite iteration " + rewriteIteration + ". Successfully validated row " + j);
          }
          assertFalse(rs.next(), "Expected no more rows at index " + recordCount);
        }
        System.out
            .println("Rewrite iteration " + rewriteIteration + ". Successfully validated up to i = " + recordCount);
      }
    }
  }

  private List<GenericRecord> generateRecords(Schema avroSchema) {
    List<GenericRecord> records = new ArrayList<>();

    GenericRecord record;
    Object fieldValue;
    Random random = new Random();
    for (int i = 0; i < 10; i++) {
      record = new GenericData.Record(avroSchema);
      for (Schema.Field field: avroSchema.getFields()) {
        if (field.name().equals("intField")) {
          // Primary key
          fieldValue = i;
        } else {
          Schema fieldSchema = field.schema();
          if (fieldSchema.getType() == Schema.Type.UNION) {
            Schema first = field.schema().getTypes().get(0);
            Schema second = field.schema().getTypes().get(1);
            if (first.getType() == Schema.Type.NULL) {
              fieldSchema = second;
            } else if (second.getType() == Schema.Type.NULL) {
              fieldSchema = first;
            } else {
              throw new IllegalArgumentException("Unsupported union: " + field.schema());
            }
          }
          fieldValue = randomValue(fieldSchema, random);
        }
        record.put(field.pos(), fieldValue);
      }
      records.add(record);
    }

    return records;
  }

  private Object randomValue(Schema schema, Random random) {
    switch (schema.getType()) {
      case STRING:
        return String.valueOf(random.nextLong());
      case INT:
        return random.nextInt();
      case LONG:
        return random.nextLong();
      case FLOAT:
        return random.nextFloat();
      case DOUBLE:
        return random.nextDouble();
      case BOOLEAN:
        return random.nextBoolean();
      case BYTES:
        return getBB(10, random);
      case FIXED:
        return getBB(schema.getFixedSize(), random);
      case NULL:
        return null;
      default:
        throw new IllegalArgumentException("Unsupported type: " + schema.getType());
    }
  }

  private ByteBuffer getBB(int size, Random random) {
    byte[] bytes = new byte[size];
    random.nextBytes(bytes);
    return ByteBuffer.wrap(bytes);
  }
}
