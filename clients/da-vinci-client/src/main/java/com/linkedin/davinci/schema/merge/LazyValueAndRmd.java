package com.linkedin.davinci.schema.merge;

import com.linkedin.venice.utils.lazy.Lazy;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;


/**
 * This class holds a value of type {@param T} and its corresponding replication metadata.
 */
public class LazyValueAndRmd<T> extends ValueAndRmd<T> {
  private Lazy<T> value;
  private int valueSchemaId;

  public LazyValueAndRmd(Lazy<T> value, @Nonnull GenericRecord rmd) {
    super(rmd);
    this.value = value;
    this.valueSchemaId = -1;
  }

  public T getValue() {
    return value.get();
  }

  public void setValue(T value) {
    this.value = Lazy.of(() -> value);
  }

  public void setValueSchemaId(int valueSchemaId) {
    this.valueSchemaId = valueSchemaId;
  }

  public int getValueSchemaId() {
    return this.valueSchemaId;
  }
}
