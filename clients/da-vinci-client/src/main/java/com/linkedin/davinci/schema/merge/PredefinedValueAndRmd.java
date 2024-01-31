package com.linkedin.davinci.schema.merge;

import org.apache.avro.generic.GenericRecord;


public class PredefinedValueAndRmd<T> extends ValueAndRmd<T> {
  private final T value;
  private final int valueSchemaId;

  public PredefinedValueAndRmd(T value, int valueSchemaId, GenericRecord rmd) {
    super(rmd);
    this.value = value;
    this.valueSchemaId = valueSchemaId;
  }

  @Override
  public T getValue() {
    return this.value;
  }

  @Override
  public int getValueSchemaId() {
    return this.valueSchemaId;
  }
}
