package com.linkedin.davinci.schema.merge;

import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;


public abstract class ValueAndRmd<T> {
  private GenericRecord rmd;
  private boolean updateIgnored; // Whether we should skip the incoming message since it could be a stale message.

  public ValueAndRmd(@Nonnull GenericRecord rmd) {
    this.rmd = Objects.requireNonNull(rmd);
  }

  public abstract T getValue();

  public GenericRecord getRmd() {
    return this.rmd;
  }

  public void setUpdateIgnored(boolean updateIgnored) {
    this.updateIgnored = updateIgnored;
  }

  public boolean isUpdateIgnored() {
    return this.updateIgnored;
  }

  public abstract int getValueSchemaId();
}
