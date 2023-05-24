package com.linkedin.venice.io;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;


/**
 * A subclass of {@link ByteArrayOutputStream} which adds support for writing data from a {@link ByteBuffer}
 *
 * The private functions are copied from the parent class, since their visibility prevents us from calling them.
 */
public class ByteOutputStream extends ByteArrayOutputStream {
  public void write(ByteBuffer buffer) {
    int amountToGetFromBuffer = buffer.remaining();
    ensureCapacity(super.count + amountToGetFromBuffer);
    buffer.get(super.buf, super.count, amountToGetFromBuffer);
    super.count += amountToGetFromBuffer;
  }

  /**
   * Increases the capacity if necessary to ensure that it can hold
   * at least the number of elements specified by the minimum
   * capacity argument.
   *
   * @param  minCapacity the desired minimum capacity
   * @throws OutOfMemoryError if {@code minCapacity < 0}.  This is
   * interpreted as a request for the unsatisfiably large capacity
   * {@code (long) Integer.MAX_VALUE + (minCapacity - Integer.MAX_VALUE)}.
   */
  private void ensureCapacity(int minCapacity) {
    // overflow-conscious code
    if (minCapacity - buf.length > 0)
      grow(minCapacity);
  }

  /**
   * The maximum size of array to allocate.
   * Some VMs reserve some header words in an array.
   * Attempts to allocate larger arrays may result in
   * OutOfMemoryError: Requested array size exceeds VM limit
   */
  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  /**
   * Increases the capacity to ensure that it can hold at least the
   * number of elements specified by the minimum capacity argument.
   *
   * @param minCapacity the desired minimum capacity
   */
  private void grow(int minCapacity) {
    // overflow-conscious code
    int oldCapacity = buf.length;
    int newCapacity = oldCapacity << 1;
    if (newCapacity - minCapacity < 0)
      newCapacity = minCapacity;
    if (newCapacity - MAX_ARRAY_SIZE > 0)
      newCapacity = hugeCapacity(minCapacity);
    buf = Arrays.copyOf(buf, newCapacity);
  }

  private static int hugeCapacity(int minCapacity) {
    if (minCapacity < 0) // overflow
      throw new OutOfMemoryError();
    return (minCapacity > MAX_ARRAY_SIZE) ? Integer.MAX_VALUE : MAX_ARRAY_SIZE;
  }
}
