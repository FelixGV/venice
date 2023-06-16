package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.collections.ArrayCollection;
import java.util.Arrays;
import java.util.Collection;


/**
 * This class represent the assignments of one resource including all of assigned partitions and expected number of
 * partitions.
 */
public class PartitionAssignment {
  private static final long NOT_DELETED = -1;
  private static final long LAG_TIME_FOR_DELETION = 5 * Time.MS_PER_MINUTE;

  private final String topic;
  private final Partition[] partitionsArrayIndexedById;
  private final ArrayCollection<Partition> partitionCollection;
  private final Time time;
  private int populatedSize = 0;

  private long deletionTimestamp = NOT_DELETED;

  public PartitionAssignment(String topic, int numberOfPartition) {
    this(topic, numberOfPartition, SystemTime.INSTANCE);
  }

  public PartitionAssignment(String topic, int numberOfPartition, Time time) {
    this.topic = topic;
    if (numberOfPartition <= 0) {
      throw new VeniceException(
          "Expected number of partition should be larger than 0 for resource '" + topic + "'. Current value:"
              + numberOfPartition);
    }
    this.partitionsArrayIndexedById = new Partition[numberOfPartition];
    this.partitionCollection = new ArrayCollection<>(this.partitionsArrayIndexedById, () -> this.populatedSize);
    this.time = time;
  }

  public Partition getPartition(int partitionId) {
    return this.partitionsArrayIndexedById[partitionId];
  }

  public void addPartition(Partition partition) {
    if (partition.getId() < 0 || partition.getId() >= getExpectedNumberOfPartitions()) {
      throw new VeniceException(
          "Invalid Partition id:" + partition.getId() + ". Partition id should be in the range of [0,"
              + getExpectedNumberOfPartitions() + "]");
    }
    if (this.partitionsArrayIndexedById[partition.getId()] == null) {
      this.populatedSize++;
    }
    this.partitionsArrayIndexedById[partition.getId()] = partition;
  }

  public Collection<Partition> getAllPartitions() {
    return this.partitionCollection;
  }

  public int getExpectedNumberOfPartitions() {
    return this.partitionsArrayIndexedById.length;
  }

  public int getAssignedNumberOfPartitions() {
    return populatedSize;
  }

  public boolean isMissingAssignedPartitions() {
    return getAssignedNumberOfPartitions() < getExpectedNumberOfPartitions();
  }

  public String getTopic() {
    return topic;
  }

  /**
   * Will set the deletionTimestamp to the current time only if it has not been set yet.
   */
  public void initializeDeletionTimestamp() {
    if (this.deletionTimestamp == NOT_DELETED) {
      this.deletionTimestamp = time.getMilliseconds();
    }
  }

  public long getDeletionTimestamp() {
    return this.deletionTimestamp;
  }

  public boolean isMarkedForDeletion() {
    return this.deletionTimestamp > NOT_DELETED;
  }

  public long secondsSinceDeletion() {
    if (isMarkedForDeletion()) {
      return (time.getMilliseconds() - this.deletionTimestamp) / Time.MS_PER_SECOND;
    }
    return 0;
  }

  public boolean eligibleForDeletion() {
    return isMarkedForDeletion() && getDeletionTimestamp() < time.getMilliseconds() - LAG_TIME_FOR_DELETION;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " {" + "\n\ttopic: " + topic + ", " + "\n\texpectedNumberOfPartitions: "
        + getExpectedNumberOfPartitions() + ", " + "\n\tpartitionsArrayIndexedById: "
        + Arrays.toString(this.partitionsArrayIndexedById) + "\n}";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof PartitionAssignment) {
      return Arrays.equals(partitionsArrayIndexedById, ((PartitionAssignment) obj).partitionsArrayIndexedById);
    }

    return false;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + topic.hashCode();
    result = 31 * result + Arrays.hashCode(this.partitionsArrayIndexedById);
    result = 31 * result + getExpectedNumberOfPartitions();
    return result;
  }
}
