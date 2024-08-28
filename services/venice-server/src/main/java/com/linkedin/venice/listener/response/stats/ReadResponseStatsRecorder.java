package com.linkedin.venice.listener.response.stats;

import com.linkedin.venice.stats.ServerHttpRequestStats;


/**
 * This class is used to record stats associated with a read response.
 */
public interface ReadResponseStatsRecorder {
  void recordMetrics(ServerHttpRequestStats stats);

  /**
   * Merge the stats contained in this instance with those contained in the {@param other} instance.
   *
   * TODO: Figure out how to support the merging of the storage queue length / submission time stats...
   *
   * @param other instance to merge with.
   */
  void merge(ReadResponseStatsRecorder other);

  /**
   * This API is needed by {@link com.linkedin.venice.listener.response.ParallelMultiKeyResponseWrapper} in order to
   * properly account for the multiple submissions that happen during parallel batch get, and which cannot be collapsed
   * into a single metric via {@link #merge(ReadResponseStatsRecorder)}. After calling this function, the state should
   * be cleared.
   *
   * @return the storage execution submission wait time stored in this container
   */
  double getAndClearStorageExecutionSubmissionWaitTime();
}
