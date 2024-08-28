package com.linkedin.venice.listener.response.stats;

import com.linkedin.venice.stats.ServerHttpRequestStats;
import com.linkedin.venice.stats.StatsUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;


public class ComputeResponseStatsWithSizeProfiling extends ComputeResponseStats {
  private final IntList keySizes;
  private final IntList valueSizes;

  public ComputeResponseStatsWithSizeProfiling(int maxKeyCount) {
    this.keySizes = new IntArrayList(maxKeyCount);
    this.valueSizes = new IntArrayList(maxKeyCount);
  }

  @Override
  public void addKeySize(int size) {
    this.keySizes.add(size);
  }

  @Override
  public void addValueSize(int size) {
    super.addValueSize(size);
    this.valueSizes.add(size);
  }

  @Override
  public void recordMetrics(ServerHttpRequestStats stats) {
    super.recordMetrics(stats);

    for (int i = 0; i < valueSizes.size(); i++) {
      StatsUtils.consumeIntIfAbove(stats::recordValueSizeInByte, valueSizes.getInt(i), 0);
    }
    for (int i = 0; i < keySizes.size(); i++) {
      stats.recordKeySizeInByte(keySizes.getInt(i));
    }
  }

  @Override
  public void merge(ReadResponseStatsRecorder other) {
    super.merge(other);
    if (other instanceof ComputeResponseStatsWithSizeProfiling) {
      ComputeResponseStatsWithSizeProfiling otherStats = (ComputeResponseStatsWithSizeProfiling) other;
      this.keySizes.addAll(otherStats.keySizes);
      this.valueSizes.addAll(otherStats.valueSizes);
    }
  }
}
