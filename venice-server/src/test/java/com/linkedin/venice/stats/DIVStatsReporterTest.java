package com.linkedin.venice.stats;

import com.linkedin.venice.tehuti.MockTehutiReporter;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.metrics.MetricsRepository;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DIVStatsReporterTest {
  @Test
  public void testDIVReporterCanReport() {
    MetricsRepository metricsRepository = new MetricsRepository();
    MockTehutiReporter reporter = new MockTehutiReporter();
    metricsRepository.addReporter(reporter);

    String storeName = TestUtils.getUniqueString("store");
    DIVStatsReporter divStatsReporter = new DIVStatsReporter(metricsRepository, storeName);

    Assert.assertEquals(reporter.query("." + storeName + "--success_msg.DIVStatsCounter").value(), 0d);

    DIVStats stats = new DIVStats();
    stats.recordSuccessMsg();
    divStatsReporter.setDIVStats(stats);
    Assert.assertEquals(reporter.query("." + storeName + "--success_msg.DIVStatsCounter").value(), 1d);

    divStatsReporter.setDIVStats(null);
    Assert.assertEquals(reporter.query("." + storeName + "--success_msg.DIVStatsCounter").value(), 0d);
  }
}
