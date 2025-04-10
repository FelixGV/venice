package com.linkedin.venice.datarecovery;

import static com.linkedin.davinci.kafka.consumer.KafkaConsumerService.ConsumerAssignmentStrategy;

import org.testng.annotations.Test;


@Test
public class TWiseHMChannelDataRecoveryTest extends DataRecoveryTest {
  @Override
  protected ConsumerAssignmentStrategy getConsumerAssignmentStrategy() {
    return ConsumerAssignmentStrategy.TOPIC_WISE_SHARED_CONSUMER_ASSIGNMENT_STRATEGY;
  }

  @Override
  protected boolean useParticipantStore() {
    return false;
  }
}
