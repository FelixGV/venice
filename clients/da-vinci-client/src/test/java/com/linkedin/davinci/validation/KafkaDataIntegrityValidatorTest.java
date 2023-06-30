package com.linkedin.davinci.validation;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.*;

import com.linkedin.venice.exceptions.validation.ImproperlyStartedSegmentException;
import com.linkedin.venice.exceptions.validation.MissingDataException;
import com.linkedin.venice.guid.GuidUtils;
import com.linkedin.venice.kafka.protocol.ControlMessage;
import com.linkedin.venice.kafka.protocol.GUID;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.LeaderMetadata;
import com.linkedin.venice.kafka.protocol.ProducerMetadata;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.StartOfSegment;
import com.linkedin.venice.kafka.protocol.enums.ControlMessageType;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.kafka.validation.Segment;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.ImmutablePubSubMessage;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.serialization.KafkaKeySerializer;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaDataIntegrityValidatorTest {
  private static final PubSubTopicRepository pubSubTopicRepository = new PubSubTopicRepository();

  @Test
  public void testClearExpiredState() throws InterruptedException {
    final long maxAgeInMs = 1000;
    Time time = new TestMockTime();
    String kafkaTopic = Utils.getUniqueString("TestStore") + "_v1";
    KafkaDataIntegrityValidator validator =
        new KafkaDataIntegrityValidator(kafkaTopic, KafkaDataIntegrityValidator.DISABLED, maxAgeInMs, time);

    GUID producerGUID = GuidUtils.getGUID(VeniceProperties.empty());
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record1 =
        buildSoSConsumerRecord(kafkaTopic, 0, 100, producerGUID, 0, time.getMilliseconds());
    validator.validateMessage(record1, false, Lazy.FALSE);

    time.sleep(10);

    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record2 =
        buildConsumerRecord(kafkaTopic, 0, 101, producerGUID, 0, 1, time.getMilliseconds());
    validator.validateMessage(record2, false, Lazy.FALSE);

    ProducerTracker producerTracker = validator.registerProducer(producerGUID);
    assertEquals(producerTracker.getNumberOfTrackedPartitions(), 1);

    // Nothing should be cleared yet
    validator.clearExpiredState();
    assertEquals(producerTracker.getNumberOfTrackedPartitions(), 1);

    // Even if we wait some more, the max age is not yet reached, so the state should still be retained
    time.sleep(maxAgeInMs / 2);
    validator.clearExpiredState();
    assertEquals(producerTracker.getNumberOfTrackedPartitions(), 1);

    // At exactly the max age, clearing should still not kick in
    time.sleep(maxAgeInMs / 2);
    validator.clearExpiredState();
    assertEquals(producerTracker.getNumberOfTrackedPartitions(), 1);

    // Finally, after the max age is exceeded, it should clear
    time.sleep(1);
    validator.clearExpiredState();
    assertEquals(producerTracker.getNumberOfTrackedPartitions(), 0);

    // If, somehow, a message still came for this producer GUID after clearing the state, DIV should catch it
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record3 =
        buildConsumerRecord(kafkaTopic, 0, 102, producerGUID, 0, 2, time.getMilliseconds());
    assertThrows(ImproperlyStartedSegmentException.class, () -> validator.validateMessage(record3, false, Lazy.FALSE));
  }

  @Test
  public void testStatelessDIV() {
    String kafkaTopic = Utils.getUniqueString("TestStore") + "_v1";
    long kafkaLogCompactionLagInMs = TimeUnit.HOURS.toMillis(24); // 24 hours
    KafkaDataIntegrityValidator stateLessDIVValidator =
        new KafkaDataIntegrityValidator(kafkaTopic, kafkaLogCompactionLagInMs);

    /**
     * Create a record that starts in the middle of a segment with sequence number 100 and the broken timestamp for this
     * record is 28 hours ago.
     */
    GUID producerGUID = GuidUtils.getGUID(VeniceProperties.empty());
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record = buildConsumerRecord(
        kafkaTopic,
        0,
        100,
        producerGUID,
        0,
        100,
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(28));

    // Stateless DIV will allow a segment starts in the middle
    stateLessDIVValidator.checkMissingMessage(record, Optional.empty());

    /**
     * Create a record with sequence number 101 in the same segment and the broken timestamp for this record is 27 hours
     * ago; no error should be thrown since sequence number is incrementing without gap.
     */
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record2 = buildConsumerRecord(
        kafkaTopic,
        0,
        101,
        producerGUID,
        0,
        101,
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(27));
    stateLessDIVValidator.checkMissingMessage(record2, Optional.empty());

    /**
     * Create a record with sequence number 103 in the same segment and the broken timestamp for this record is 20 hours
     * ago; there is a gap between sequence number 101 and 103; however, since the previous record is older than the
     * log compaction delay threshold (24 hours), missing message is allowed.
     */
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record3 = buildConsumerRecord(
        kafkaTopic,
        0,
        200,
        producerGUID,
        0,
        103,
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(20));
    stateLessDIVValidator.checkMissingMessage(record3, Optional.empty());

    /**
     * Create a record with sequence number 105 in the same segment and the broken timestamp for this record is 10 hours
     * ago; there is a gap between sequence number 103 and 105; MISSING_MESSAGE exception should be thrown this time
     * because the previous message for the same segment is fresh (20 hours ago), Kafka log compaction hasn't started
     * yet, so missing message is not expected
     */
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record4 = buildConsumerRecord(
        kafkaTopic,
        0,
        205,
        producerGUID,
        0,
        105,
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(10));
    Assert.assertThrows(
        MissingDataException.class,
        () -> stateLessDIVValidator.checkMissingMessage(record4, Optional.empty()));

    ProducerTracker.DIVErrorMetricCallback errorMetricCallback = mock(ProducerTracker.DIVErrorMetricCallback.class);

    /**
     * Create a record with a gap in segment number. MISSING_MESSAGE exception should be thrown
     */
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> record5 = buildConsumerRecord(
        kafkaTopic,
        0,
        206,
        producerGUID,
        2,
        1,
        System.currentTimeMillis() - TimeUnit.HOURS.toMillis(10));
    Assert.assertThrows(
        MissingDataException.class,
        () -> stateLessDIVValidator.checkMissingMessage(record5, Optional.of(errorMetricCallback)));
    verify(errorMetricCallback, times(1)).execute(any());
  }

  private static PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> buildConsumerRecord(
      String kafkaTopic,
      int partition,
      long offset,
      GUID producerGUID,
      int segmentNumber,
      int sequenceNumber,
      long brokerTimestamp) {
    KafkaKeySerializer kafkaKeySerializer = new KafkaKeySerializer();
    KafkaKey kafkaKey = kafkaKeySerializer.deserialize(kafkaTopic, "key".getBytes());
    KafkaMessageEnvelope messageEnvelope = new KafkaMessageEnvelope();
    messageEnvelope.messageType = MessageType.PUT.getValue();

    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.producerGUID = producerGUID;
    Segment currentSegment = new Segment(partition, segmentNumber, CheckSumType.MD5);
    currentSegment.setSequenceNumber(sequenceNumber);
    producerMetadata.segmentNumber = currentSegment.getSegmentNumber();
    producerMetadata.messageSequenceNumber = currentSegment.getSequenceNumber();
    producerMetadata.messageTimestamp = brokerTimestamp;
    messageEnvelope.producerMetadata = producerMetadata;
    messageEnvelope.leaderMetadataFooter = new LeaderMetadata();
    messageEnvelope.leaderMetadataFooter.upstreamOffset = -1;

    Put putPayload = new Put();
    putPayload.putValue = ByteBuffer.wrap("value".getBytes());
    putPayload.schemaId = 0;
    putPayload.replicationMetadataVersionId = VeniceWriter.VENICE_DEFAULT_TIMESTAMP_METADATA_VERSION_ID;
    putPayload.replicationMetadataPayload = ByteBuffer.wrap(new byte[0]);
    messageEnvelope.payloadUnion = putPayload;

    return new ImmutablePubSubMessage<>(
        kafkaKey,
        messageEnvelope,
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(kafkaTopic), partition),
        offset,
        brokerTimestamp,
        0);
  }

  private static PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> buildSoSConsumerRecord(
      String kafkaTopic,
      int partition,
      long offset,
      GUID producerGUID,
      int segmentNumber,
      long brokerTimestamp) {
    KafkaKeySerializer kafkaKeySerializer = new KafkaKeySerializer();
    KafkaKey kafkaKey = kafkaKeySerializer.deserialize(kafkaTopic, "key".getBytes());
    KafkaMessageEnvelope messageEnvelope = new KafkaMessageEnvelope();
    messageEnvelope.messageType = MessageType.CONTROL_MESSAGE.getValue();

    ProducerMetadata producerMetadata = new ProducerMetadata();
    producerMetadata.producerGUID = producerGUID;
    producerMetadata.segmentNumber = segmentNumber;
    producerMetadata.messageSequenceNumber = 0;
    producerMetadata.messageTimestamp = brokerTimestamp;
    messageEnvelope.producerMetadata = producerMetadata;
    messageEnvelope.leaderMetadataFooter = new LeaderMetadata();
    messageEnvelope.leaderMetadataFooter.upstreamOffset = -1;

    ControlMessage controlMessage = new ControlMessage();
    controlMessage.controlMessageType = ControlMessageType.START_OF_SEGMENT.getValue();
    StartOfSegment startOfSegment = new StartOfSegment();
    startOfSegment.checksumType = CheckSumType.MD5.getValue();
    startOfSegment.upcomingAggregates = Collections.emptyList();
    controlMessage.controlMessageUnion = startOfSegment;
    messageEnvelope.payloadUnion = controlMessage;

    return new ImmutablePubSubMessage<>(
        kafkaKey,
        messageEnvelope,
        new PubSubTopicPartitionImpl(pubSubTopicRepository.getTopic(kafkaTopic), partition),
        offset,
        brokerTimestamp,
        0);
  }
}
