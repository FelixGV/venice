package com.linkedin.venice.integration.utils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.concurrent.TimeUnit;


public class ParticipantStoreUtils {
  /**
   * Participant store should be set up by child controller.
   */
  public static void verifyParticipantMessageStoreSetup(
      Admin veniceAdmin,
      String clusterName,
      PubSubTopicRepository pubSubTopicRepository) {
    TopicManager topicManager = veniceAdmin.getTopicManager();
    String participantStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName);
    PubSubTopic participantStoreRt = pubSubTopicRepository.getTopic(Utils.composeRealTimeTopic(participantStoreName));
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      Store store = veniceAdmin.getStore(clusterName, participantStoreName);
      assertNotNull(store);
      assertEquals(store.getVersions().size(), 1);
      assertEquals(store.getCurrentVersion(), 1);
    });
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
      assertTrue(topicManager.containsTopic(participantStoreRt));
    });
  }
}
