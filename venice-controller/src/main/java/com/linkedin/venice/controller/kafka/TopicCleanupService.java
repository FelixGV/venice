package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.kafka.TopicManager;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Time;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;


/**
 * The topic cleanup in Venice adopts the following strategy:
 * 1. When Controller needs to clean up topics for retired versions or uncompleted store pushes or store deletion, it only
 * truncates the topics (lower topic retention) instead of deleting them right away.
 * 2. The {@link TopicCleanupService} is working as a single process to clean up all the unused topics.
 * With this way, most of time (no mastership handover), there is only one controller talking to Kafka to delete topic, which is expected
 * from Kafka's perspective to avoid concurrent topic deletion.
 * In theory, it is still possible to have two controllers talking to Kafka to delete topic during mastership handover since
 * the previous master controller could be still working on the topic cleaning up but the new master controller starts
 * processing.
 *
 * If required, there might be several ways to alleviate this potential concurrent Kafka topic deletion:
 * 1. Do master controller check every time when deleting topic;
 * 2. Register a callback to monitor mastership change;
 * 3. Use a global Zookeeper lock;
 *
 * Right now, {@link TopicCleanupService} is fully decoupled from {@link com.linkedin.venice.meta.Store} since there is
 * only one process actively running to cleanup topics and the controller running this process may not be the master
 * controller of the cluster owning the store that the topic to be deleted belongs to.
 *
 *
 * Here is how {@link TopicCleanupService} works to clean up deprecated topics [topic with low retention policy]:
 * 1. This service is only running in master controller of controller cluster, which means there should be only one
 * topic cleanup service running among all the Venice clusters (not strictly considering master handover.);
 * 2. This service is running in a infinite loop, which will execute the following operations:
 *    2.1 For every round, check whether current controller is the master controller of controller parent.
 *        If yes, continue; Otherwise, sleep for a pre-configured period and check again;
 *    2.2 Collect all the topics and categorize them based on store names;
 *    2.3 For deprecated real-time topic, will remove it right away;
 *    2.4 For deprecated version topics, will keep pre-configured minimal unused topics to avoid MM crash and remove others;
 */
public class TopicCleanupService extends AbstractVeniceService {
  private static final Logger LOGGER = Logger.getLogger(TopicCleanupService.class);

  private final Admin admin;
  private final Thread cleanupThread;
  protected final long sleepIntervalBetweenTopicListFetchMs;
  protected final int delayFactor;
  private final int minNumberOfUnusedKafkaTopicsToPreserve;
  private AtomicBoolean stop = new AtomicBoolean(false);
  private boolean isMasterControllerOfControllerCluster = false;
  private long refreshQueueCycle = Time.MS_PER_MINUTE;
  protected final VeniceControllerMultiClusterConfig multiClusterConfigs;

  public TopicCleanupService(Admin admin, VeniceControllerMultiClusterConfig multiClusterConfigs) {
    this.admin = admin;
    this.sleepIntervalBetweenTopicListFetchMs = multiClusterConfigs.getTopicCleanupSleepIntervalBetweenTopicListFetchMs();
    this.delayFactor = multiClusterConfigs.getTopicCleanupDelayFactor();
    this.minNumberOfUnusedKafkaTopicsToPreserve = multiClusterConfigs.getMinNumberOfUnusedKafkaTopicsToPreserve();
    this.cleanupThread = new Thread(new TopicCleanupTask(), "TopicCleanupTask");
    this.multiClusterConfigs = multiClusterConfigs;
  }

  @Override
  public boolean startInner() throws Exception {
    cleanupThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    stop.set(true);
    cleanupThread.interrupt();
  }

  public TopicManager getTopicManager() {
    return admin.getTopicManager();
  }

  public TopicManager getTopicManager(Pair<String, String> kafkaBootstrapServersAndZkAddress) {
    return admin.getTopicManager(kafkaBootstrapServersAndZkAddress);
  }

  private class TopicCleanupTask implements Runnable {

    @Override
    public void run() {
      while (!stop.get()) {
        try {
          Thread.sleep(sleepIntervalBetweenTopicListFetchMs);
        } catch (InterruptedException e) {
          LOGGER.error("Received InterruptedException during sleep in TopicCleanup thread");
          break;
        }
        if (stop.get()) {
          break;
        }
        try {
          if (admin.isMasterControllerOfControllerCluster()) {
            if (!isMasterControllerOfControllerCluster) {
              /**
               * Sleep for some time when current controller firstly becomes master controller of controller cluster.
               * This is trying to avoid concurrent Kafka topic deletion sent by both previous master controller
               * (Kafka topic cleanup doesn't finish in a short period) and the new master controller.
               */
              isMasterControllerOfControllerCluster = true;
              LOGGER.info("Current controller becomes the master controller of controller cluster");
              continue;
            }
            cleanupVeniceTopics();
          } else {
            isMasterControllerOfControllerCluster = false;
          }
        } catch (Exception e) {
          LOGGER.error("Received exception when cleaning up topics", e);
        }
      }
      LOGGER.info("TopicCleanupTask stopped");
    }
  }

  protected Admin getAdmin() {
    return admin;
  }

  /**
   * The following will delete topics based on their priority. Real-time topics are given higher priority than version topics.
   * If version topic deletion takes more than certain time it refreshes the entire topic list and start deleting from RT topics again.
    */
  protected void cleanupVeniceTopics() {
    PriorityQueue<String> allTopics =  new PriorityQueue<>((s1, s2) -> Version.isRealTimeTopic(s1) ? -1 : 0);
    populateDeprecatedTopicQueue(allTopics);
    long refreshTime = System.currentTimeMillis();

    while (!allTopics.isEmpty()) {
      String topic = allTopics.poll();
      /**
       * Until now, we haven't figured out a good way to handle real-time topic cleanup:
       *     1. If {@link TopicCleanupService} doesn't delete real-time topic, the truncated real-time topic could cause inconsistent data problem
       *       between parent cluster and prod cluster if the deleted hybrid store gets re-created;
       *     2. If {@link TopicCleanupService} deletes the real-time topic, it might crash MM if application is still producing to the real-time topic
       *       in parent cluster;
       *
       *     Since Kafka nurse script will automatically kick in if MM crashes (which should still happen very infrequently),
       *     for the time being, we choose to delete the real-time topic.
       */
      try {
        try {
          /**
           * Best effort to clean up staled replica statuses from meta system store.
           */
          cleanupReplicaStatusesFromMetaSystemStore(topic);
        } catch (Exception e) {
          LOGGER.error("Received exception while trying to clean up replica statuses from meta system store for topic: "
              + topic + ", but topic deletion will continue");
        }
        getTopicManager().ensureTopicIsDeletedAndBlockWithRetry(topic);
      } catch (ExecutionException e) {
        LOGGER.warn("ExecutionException caught when trying to delete topic: " + topic, e);
        // No op, will try again in the next cleanup cycle.
      }

      if (!Version.isRealTimeTopic(topic)) {
       // If Version topic deletion took long time, skip further VT deletion and check if we have new RT topic to delete
        if (System.currentTimeMillis() - refreshTime > refreshQueueCycle) {
          allTopics.clear();
          populateDeprecatedTopicQueue(allTopics);
          if (allTopics.isEmpty()) {
            break;
          }
          refreshTime = System.currentTimeMillis();
        }
      }
    }
  }

  private void populateDeprecatedTopicQueue(PriorityQueue<String> topics) {
    Map<String, Map<String, Long>> allStoreTopics = getAllVeniceStoreTopicsRetentions(getTopicManager());
    allStoreTopics.forEach((storeName, topicRetentions) -> {
      String realTimeTopic = Version.composeRealTimeTopic(storeName);
      if (topicRetentions.containsKey(realTimeTopic)) {
        if (admin.isTopicTruncatedBasedOnRetention(topicRetentions.get(realTimeTopic))) {
          topics.offer(realTimeTopic);
        }
        topicRetentions.remove(realTimeTopic);
      }
      List<String> oldTopicsToDelete = extractVeniceTopicsToCleanup(admin, topicRetentions,
          minNumberOfUnusedKafkaTopicsToPreserve);
      if (!oldTopicsToDelete.isEmpty()) {
        topics.addAll(oldTopicsToDelete);
      }
    });
  }

  public static Map<String, Map<String, Long>> getAllVeniceStoreTopicsRetentions(TopicManager topicManager) {
    Map<String, Long> topicsWithRetention = topicManager.getAllTopicRetentions();
    Map<String, Map<String, Long>> allStoreTopics = new HashMap<>();

    for (Map.Entry<String, Long> entry : topicsWithRetention.entrySet()) {
      String topic = entry.getKey();
      long retention = entry.getValue();
      String storeName = Version.parseStoreFromKafkaTopicName(topic);
      if (storeName.isEmpty()) {
        // TODO: check whether Venice needs to cleanup topics not belonging to Venice.
        continue;
      }
      allStoreTopics.compute(storeName, (s, topics) -> {
        if (null == topics) {
          topics = new HashMap<>();
        }
        topics.put(topic, retention);
        return topics;
      });
    }
    return allStoreTopics;
  }

  public static List<String> extractVeniceTopicsToCleanup(Admin admin, Map<String, Long> topicRetentions,
      int minNumberOfUnusedKafkaTopicsToPreserve) {
    return extractVeniceTopicsToCleanup(admin, topicRetentions, minNumberOfUnusedKafkaTopicsToPreserve, true);
  }

  // TODO Remove the parameter preserveTopicsForDeletedStore once we move away from KMM. This parameter controls whether
  // to respect the minNumberOfUnusedKafkaTopicsToPreserve when store is deleted. We'd like to get ALL topics for a
  // deleted store regardless of minNumberOfUnusedKafkaTopicsToPreserve when queried from the controller endpoint by a
  // SRE/DEV but not when TopicCleanupService is fetching them for auto delete. This is because topic deletion is async
  // and we can/might crash the KMM if the topic in child fabric is deleted before the parent fabric while KMM is
  // performing copying messages.
  public static List<String> extractVeniceTopicsToCleanup(Admin admin, Map<String, Long> topicRetentions,
      int minNumberOfUnusedKafkaTopicsToPreserve, boolean preserveTopicsForDeletedStore) {
    if (topicRetentions.isEmpty()) {
      return Collections.emptyList();
    }
    Set<String> veniceTopics = topicRetentions.keySet();
    int maxVersion = veniceTopics.stream()
        .map(t -> Version.parseVersionFromKafkaTopicName(t))
        .max(Integer::compare)
        .get();

    String storeName = Version.parseStoreFromKafkaTopicName(veniceTopics.iterator().next());
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    boolean isStoreZkShared = systemStoreType != null && systemStoreType.isStoreZkShared();
    boolean isStoreDeleted = !preserveTopicsForDeletedStore && !isStoreZkShared && !admin.getStoreConfigRepo()
        .getStoreConfig(storeName).isPresent();
    // Do not preserve any VT for deleted user stores or zk shared system stores.
    // TODO revisit the behavior if we'd like to support rollback for zk shared system stores.
    final long maxVersionNumberToDelete = isStoreDeleted || isStoreZkShared ?
        maxVersion : maxVersion - minNumberOfUnusedKafkaTopicsToPreserve;

    return veniceTopics.stream()
        /** Consider only truncated topics */
        .filter(t -> admin.isTopicTruncatedBasedOnRetention(topicRetentions.get(t)))
        /** Always preserve the last {@link #minNumberOfUnusedKafkaTopicsToPreserve} topics, whether they are healthy or not */
        .filter(t -> Version.parseVersionFromKafkaTopicName(t) <= maxVersionNumberToDelete)
        /**
         * Filter out resources, which haven't been fully removed.
         *
         * The reason to filter out still-alive resource is to avoid triggering the non-existing topic issue
         * of Kafka consumer happening in Storage Node.
         *
         */
        .filter(t -> !admin.isResourceStillAlive(t))
        .collect(Collectors.toList());
  }

  /**
   * Clean up staled replica status from meta system store if necessary.
   * @param topic
   * @return whether the staled replica status cleanup happens or not.
   */
  protected boolean cleanupReplicaStatusesFromMetaSystemStore(String topic) {
    if (admin.isParent()) {
      // No op in Parent Controller
      return false;
    }
    if (!Version.isVersionTopic(topic)) {
      // Only applicable to version topic
      return false;
    }

    String storeName = Version.parseStoreFromKafkaTopicName(topic);
    int version = Version.parseVersionFromKafkaTopicName(topic);
    HelixReadOnlyStoreConfigRepository storeConfigRepository = admin.getStoreConfigRepo();
    Optional<StoreConfig> storeConfig = storeConfigRepository.getStoreConfig(storeName);
    // Get cluster name for current store
    if (!storeConfig.isPresent()) {
      throw new VeniceException("Failed to get store config for store: " + storeName);
    }
    /**
     * This logic won't take care of store migration scenarios properly since it will just look at the current cluster
     * when topic deletion happens.
     * But it is fine at this stage since a minor leaking of store replica statuses in meta system store is acceptable.
     * If the size of meta system store becomes unacceptable because of unknown issue, we could always do an empty push
     * to clean it up.
     */
    String clusterName = storeConfig.get().getCluster();
    /**
     * Check whether RT topic for the meta system store exists or not to decide whether we should clean replica statuses from meta System Store or not.
     * Since {@link TopicCleanupService} will be running in the master Controller of Controller Cluster, so
     * we won't have access to store repository for every Venice cluster, and the existence of RT topic for
     * meta System store is the way to check whether meta System store is enabled or not.
     */
    String rtTopicForMetaSystemStore = Version.composeRealTimeTopic(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName));
    TopicManager topicManager = getTopicManager();
    if (topicManager.containsTopic(rtTopicForMetaSystemStore)) {
      /**
       * Find out the total number of partition of version topic, and we will use this info to clean up replica statuses for each partition.
       */
      int partitionCount = topicManager.partitionsFor(topic).size();

      MetaStoreWriter metaStoreWriter = admin.getMetaStoreWriter();
      for (int i = 0; i < partitionCount; ++i) {
        metaStoreWriter.deleteStoreReplicaStatus(clusterName, storeName, version, i);
      }
      logger.info("Successfully removed store replica status from meta system store for store: " + storeName + ", version: "
          + version + " with partition count: " + partitionCount + " in cluster: " + clusterName);
      return true;
    }
    return false;
  }
}