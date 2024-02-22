package com.linkedin.venice.controller;

import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.assertNodeIsNotRemovable;
import static com.linkedin.venice.utils.TestUtils.assertNodeIsRemovable;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestInstanceRemovable {
  private VeniceClusterWrapper cluster;
  int partitionSize = 1000;
  int replicaFactor = 3;
  int numberOfServer = 3;

  @BeforeMethod
  public void setUp() {
    int numberOfController = 1;

    int numberOfRouter = 1;

    cluster = ServiceFactory.getVeniceCluster(
        numberOfController,
        numberOfServer,
        numberOfRouter,
        replicaFactor,
        partitionSize,
        false,
        false);
  }

  @AfterMethod
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  @Test(timeOut = 120 * Time.MS_PER_SECOND)
  public void testIsInstanceRemovableDuringPush() {
    String clusterName = cluster.getClusterName();

    // First, we ensure that the participant store is fully setup...
    // cluster.getLeaderVeniceController().getVeniceAdmin().; // TODO: Bump up PS RF to 4? Or else delete the PS?
    String participantStoreName = VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName);
    cluster.useControllerClient(controllerClient -> waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
      StoreResponse storeResponse = assertCommand(controllerClient.getStore(participantStoreName));
      assertNotNull(storeResponse.getStore());
      assertCommand(
          controllerClient.updateStore(
              participantStoreName,
              new UpdateStoreQueryParams().setEnableReads(false).setEnableWrites(false)));
      // TrackableControllerResponse deleteStoreResponse =
      assertCommand(controllerClient.deleteStore(participantStoreName));
      // AdminCommandExecutionResponse adminCommandExecutionResponse;
      //
      // while (true) {
      // adminCommandExecutionResponse =
      // controllerClient.getAdminCommandExecution(deleteStoreResponse.getExecutionId());
      // if (!adminCommandExecutionResponse.isError()
      // && adminCommandExecutionResponse.getExecution().isSucceedInAllFabric()) {
      // break;
      // }
      // }
    }));
    // String participantStoreVersionTopicName = Version.composeKafkaTopic(participantStoreName, 1);
    // TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
    // try {
    // assertEquals(
    // cluster.getLeaderVeniceController()
    // .getVeniceAdmin()
    // .getOffLinePushStatus(clusterName, participantStoreVersionTopicName)
    // .getExecutionStatus(),
    // ExecutionStatus.COMPLETED);
    // } catch (VeniceNoStoreException e) {
    // fail("The participant store still does not exist: " + participantStoreName, e);
    // }
    // });

    String storeName = Utils.getUniqueString("testIsInstanceRemovableDuringPush");
    int partitionCount = 1;
    long storageQuota = (long) partitionCount * partitionSize;

    cluster.getNewStore(storeName);
    cluster.updateStore(
        storeName,
        new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota).setReplicationFactor(replicaFactor - 1));

    VersionCreationResponse response = TestUtils.assertCommand(cluster.getNewVersion(storeName));
    String topicName = response.getKafkaTopic();

    try (VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName)) {
      veniceWriter.broadcastStartOfPush(new HashMap<>());

      TestUtils.waitForNonDeterministicAssertion(
          3,
          TimeUnit.SECONDS,
          () -> assertEquals(
              cluster.getLeaderVeniceController()
                  .getVeniceAdmin()
                  .getOffLinePushStatus(clusterName, topicName)
                  .getExecutionStatus(),
              ExecutionStatus.STARTED));

      // All of replica in BOOTSTRAP
      String urls = cluster.getAllControllersURLs();
      int serverPort1 = cluster.getVeniceServers().get(0).getPort();
      int serverPort2 = cluster.getVeniceServers().get(1).getPort();
      int serverPort3 = cluster.getVeniceServers().get(2).getPort();
      String helixNodeId1 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1);
      String helixNodeId2 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2);
      String helixNodeId3 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort3);

      try (ControllerClient client = new ControllerClient(clusterName, urls)) {
        assertNodeIsRemovable(client.isNodeRemovable(helixNodeId1));
        assertNodeIsRemovable(client.isNodeRemovable(helixNodeId2));
        assertNodeIsRemovable(client.isNodeRemovable(helixNodeId3));

        /*
         * We test all permutations of removing one node and locking one other node. With this, we should hit the
         * case where all 2 partition-replicas of the user's store are removed, and that should be fine since the
         * user's store only has an in-flight push and no current versions yet. We need to keep one other replica
         * in play since the Participant's store needs to have at least one of its 3 replicas still online...
         */
        assertNodeIsRemovable(client.isNodeRemovable(helixNodeId1, Arrays.asList(helixNodeId2)));
        assertNodeIsRemovable(client.isNodeRemovable(helixNodeId1, Arrays.asList(helixNodeId3)));
        assertNodeIsRemovable(client.isNodeRemovable(helixNodeId2, Arrays.asList(helixNodeId1)));
        assertNodeIsRemovable(client.isNodeRemovable(helixNodeId2, Arrays.asList(helixNodeId3)));
        assertNodeIsRemovable(client.isNodeRemovable(helixNodeId3, Arrays.asList(helixNodeId1)));
        assertNodeIsRemovable(client.isNodeRemovable(helixNodeId3, Arrays.asList(helixNodeId2)));

        // choose a server (which contains a replica of the user's store) to stop during the push
        Set<String> replicaNodeIds = cluster.getLeaderVeniceController()
            .getVeniceAdmin()
            .getReplicas(clusterName, topicName)
            .stream()
            .map(replica -> replica.getInstance().getNodeId())
            .collect(Collectors.toSet());
        List<VeniceServerWrapper> servers = cluster.getVeniceServers();
        List<VeniceServerWrapper> serversWithUserStore = servers.stream()
            .filter(server -> replicaNodeIds.contains(server.getHelixNodeId()))
            .collect(Collectors.toList());
        assertEquals(serversWithUserStore.size(), 2);
        VeniceServerWrapper chosenServer = servers.get(0);

        cluster.stopVeniceServer(chosenServer.getPort());
        int expectedNumberOfReplicasRemainingAfterStoppingOneServer = (replicaFactor - 1) * partitionCount;
        TestUtils.waitForNonDeterministicAssertion(
            3,
            TimeUnit.SECONDS,
            () -> assertEquals(
                cluster.getLeaderVeniceController().getVeniceAdmin().getReplicas(clusterName, topicName).size(),
                expectedNumberOfReplicasRemainingAfterStoppingOneServer));

        // could remove any one node from the rest as well
        VeniceServerWrapper otherServerWithUserStore = servers.get(1);
        VeniceServerWrapper serverWithoutUserStore = servers.stream()
            .filter(server -> !replicaNodeIds.contains(server.getHelixNodeId()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("There must be a node which does not hosts the user's store"));
        assertNodeIsRemovable(client.isNodeRemovable(otherServerWithUserStore.getHelixNodeId()));
        assertNodeIsRemovable(client.isNodeRemovable(serverWithoutUserStore.getHelixNodeId()));

        // stop one more server
        cluster.stopVeniceServer(otherServerWithUserStore.getPort());
        int expectedNumberOfReplicasRemainingAfterStoppingTwoServers = (replicaFactor - 2) * partitionCount;
        TestUtils.waitForNonDeterministicAssertion(
            3,
            TimeUnit.SECONDS,
            () -> assertEquals(
                cluster.getLeaderVeniceController().getVeniceAdmin().getReplicas(clusterName, topicName).size(),
                expectedNumberOfReplicasRemainingAfterStoppingTwoServers));

        // The last server should not be removable due to the Participant Store.
        // assertNodeIsNotRemovable(client.isNodeRemovable(serverWithoutUserStore.getHelixNodeId()));

        // Add back two new servers and the ingestion should still be able to complete.
        cluster.addVeniceServer(new Properties(), new Properties());
        cluster.addVeniceServer(new Properties(), new Properties());
        veniceWriter.broadcastEndOfPush(new HashMap<>());
        TestUtils.waitForNonDeterministicAssertion(
            3,
            TimeUnit.SECONDS,
            () -> assertEquals(
                cluster.getLeaderVeniceController()
                    .getVeniceAdmin()
                    .getOffLinePushStatus(clusterName, topicName)
                    .getExecutionStatus(),
                ExecutionStatus.COMPLETED));
      }
    }
  }

  @Test(timeOut = 60 * Time.MS_PER_SECOND)
  public void testIsInstanceRemovableAfterPush() {
    String storeName = Utils.getUniqueString("testIsInstanceRemovableAfterPush");
    int partitionCount = 2;
    long storageQuota = (long) partitionCount * partitionSize;

    cluster.getNewStore(storeName);
    cluster.updateStore(storeName, new UpdateStoreQueryParams().setStorageQuotaInByte(storageQuota));

    VersionCreationResponse response = assertCommand(cluster.getNewVersion(storeName));
    String topicName = response.getKafkaTopic();

    try (VeniceWriter<String, String, byte[]> veniceWriter = cluster.getVeniceWriter(topicName)) {
      veniceWriter.broadcastStartOfPush(new HashMap<>());
      veniceWriter.put("test", "test", 1);
      veniceWriter.broadcastEndOfPush(new HashMap<>());
    }

    // Wait push completed.
    TestUtils.waitForNonDeterministicAssertion(
        3,
        TimeUnit.SECONDS,
        () -> assertEquals(
            cluster.getLeaderVeniceController()
                .getVeniceAdmin()
                .getOffLinePushStatus(cluster.getClusterName(), topicName)
                .getExecutionStatus(),
            ExecutionStatus.COMPLETED));

    String clusterName = cluster.getClusterName();
    String urls = cluster.getAllControllersURLs();
    int serverPort1 = cluster.getVeniceServers().get(0).getPort();
    int serverPort2 = cluster.getVeniceServers().get(1).getPort();
    int serverPort3 = cluster.getVeniceServers().get(2).getPort();
    String helixNodeId1 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort1);
    String helixNodeId2 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort2);
    String helixNodeId3 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort3);

    /*
     * This is the same scenario as we would do later in the following test steps.
     * 1. Host serverPort1 is removable.
     * 2. If serverPort1 were stopped, host serverPort2 is non-removable.
     * 3. If serverPort1 were stopped, host serverPort3 is non-removable.
     */
    try (ControllerClient client = new ControllerClient(clusterName, urls)) {
      assertNodeIsRemovable(client.isNodeRemovable(helixNodeId1));
      assertNodeIsNotRemovable(client.isNodeRemovable(helixNodeId2, Arrays.asList(helixNodeId1)));
      assertNodeIsNotRemovable(client.isNodeRemovable(helixNodeId3, Arrays.asList(helixNodeId1)));
    }

    cluster.stopVeniceServer(serverPort1);
    TestUtils.waitForNonDeterministicAssertion(
        3,
        TimeUnit.SECONDS,
        () -> assertEquals(
            cluster.getLeaderVeniceController().getVeniceAdmin().getReplicas(clusterName, topicName).size(),
            4));
    // Can not remove node cause, it will trigger re-balance.
    try (ControllerClient client = new ControllerClient(clusterName, urls)) {
      assertNodeIsRemovable(client.isNodeRemovable(helixNodeId1));
      assertNodeIsNotRemovable(client.isNodeRemovable(helixNodeId2));
      assertNodeIsNotRemovable(client.isNodeRemovable(helixNodeId3));

      VeniceServerWrapper newServer = cluster.addVeniceServer(false, false);
      int serverPort4 = newServer.getPort();
      String helixNodeId4 = Utils.getHelixNodeIdentifier(Utils.getHostName(), serverPort4);
      TestUtils.waitForNonDeterministicAssertion(
          10,
          TimeUnit.SECONDS,
          true,
          () -> assertEquals(
              cluster.getLeaderVeniceController()
                  .getVeniceAdmin()
                  .getReplicasOfStorageNode(clusterName, helixNodeId4)
                  .stream()
                  /** N.B.: This filtering is necessary otherwise the Participant Store can get counted... */
                  .filter(replica -> replica.getResource().equals(topicName))
                  .count(),
              partitionCount));

      // After replica number back to 3, all of node could be removed.
      assertNodeIsRemovable(client.isNodeRemovable(helixNodeId2));
      assertNodeIsRemovable(client.isNodeRemovable(helixNodeId3));
      assertNodeIsRemovable(client.isNodeRemovable(helixNodeId4));

      // After adding a new server, all servers are removable, even serverPort1 is still stopped.
      assertNodeIsRemovable(client.isNodeRemovable(helixNodeId2, Arrays.asList(helixNodeId1)));
      assertNodeIsRemovable(client.isNodeRemovable(helixNodeId3, Arrays.asList(helixNodeId1)));
      assertNodeIsRemovable(client.isNodeRemovable(helixNodeId4, Arrays.asList(helixNodeId1)));

      // Test if all instances of a partition are removed via a combination of locked instances and the requested node.
      assertNodeIsNotRemovable(
          client.isNodeRemovable(helixNodeId4, Arrays.asList(helixNodeId1, helixNodeId2, helixNodeId3)));
    }
  }
}
