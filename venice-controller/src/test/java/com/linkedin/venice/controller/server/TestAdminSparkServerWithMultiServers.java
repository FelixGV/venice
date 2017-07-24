package com.linkedin.venice.controller.server;

import com.linkedin.venice.controllerapi.ControllerApiConstants;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.FlakyTestRetryAnalyzer;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.stats.Count;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Separate the tests from {@link TestAdminSparkServer}, because we need start more storage nodes to execute the
 * resource with bunch of partitions.
 * TODO: Since {@link TestAdminSparkServer} has adapted to have multi-servers. It's better to merge test cases.
 */
public class TestAdminSparkServerWithMultiServers {
  private VeniceClusterWrapper venice;
  private ControllerClient controllerClient;
  private final int numberOfServer = 6;

  @BeforeClass
  public void setUp() {
    venice = ServiceFactory.getVeniceCluster(1, numberOfServer, 1);
    controllerClient = new ControllerClient(venice.getClusterName(), venice.getRandomRouterURL());
  }

  @AfterClass
  public void tearDown() {
    controllerClient.close();
    venice.close();
  }

  /**
   * TODO: This test should be fixed. It is flaky, especially on a slow or heavily loaded machine.
   */
  @Test(retryAnalyzer = FlakyTestRetryAnalyzer.class)
  public void controllerClientShouldListStores() {
    List<String> storeNames = new ArrayList<>();
    for (int i = 0; i < 10; i++) { //add 10 stores;
      storeNames.add(Version.parseStoreFromKafkaTopicName(venice.getNewStoreVersion().getKafkaTopic()));
    }

    MultiStoreResponse storeResponse = controllerClient.queryStoreList();
    Assert.assertFalse(storeResponse.isError());
    List<String> returnedStoreNames = Arrays.asList(storeResponse.getStores());
    for (String expectedStore : storeNames) {
      Assert.assertTrue(returnedStoreNames.contains(expectedStore), "Query store list should include " + expectedStore);
    }
  }


  @Test
  public void requestTopicIsIdempotent() {
    String storeName = TestUtils.getUniqueString("store");
    String pushOne = TestUtils.getUniqueString("pushId");
    String pushTwo = TestUtils.getUniqueString("pushId");

    venice.getNewStore(storeName);
    VersionCreationResponse
        responseOneA = controllerClient.requestTopicForWrites(storeName, 1, ControllerApiConstants.PushType.BATCH, pushOne);
    if (responseOneA.isError()){
      Assert.fail(responseOneA.getError());
    }
    VersionCreationResponse responseTwo = controllerClient.requestTopicForWrites(storeName, 1, ControllerApiConstants.PushType.BATCH, pushTwo);
    if (responseTwo.isError()){
      Assert.fail(responseOneA.getError());
    }

    Assert.assertEquals(responseOneA.getKafkaTopic(), responseTwo.getKafkaTopic(), "Multiple requests for topics with the same pushId must return the same kafka topic");
  }

  /**
   * Multiple requests for a topic to write to for the same store.  Each request must provide the same version number
   * After the attempt, the version is made current so the next attempt generates a new version.
   * @throws InterruptedException
   */
  @Test
  public void requestTopicIsIdempotentWithConcurrency() throws InterruptedException {
    String storeName = TestUtils.getUniqueString("store");
    venice.getNewStore(storeName);
    AtomicReference<String> errString = new AtomicReference<>();
    try {
      for (int i = 0; i < 5; i++) { // number of attempts
        String pushId = TestUtils.getUniqueString("pushId");
        final List<VersionCreationResponse> responses = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        int threadCount = 3; // number of concurrent requests
        CountDownLatch latch = new CountDownLatch(threadCount);
        for (int j = 0; j < threadCount; j++) {
          Thread t = requestTopicThread(pushId, storeName, responses, latch, errString);
          threads.add(t);
          t.setUncaughtExceptionHandler((t1, e) -> e.printStackTrace());
        }
        for (Thread t : threads) {
          t.start();
        }
        latch.await(10, TimeUnit.SECONDS);
        for (int j = 0; j < threadCount; j++) {
          if (responses.get(j).isError()) {
            Assert.fail(responses.get(j).getError());
          }
        }
        for (int j = 1; j < threadCount; j++) {
          Assert.assertEquals(responses.get(0).getKafkaTopic(), responses.get(j).getKafkaTopic(),
              "Idempotent topic requests failed under concurrency on attempt " + i + ".  If this test ever fails, investigate! Don't just run it again and hope it passes");
        }
        //close the new version so the next iteration gets a new version.
        controllerClient.writeEndOfPush(storeName, responses.get(0).getVersion());
        while (controllerClient.getStore(storeName).getStore().getCurrentVersion() < responses.get(0).getVersion()) {
          Utils.sleep(200);
        }

      }
    } catch (Exception e){
      e.printStackTrace();
      System.err.println("Captured message: " + errString.get());
    }
  }

  private Thread requestTopicThread(String pushId, String storeName, List<VersionCreationResponse> output, CountDownLatch latch, AtomicReference<String> errString) {
    return new Thread(() -> {
      final VersionCreationResponse vcr = new VersionCreationResponse();
      try {
        VersionCreationResponse thisVcr = controllerClient.requestTopicForWrites(storeName, 1, ControllerApiConstants.PushType.BATCH, pushId);
        vcr.setKafkaTopic(thisVcr.getKafkaTopic());
        vcr.setVersion(thisVcr.getVersion());
      } catch (Throwable t) {
        errString.set(t.getMessage());
        vcr.setError(t.getMessage());
      } finally {
        output.add(vcr);
        latch.countDown();
      }
    });
  }

  @Test
  public void endOfPushEndpointTriggersVersionSwap(){
    String storeName = TestUtils.getUniqueString("store");
    String pushId = TestUtils.getUniqueString("pushId");
    venice.getNewStore(storeName);
    StoreResponse freshStore = controllerClient.getStore(storeName);
    int oldVersion = freshStore.getStore().getCurrentVersion();
    VersionCreationResponse versionResponse = controllerClient.requestTopicForWrites(storeName, 1, ControllerApiConstants.PushType.BATCH, pushId);
    int newVersion = versionResponse.getVersion();
    Assert.assertNotEquals(newVersion, oldVersion, "Requesting a new version must not return the current version number");
    controllerClient.writeEndOfPush(storeName, newVersion);
    TestUtils.waitForNonDeterministicAssertion(5, TimeUnit.SECONDS, () -> {
      StoreResponse storeResponse = controllerClient.getStore(storeName);
      Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), newVersion, "Writing end of push must flip the version to current");
    });
    controllerClient.close();
  }
}
