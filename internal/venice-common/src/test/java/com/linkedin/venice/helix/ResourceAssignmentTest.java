package com.linkedin.venice.helix;

import static com.linkedin.venice.helix.ResourceAssignment.ResourceAssignmentChanges;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.TestMockTime;
import com.linkedin.venice.utils.Time;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ResourceAssignmentTest {
  @Test
  public void testAddAndGetPartitionAssignment() {
    String resource = "test";
    ResourceAssignment resourceAssignment = new ResourceAssignment();
    PartitionAssignment partitionAssignment = new PartitionAssignment(resource, 1);
    resourceAssignment.setPartitionAssignment(resource, partitionAssignment);
    Assert.assertTrue(resourceAssignment.containsResource(resource));
    Assert.assertEquals(resourceAssignment.getPartitionAssignment(resource), partitionAssignment);
  }

  @Test
  public void testUpdateResourceAssignment() {
    String resource = "test";
    ResourceAssignment resourceAssignment = new ResourceAssignment();
    ResourceAssignment newResourceAssignment = new ResourceAssignment();
    PartitionAssignment partitionAssignment = new PartitionAssignment(resource, 1);
    newResourceAssignment.setPartitionAssignment(resource, partitionAssignment);
    resourceAssignment.updateResourceAssignment(newResourceAssignment);
    Assert.assertEquals(resourceAssignment.getPartitionAssignment(resource), partitionAssignment);
    Assert.assertEquals(resourceAssignment.getAssignedResources(), newResourceAssignment.getAssignedResources());
  }

  @Test
  public void testUpdateResourceAssignmentGenerateChange() throws InterruptedException {
    // init 2 resource assignments
    ResourceAssignment resourceAssignment = new ResourceAssignment();
    ResourceAssignment newResourceAssignment = new ResourceAssignment();

    Time mockTime = new TestMockTime();

    for (String resource: new String[] { "1", "2", "3", "4" }) {
      resourceAssignment
          .setPartitionAssignment(resource, getDefaultPartitionAssignment(resource, HelixState.STANDBY, mockTime));
    }

    // resource 1 is the same in both resource assignments
    newResourceAssignment.setPartitionAssignment("1", getDefaultPartitionAssignment("1", HelixState.STANDBY, mockTime));

    // resource 2 has different Helix state in new resource assignment
    newResourceAssignment.setPartitionAssignment("2", getDefaultPartitionAssignment("2", HelixState.ERROR, mockTime));

    // resource 3 has different partitions in new resource assignment
    PartitionAssignment partitionAssignment = new PartitionAssignment("3", 2, mockTime);
    partitionAssignment.addPartition(getDefaultPartition(0, 1, HelixState.STANDBY));
    partitionAssignment.addPartition(getDefaultPartition(1, 1, HelixState.STANDBY));
    newResourceAssignment.setPartitionAssignment("3", partitionAssignment);

    // resource 4 is removed in new assignment and resource 5 is a new assignment
    newResourceAssignment.setPartitionAssignment("5", getDefaultPartitionAssignment("5", HelixState.STANDBY));

    // Emulate that it's not the first time resource 4 was deleted, and that enough time has passed.
    resourceAssignment.getPartitionAssignment("4").initializeDeletionTimestamp();
    mockTime.sleep(6 * Time.MS_PER_MINUTE);

    ResourceAssignmentChanges changes = resourceAssignment.updateResourceAssignment(newResourceAssignment);
    Set<String> deletedResources = changes.deletedResources;
    Set<String> updatedResources = changes.updatedResources;

    Assert.assertEquals(deletedResources.size(), 1);
    Assert.assertTrue(deletedResources.contains("4"));

    Assert.assertEquals(updatedResources.size(), 3);
    Assert.assertTrue(updatedResources.containsAll(Stream.of("2", "3", "5").collect(Collectors.toSet())));
  }

  @Test
  public void testCompareAndGetDeletedResources() throws InterruptedException {
    ResourceAssignment resourceAssignment1 = new ResourceAssignment();
    ResourceAssignment resourceAssignment2 = new ResourceAssignment();
    ResourceAssignment resourceAssignment3 = new ResourceAssignment();
    ResourceAssignment resourceAssignment4 = new ResourceAssignment();
    ResourceAssignment resourceAssignment5 = new ResourceAssignment();

    String resurrectedResource = "1";
    String[] oldResources = new String[] { resurrectedResource, "2", "3" };
    String[] newResources = new String[] { "4", "2", "5" };

    Time mockTime = new TestMockTime();

    for (String resource: oldResources) {
      resourceAssignment1.setPartitionAssignment(resource, new PartitionAssignment(resource, 1, mockTime));
    }
    for (String newResource: newResources) {
      resourceAssignment2.setPartitionAssignment(newResource, new PartitionAssignment(newResource, 1, mockTime));
      resourceAssignment3.setPartitionAssignment(newResource, new PartitionAssignment(newResource, 1, mockTime));
      resourceAssignment4.setPartitionAssignment(newResource, new PartitionAssignment(newResource, 1, mockTime));
      resourceAssignment5.setPartitionAssignment(newResource, new PartitionAssignment(newResource, 1, mockTime));
    }

    // The newly deleted resources should be kept
    Set<String> deletedResources = resourceAssignment1.compareAndGetDeletedResources(resourceAssignment2);
    Assert.assertEquals(deletedResources.size(), 0);
    for (String resource: oldResources) {
      assertTrue(resourceAssignment2.containsResource(resource));
    }
    for (String resource: newResources) {
      assertTrue(resourceAssignment2.containsResource(resource));
    }

    // After 2 more minutes, one of the newly deleted resources comes back, and the other remains deleted (but should
    // still be kept)
    mockTime.sleep(2 * Time.MS_PER_MINUTE);
    resourceAssignment3
        .setPartitionAssignment(resurrectedResource, new PartitionAssignment(resurrectedResource, 1, mockTime));
    deletedResources = resourceAssignment2.compareAndGetDeletedResources(resourceAssignment3);
    Assert.assertEquals(deletedResources.size(), 0);
    for (String resource: oldResources) {
      assertTrue(resourceAssignment3.containsResource(resource));
    }
    for (String resource: newResources) {
      assertTrue(resourceAssignment3.containsResource(resource));
    }

    // After 6 minutes total, the resource which remained deleted the whole time should finally be deleted,
    // while the resurrected one remains
    mockTime.sleep(4 * Time.MS_PER_MINUTE);
    deletedResources = resourceAssignment3.compareAndGetDeletedResources(resourceAssignment4);
    for (String resource: newResources) {
      assertTrue(resourceAssignment4.containsResource(resource));
    }
    assertTrue(resourceAssignment4.containsResource(resurrectedResource));
    assertFalse(resourceAssignment4.containsResource("3"));
    Assert.assertEquals(deletedResources.size(), 1);
    Assert.assertFalse(deletedResources.contains(resurrectedResource));
    Assert.assertTrue(deletedResources.contains("3"));

    // After 12 minutes total, even the resurrected resource should finally go away
    mockTime.sleep(6 * Time.MS_PER_MINUTE);
    deletedResources = resourceAssignment4.compareAndGetDeletedResources(resourceAssignment5);
    for (String resource: newResources) {
      assertTrue(resourceAssignment5.containsResource(resource));
    }
    assertFalse(resourceAssignment5.containsResource(resurrectedResource));
    assertFalse(resourceAssignment5.containsResource("3"));
    Assert.assertEquals(deletedResources.size(), 1);
    Assert.assertTrue(deletedResources.contains(resurrectedResource));
  }

  @Test
  public void testUpdateResourceAssignmentNoChange() {
    // init 2 resource assignments
    ResourceAssignment resourceAssignment = new ResourceAssignment();
    ResourceAssignment newResourceAssignment = new ResourceAssignment();
    for (String resource: new String[] { "1", "2" }) {
      resourceAssignment.setPartitionAssignment(resource, getDefaultPartitionAssignment(resource, HelixState.STANDBY));
      newResourceAssignment
          .setPartitionAssignment(resource, getDefaultPartitionAssignment(resource, HelixState.STANDBY));
    }
    ResourceAssignmentChanges changes = resourceAssignment.updateResourceAssignment(newResourceAssignment);
    Set<String> deletedResources = changes.deletedResources;
    Set<String> updatedResources = changes.updatedResources;
    Assert.assertEquals(deletedResources.size(), 0);
    Assert.assertEquals(updatedResources.size(), 0);
  }

  private PartitionAssignment getDefaultPartitionAssignment(String topicName, HelixState helixState) {
    return getDefaultPartitionAssignment(topicName, helixState, SystemTime.INSTANCE);
  }

  private PartitionAssignment getDefaultPartitionAssignment(String topicName, HelixState helixState, Time time) {
    Partition partition = getDefaultPartition(0, 1, helixState);
    PartitionAssignment partitionAssignment = new PartitionAssignment(topicName, 1, time);
    partitionAssignment.addPartition(partition);

    return partitionAssignment;
  }

  private Partition getDefaultPartition(int partitionId, int replicaNum, HelixState helixState) {
    EnumMap<HelixState, List<Instance>> stateToInstancesMap = new EnumMap<>(HelixState.class);
    for (int i = 0; i < replicaNum; i++) {
      stateToInstancesMap.put(helixState, Collections.singletonList(new Instance(String.valueOf(i), "localhost", i)));
    }

    return new Partition(partitionId, stateToInstancesMap, new EnumMap<>(ExecutionStatus.class));
  }

}
