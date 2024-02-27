package com.linkedin.venice.hooks;

import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Map;


/**
 * This interface defines a set of lifecycle events for stores and their store-versions.
 *
 * <b>Pre-hooks and post-hooks</b>
 *
 * Most events have a pre- and post- variant, with the pre-hook providing an option to control the outcome of the event
 * (e.g. to abort it), while the post-hook is intended for informational purposes only.
 *
 * <b>Failures and timeouts</b>
 *
 * All exceptions thrown from any of the hooks are swallowed, logged and emitted as metrics. In the case of pre-hooks,
 * exceptions will be treated as an indication to proceed. Likewise, the framework executing these hooks is allowed to
 * impose a maximum duration on the hook's runtime, and to proceed forward in case of timeouts, so hooks should not
 * block for extended durations. Note that some hooks allow for deferring their control flow decisions, via
 * {@link StoreVersionLifecycleEventOutcome#WAIT}, thus providing a first-class mechanism for stalling the operation in
 * case the hook needs more time to make its decision. If a hook implementer wishes to prevent an operation from
 * proceeding if the hook fails or hangs, then it is the implementer's responsibility to wrap the whole hook's logic
 * into a try-catch or similar safeguard, and to return the signal to abort (or wait) in case of failure or time out.
 *
 * <b>Instance creation, state and lifetime</b>
 *
 * All {@link StoreLifecycleHooks} instances will be created via an accompanying {@link StoreLifecycleHooksFactory},
 * which needs to be registered at startup time. If multiple factories are registered, then hooks will be invoked in the
 * order in which they are registered in the config. The hooks framework can, at its discretion, create, reuse and close
 * instances whenever. Related hooks (such as pre- and post-hooks of a certain event type) are not guaranteed to be
 * invoked on the same hook instance, or even in the same JVM. Therefore, any state which a hook instance accumulates
 * should be strictly limited to best-effort optimizations, e.g. establishing a connection to another service, and
 * keeping that connection (or connection pool) open is fine. All state which is significant for the correctness of the
 * hook should be persisted outside the hooks' runtime memory. Likewise, no state should ever be persisted in the JVM's
 * local filesystem.
 *
 * <b>Store-level hooks configs</b>
 *
 * The store config contains a bag of arbitrary properties which can be used to override the behavior of hooks. This bag
 * is passed into all hooks. Note that there is just a single bag of properties per store, therefore the configs of all
 * registered hooks must coexist within that bag. As such, namespacing is strongly recommended, so that different hooks
 * don't conflict with each other.
 *
 * <b>Cardinality</b>
 *
 * Each hook is annotated with a cardinality, providing a rough indication of how often the hook can be expected to be
 * invoked.
 *
 * <b>Thread safety</b>
 *
 * Hook functions can be invoked concurrently and so hook implementations are expected to be thread-safe.
 */
@Threadsafe
public abstract class StoreLifecycleHooks {
  protected final VeniceProperties defaultConfigs;

  public StoreLifecycleHooks(VeniceProperties defaultConfigs) {
    this.defaultConfigs = defaultConfigs;
  }

  /**
   * Invoked prior to starting a new job. The hook has the option of aborting the job.
   *
   * N.B.: this hook returns a {@link StoreLifecycleEventOutcome}, and not a {@link StoreVersionLifecycleEventOutcome},
   * because the new store-version is not created yet at the start of the push job.
   *
   * Cardinality: once per push job.
   */
  StoreLifecycleEventOutcome preStartOfPushJob(
      String clusterName,
      String storeName,
      VeniceProperties jobProperties,
      VeniceProperties storeHooksConfigs) {
    return StoreLifecycleEventOutcome.PROCEED;
  }

  /**
   * Invoked after starting a new job (i.e. if all {@link #preStartOfPushJob(String, String, VeniceProperties, VeniceProperties)}
   * hooks succeeded).
   *
   * Cardinality: once per push job, assuming no previous failures.
   */
  void postStartOfPushJob(
      String clusterName,
      String storeName,
      VeniceProperties jobProperties,
      VeniceProperties storeHooksConfigs) {
  }

  /**
   * Invoked prior to registering a new schema, but after standard checks already succeeded. The hook has the option of
   * aborting the registration (which causes the whole push job to abort as well, if this is happening as part of a
   * job's auto-registration step).
   *
   * Cardinality: once per schema registration attempt.
   */
  StoreLifecycleEventOutcome preSchemaRegistration(
      String clusterName,
      String storeName,
      String newSchema,
      Map<Integer, String> existingSchemas,
      VeniceProperties storeHooksConfigs) {
    return StoreLifecycleEventOutcome.PROCEED;
  }

  /**
   * Invoked after registering a new schema.
   *
   * Cardinality: once per successful schema registration.
   */
  void postSchemaRegistration(
      String clusterName,
      String storeName,
      String newSchema,
      Map<Integer, String> existingSchemas,
      VeniceProperties storeHooksConfigs) {
  }

  /**
   * Invoked prior to creating a store-version in a given region.
   *
   * The hook has the option of proceeding, aborting, waiting or rolling back. In case of rollback, the effect is global
   * (i.e. other regions which already proceeded will kill the ongoing push if it is still in process, or rollback to
   * the previous store-version, if the push already completed in that region).
   *
   * Cardinality: once per store-version creation attempt per region, assuming no previous failures.
   */
  StoreVersionLifecycleEventOutcome preStoreVersionCreation(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      VeniceProperties storeHooksConfigs) {
    return StoreVersionLifecycleEventOutcome.PROCEED;
  }

  /**
   * Invoked after creating a store-version in a given region, which means the following actions succeeded:
   *
   * - All {@link #preStoreVersionCreation(String, String, int, String, VeniceProperties)} hooks.
   * - Creation of the store-version's dedicated resources (pub sub topic, Helix resource).
   * - Server replicas have begun ingesting.
   *
   * Cardinality: once per successful store-version creation per region.
   */
  void postStoreVersionCreation(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      VeniceProperties storeHooksConfigs) {
  }

  /**
   * Invoked prior to informing Da Vinci Clients about starting to ingest a new store-version.
   *
   * The hook has the option of proceeding, aborting, waiting or rolling back. In case of rollback, the effect is global
   * (i.e. other regions which already proceeded will kill the ongoing push if it is still in process, or rollback to
   * the previous store-version, if the push already completed in that region).
   *
   * Cardinality: once per store-version per region.
   */
  StoreVersionLifecycleEventOutcome preStoreVersionIngestionForDaVinci(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      VeniceProperties storeHooksConfigs) {
    return StoreVersionLifecycleEventOutcome.PROCEED;
  }

  /**
   * Invoked after informing Da Vinci Clients that they may start to ingest a new store-version.
   *
   * It is important to note that:
   *
   * 1. DVC will not necessarily have started ingesting when this hook is invoked, since that process happens
   *    asynchronously after notifying.
   *
   * 2. DVC will proceed to swap for serving reads on a per-instance basis as soon the ingestion completes. At this time
   *    there is no support for deferring the swap until all instances of a region have completed (as is the case for
   *    servers).
   *
   * Cardinality: once per store-version per region.
   */
  void postStoreVersionIngestionForDaVinci(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      VeniceProperties storeHooksConfigs) {
  }

  /**
   * Invoked after all leader replicas of a store-version within a single region have completed, which means the data
   * replication to that region is done.
   *
   * Cardinality: once per successfully-replicated store-version per region.
   */
  void postStoreVersionLeaderReplication(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      VeniceProperties storeHooksConfigs) {
  }

  /**
   * Invoked prior to swapping read traffic for servers. Specifically, this means:
   *
   * - Leader replication is complete.
   * - A sufficient number of followers have completed their ingestion.
   * - All online Da Vinci Client instances have already swapped their reads to the new store-version.
   *
   * The hook has the option of proceeding, aborting, waiting or rolling back. In case of rollback, the effect is global
   * (i.e. other regions which already proceeded will kill the ongoing push if it is still in process, or rollback to
   * the previous store-version, if the push already completed in that region).
   *
   * Cardinality: once per store-version per region which is ready to swap.
   */
  StoreVersionLifecycleEventOutcome preStoreVersionSwap(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      VeniceProperties storeHooksConfigs) {
    return StoreVersionLifecycleEventOutcome.PROCEED;
  }

  /**
   * Invoked after swapping read traffic for servers.
   *
   * Cardinality: once per store-version per region which has successfully swapped.
   */
  void postStoreVersionSwap(
      String clusterName,
      String storeName,
      int versionNumber,
      String regionName,
      VeniceProperties storeHooksConfigs) {
  }

  /**
   * Invoked prior to ending a job. Specifically, this means that the new store-version was swapped in all regions. This
   * is a final chance to control the job and its associated store-version prior to termination.
   *
   * The hook has the option of proceeding, aborting, waiting or rolling back. In case of rollback, the effect is global
   * (i.e. other regions which already proceeded will kill the ongoing push if it is still in process, or rollback to
   * the previous store-version, if the push already completed in that region).
   *
   * Cardinality: once per push job.
   */
  StoreVersionLifecycleEventOutcome preEndOfPushJob(
      String clusterName,
      String storeName,
      VeniceProperties jobProperties,
      VeniceProperties storeHooksConfigs) {
    return StoreVersionLifecycleEventOutcome.PROCEED;
  }

  /**
   * Invoked after a job ends (i.e. if all {@link #preEndOfPushJob(String, String, VeniceProperties, VeniceProperties)}
   * hooks succeeded).
   *
   * Cardinality: once per push job, assuming no previous failures.
   */
  void postEndOfPushJob(
      String clusterName,
      String storeName,
      VeniceProperties jobProperties,
      VeniceProperties storeHooksConfigs) {
  }
}
