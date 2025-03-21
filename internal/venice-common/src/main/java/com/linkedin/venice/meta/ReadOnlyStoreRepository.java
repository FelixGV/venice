package com.linkedin.venice.meta;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Utils;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Interface defined readonly operations to access stores.
 */
public interface ReadOnlyStoreRepository extends VeniceResource {
  /**
   * Get one store by given name from repository.
   *
   * Implementations can override this to leverage the performance benefits of {@link StoreName}.
   *
   * @param storeName name of wanted store.
   * @return Store for given name.
   */
  default Store getStore(StoreName storeName) {
    return getStore(storeName.getName());
  }

  /**
   * Get one store by given name from repository.
   *
   * @param storeName name of wanted store.
   * @return Store for given name.
   * @deprecated use {@link #getStore(StoreName)} instead.
   */
  @Deprecated
  Store getStore(String storeName);

  /**
   * Implementations can override this to leverage the performance benefits of {@link StoreName}.
   *
   * @param storeName name of wanted store.
   * @return Store for given name.
   */
  default Store getStoreOrThrow(StoreName storeName) {
    return getStoreOrThrow(storeName.getName());
  }

  /**
   * @deprecated use {@link #getStoreOrThrow(StoreName)} instead.
   */
  @Deprecated
  Store getStoreOrThrow(String storeName) throws VeniceNoStoreException;

  /**
   * Wait for a specified store/version to appear in the Store Repository and retrieve them.
   *
   * @param storeName       Store name to wait for.
   * @param versionNumber   Version number to wait for.
   * @param timeout         Maximum wait time allowed before giving up.
   * @return (store, version) pair on success.
   *         (store, null) if store exists, but version still isn't after waiting for allowed time.
   *         (null, null) if store still doesn't exit after waiting for allowed time.
   */
  default Pair<Store, Version> waitVersion(String storeName, int versionNumber, Duration timeout) {
    return waitVersion(storeName, versionNumber, timeout, TimeUnit.SECONDS.toMillis(1));
  }

  default Pair<Store, Version> waitVersion(String storeName, int versionNumber, Duration timeout, long delayMs) {
    long expirationTime = System.currentTimeMillis() + timeout.toMillis() - delayMs;
    Store store = getStore(storeName);
    for (;;) {
      if (store != null) {
        Version version = store.getVersion(versionNumber);
        if (version != null) {
          return new Pair<>(store, version);
        }
      }
      if (expirationTime < System.currentTimeMillis() || !Utils.sleep(delayMs)) {
        return new Pair<>(store, null);
      }
      store = refreshOneStore(storeName);
    }
  }

  /**
   * Implementations can override this to leverage the performance benefits of {@link StoreName}.
   *
   * @param storeName to check the existence for.
   * @return Whether the store exists or not.
   */
  default boolean hasStore(StoreName storeName) {
    return hasStore(storeName.getName());
  }

  /**
   * @param storeName to check the existence for.
   * @return Whether the store exists or not.
   * @deprecated use {@link #hasStore(StoreName)} instead.
   */
  @Deprecated
  boolean hasStore(String storeName);

  /**
   * Selective refresh operation which fetches one store from ZK
   *
   * Implementations can override this to leverage the performance benefits of {@link StoreName}.
   *
   * @param storeName store name
   * @return the newly refreshed store
   */
  default Store refreshOneStore(StoreName storeName) {
    return refreshOneStore(storeName.getName());
  }

  /**
   * Selective refresh operation which fetches one store from ZK
   *
   * @param storeName store name
   * @return the newly refreshed store
   * @deprecated use {@link #refreshOneStore(StoreName)} instead.
   */
  @Deprecated
  Store refreshOneStore(String storeName);

  /**
   * Get all stores in the current repository
   * @return
   */
  List<Store> getAllStores();

  /**
   * Get total read quota of all stores.
   */
  long getTotalStoreReadQuota();

  /**
   * Register store data change listener.
   *
   * @param listener
   */
  void registerStoreDataChangedListener(StoreDataChangedListener listener);

  /**
   * Unregister store data change listener.
   * @param listener
   */
  void unregisterStoreDataChangedListener(StoreDataChangedListener listener);

  /**
   * Get batch-get limit for the specified store
   *
   * Implementations can override this to leverage the performance benefits of {@link StoreName}.
   */
  default int getBatchGetLimit(StoreName storeName) {
    return getBatchGetLimit(storeName.getName());
  }

  /**
   * Get batch-get limit for the specified store
   * @deprecated use {@link #getBatchGetLimit(StoreName)} instead
   */
  @Deprecated
  int getBatchGetLimit(String storeName);

  /**
   * Implementations can override this to leverage the performance benefits of {@link StoreName}.
   * @param storeName store name
   * @return Whether computation is enabled for the specified store.
   */
  default boolean isReadComputationEnabled(StoreName storeName) {
    return isReadComputationEnabled(storeName.getName());
  }

  /**
   * @param storeName store name
   * @return Whether computation is enabled for the specified store.
   * @deprecated use {@link #isReadComputationEnabled(StoreName)} instead.
   */
  @Deprecated
  boolean isReadComputationEnabled(String storeName);
}
