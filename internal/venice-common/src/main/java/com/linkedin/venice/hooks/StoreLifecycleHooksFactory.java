package com.linkedin.venice.hooks;

import com.linkedin.venice.utils.VeniceProperties;


public abstract class StoreLifecycleHooksFactory {
  public abstract StoreLifecycleHooks getHooks(
      String clusterName,
      String storeName,
      VeniceProperties generalConfigs,
      VeniceProperties storeConfigs);
}
