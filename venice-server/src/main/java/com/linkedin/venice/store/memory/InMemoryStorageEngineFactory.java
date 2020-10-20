package com.linkedin.venice.store.memory;

import com.linkedin.venice.config.VeniceServerConfig;
import com.linkedin.venice.config.VeniceStoreConfig;
import com.linkedin.venice.exceptions.StorageInitializationException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.store.AbstractStorageEngine;
import com.linkedin.venice.store.StorageEngineFactory;

import java.util.HashSet;
import java.util.Set;


public class InMemoryStorageEngineFactory extends StorageEngineFactory {
  private final Object lock = new Object();


  public InMemoryStorageEngineFactory(VeniceServerConfig serverConfig) {

  }

  @Override
  public AbstractStorageEngine getStorageEngine(VeniceStoreConfig storeConfig)
      throws StorageInitializationException {
    verifyPersistenceType(storeConfig);
    synchronized (lock) {
      try {
        return new InMemoryStorageEngine(storeConfig);
      } catch (Exception e) {
        throw new StorageInitializationException(e);
      }
    }
  }

  @Override
  public Set<String> getPersistedStoreNames() {
    // Nothing to restore here
    return new HashSet<>();
  }

  @Override
  public void close() {
    //Nothing to do here since we are not tracking specific created environments.
  }

  @Override
  public void removeStorageEngine(AbstractStorageEngine engine) {
    // Nothing to do here since we do not track the created storage engine
  }

  @Override
  public void closeStorageEngine(AbstractStorageEngine engine) {
    // Nothing to do here since we do not track the created storage engine
  }

  @Override
  public PersistenceType getPersistenceType() {
    return PersistenceType.IN_MEMORY;
  }
}
