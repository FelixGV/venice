package com.linkedin.venice.hooks;

public enum StoreVersionLifecycleEventOutcome {
  PROCEED, ABORT, WAIT, ROLLBACK;
}
