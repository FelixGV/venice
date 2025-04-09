package com.linkedin.venice.endToEnd;

import org.testng.annotations.Test;


@Test
public class DataRecoveryTestWithHMC extends DataRecoveryTest {
  @Override
  protected boolean getHelixMessagingChannelEnabled() {
    /** TODO: Figure out why the test fails when the Helix admin channel is disabled... */
    return true;
  }
}
