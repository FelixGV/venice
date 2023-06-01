package com.linkedin.venice.routerapi;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.linkedin.venice.pushmonitor.ExecutionStatus;


public class ReplicaState {
  private int partitionId;
  private String participantId;
  private ExecutionStatus venicePushStatus;
  private boolean isReadyToServe;

  public ReplicaState() {
    // Dummy constructor for JSON
  }

  public ReplicaState(int partitionId, String participantId, ExecutionStatus venicePushStatus, boolean isReadyToServe) {
    this.partitionId = partitionId;
    this.participantId = participantId;
    this.venicePushStatus = venicePushStatus;
    this.isReadyToServe = isReadyToServe;
  }

  public int getPartition() {
    return partitionId;
  }

  public String getParticipantId() {
    return participantId;
  }

  public ExecutionStatus getVenicePushStatus() {
    return venicePushStatus;
  }

  public boolean isReadyToServe() {
    return isReadyToServe;
  }

  public void setPartition(int partitionId) {
    this.partitionId = partitionId;
  }

  public void setParticipantId(String participantId) {
    this.participantId = participantId;
  }

  public void setVenicePushStatus(String venicePushStatus) {
    this.venicePushStatus = ExecutionStatus.valueOf(venicePushStatus);
  }

  public void setVenicePushStatus(ExecutionStatus venicePushStatus) {
    this.venicePushStatus = venicePushStatus;
  }

  public void setReadyToServe(boolean readyToServe) {
    isReadyToServe = readyToServe;
  }

  @JsonIgnore
  public String toString() {
    return "{" + partitionId + ", " + participantId + ", " + venicePushStatus + ", " + isReadyToServe + "}";
  }
}
