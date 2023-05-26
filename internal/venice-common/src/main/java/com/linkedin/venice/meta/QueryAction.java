package com.linkedin.venice.meta;

import java.util.Arrays;
import java.util.stream.Collectors;


public enum QueryAction {
  // STORAGE is a GET request to storage/storename/key on the router or storage/resourcename/partition/key on the
  // storage node
  STORAGE,

  // Health check request from routers
  HEALTH,

  // read-compute request from routers
  COMPUTE,

  // DICTIONARY is a GET request to storage/storename/version on the storage node to fetch compression dictionary for
  // that version
  DICTIONARY,

  // Admin request from server admin tool
  ADMIN,

  // METADATA is a GET request to /metadata/storename on the storage node to fetch metadata for that node
  METADATA;

  public static final String VALID_ACTIONS_STRING =
      Arrays.stream(QueryAction.values()).map(e -> e.toString().toLowerCase()).collect(Collectors.joining(", "));
}
