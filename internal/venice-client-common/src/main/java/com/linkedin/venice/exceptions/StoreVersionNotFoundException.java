package com.linkedin.venice.exceptions;

public class StoreVersionNotFoundException extends VeniceException {
  public StoreVersionNotFoundException(String message) {
    super(message);
  }
}
