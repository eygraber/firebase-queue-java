package com.firebase.queue;

import java.util.UUID;

class UuidUtils {
  private UuidUtils() {}

  static String randomUUID() {
    return UUID.randomUUID().toString();
  }
}
