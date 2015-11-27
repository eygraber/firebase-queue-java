package com.firebase.queue;

import org.jetbrains.annotations.NotNull;

public interface TaskProcessor {
  void process(@NotNull Task task) throws InterruptedException;
}
