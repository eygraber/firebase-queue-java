package com.firebase.queue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class UuidUtilsTest {
  @Test
  public void getUUID_doesNotReturnTheSameValueIfCalledAgain() {
    String firstValue = UuidUtils.randomUUID();
    String secondValue = UuidUtils.randomUUID();

    assertThat(firstValue).isNotEqualTo(secondValue);
  }
}
