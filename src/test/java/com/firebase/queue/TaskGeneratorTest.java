package com.firebase.queue;

import com.firebase.client.Firebase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(JUnit4.class)
public class TaskGeneratorTest {
  @Test
  public void generateTask_generatesATaskWithTheProperValues() {
    Firebase taskRef = mock(Firebase.class);
    Map<String, Object> taskData = mock(Map.class);
    TaskClaimer.TaskGenerator taskGenerator = new TaskClaimer.TaskGenerator(taskRef, taskData);

    TaskSpec taskSpec = new TaskSpec(TestUtils.getValidTaskSpecSnapshot());
    Task task = taskGenerator.generateTask("id", taskSpec, mock(TaskReset.class), mock(ValidityChecker.class), mock(Queue.Options.class));

    assertThat(Whitebox.getInternalState(task, "taskRef")).isEqualTo(taskRef);
    assertThat(Whitebox.getInternalState(task, "data")).isEqualTo(taskData);
  }
}
