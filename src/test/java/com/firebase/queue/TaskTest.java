package com.firebase.queue;

import com.firebase.client.Firebase;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.firebase.queue.TestUtils.getDefaultTaskSpecSnapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

@RunWith(JUnit4.class)
public class TaskTest {
  private static final String OWNER_ID = "owner";
  private static final String TASK_KEY = "task_key";

  private Task subject;

  private Firebase taskRef;
  private TaskSpec taskSpec;
  private TaskReset taskReset;
  private ValidityChecker validityChecker;

  private Log.Logger logger;

  @Before
  public void setUp() throws Exception {
    // clear the interrupt flag if it is set
    if(Thread.currentThread().isInterrupted()) {
      try {Thread.sleep(1);} catch(InterruptedException ignore) {}
    }

    taskRef = mock(Firebase.class);
    stub(taskRef.getKey()).toReturn(TASK_KEY);

    taskSpec = new TaskSpec(getDefaultTaskSpecSnapshot());

    taskReset = mock(TaskReset.class);

    validityChecker = mock(ValidityChecker.class);

    logger = mock(Log.Logger.class);
    stub(logger.shouldLogDebug(any(Log.Level.class))).toReturn(true);
    Log.DEBUG = true;
    Log.setLogger(logger);
  }

  @Test(expected = IllegalStateException.class)
  public void process_calledTwice_throwsAnException() {
    initSubject().countDown();
    subject.process(mock(TaskProcessor.class));
    subject.process(mock(TaskProcessor.class));
  }

  @Test(expected = IllegalStateException.class)
  public void process_calledTwice_doesNotResetTheTask() {
    initSubject().countDown();

    TaskProcessor taskProcessor = new NoOpTaskProcessor();
    subject.process(taskProcessor);
    subject.process(mock(TaskProcessor.class));

    verifyZeroInteractions(taskReset);
  }

  @Test
  public void process_ifTheThreadIsInterruptedBeforeTheTaskIsProcessed_doesNotDelegateToTaskProcessor() {
    initSubject();

    Thread.currentThread().interrupt();

    TaskProcessor taskProcessor = mock(TaskProcessor.class);
    subject.process(taskProcessor);

    verifyZeroInteractions(taskProcessor);
  }

  @Test
  public void process_ifTheThreadIsInterruptedBeforeTheTaskIsProcessed_causesTheTaskToBeCancelled() {
    initSubject();

    Thread.currentThread().interrupt();

    subject.process(new NoOpTaskProcessor());

    assertThat(subject.isCancelled()).isTrue();
  }

  @Test
  public void process_ifTheThreadIsInterruptedBeforeTheTaskIsProcessed_finishesTheTask() {
    initSubject();

    Thread.currentThread().interrupt();

    subject.process(new NoOpTaskProcessor());

    assertThat(subject.isFinished()).isTrue();
  }

  @Test
  public void process_ifTheThreadIsInterruptedBeforeTheTaskIsProcessed_resetsTheTask() {
    initSubject();

    Thread.currentThread().interrupt();

    subject.process(new NoOpTaskProcessor());

    verify(taskReset).reset(taskRef, taskSpec.getInProgressState());
  }

  @Test
  public void process_delegatesToTheTaskProcessor() throws InterruptedException {
    initSubject().countDown();

    TaskProcessor taskProcessor = mock(TaskProcessor.class);
    subject.process(taskProcessor);

    verify(taskProcessor).process(subject);
  }

  @Test
  public void process_doesNotFinishTheTask() throws InterruptedException {
    initSubject().countDown();

    TaskProcessor taskProcessor = mock(TaskProcessor.class);
    subject.process(taskProcessor);

    assertThat(subject.isFinished()).isFalse();
  }

  @Test
  public void process_ifTaskProcessorThrowsInterruptedException_causesTheTaskToBeCancelled() {
    initSubject();

    subject.process(new InterruptedTaskProcessor());

    assertThat(subject.isCancelled()).isTrue();
  }

  @Test
  public void process_ifTaskProcessorThrowsInterruptedException_butTheTaskCannotBeInterrupted_doesNotCauseTheTaskToBeCancelled() {
    initSubject();

    Whitebox.setInternalState(subject, "aborted", true);

    subject.process(new InterruptedTaskProcessor());

    assertThat(subject.isCancelled()).isFalse();
  }

  @Test
  public void process_ifTaskProcessorThrowsInterruptedException_finishesTheTask() {
    initSubject();

    subject.process(new InterruptedTaskProcessor());

    assertThat(subject.isFinished()).isTrue();
  }

  @Test
  public void process_ifTaskProcessorThrowsInterruptedException_resetsTheTask() {
    initSubject();

    subject.process(new InterruptedTaskProcessor());

    verify(taskReset).reset(taskRef, taskSpec.getInProgressState());
  }

  @Test
  public void process_ifTaskProcessorThrowsInterruptedException_butTheTaskCannotBeInterrupted_doesNotResetTheTask() {
    initSubject();

    Whitebox.setInternalState(subject, "aborted", true);

    subject.process(new InterruptedTaskProcessor());

    verifyZeroInteractions(taskReset);
  }

  @Test
  public void process_ifTheThreadIsInterruptedAfterTheTaskIsProcessed_causesTheTaskToBeCancelled() {
    initSubject();

    subject.process(new InterruptingTaskProcessor());

    assertThat(subject.isCancelled()).isTrue();
  }

  @Test
  public void process_ifTheThreadIsInterruptedAfterTheTaskIsProcessed_butTheTaskCannotBeInterrupted_doesNotCauseTheTaskToBeCancelled() {
    initSubject();

    Whitebox.setInternalState(subject, "aborted", true);

    subject.process(new InterruptingTaskProcessor());

    assertThat(subject.isCancelled()).isFalse();
  }

  @Test
  public void process_ifTheThreadIsInterruptedAfterTheTaskIsProcessed_finishesTheTask() {
    initSubject();

    subject.process(new InterruptingTaskProcessor());

    assertThat(subject.isFinished()).isTrue();
  }

  @Test
  public void process_ifTheThreadIsInterruptedAfterTheTaskIsProcessed_resetsTheTask() {
    initSubject();

    subject.process(new InterruptingTaskProcessor());

    verify(taskReset).reset(taskRef, taskSpec.getInProgressState());
  }

  @Test
  public void process_ifTheThreadIsInterruptedAfterTheTaskIsProcessed_butTheTaskCannotBeInterrupted_doesNotResetTheTask() {
    initSubject();

    Whitebox.setInternalState(subject, "aborted", true);

    subject.process(new InterruptingTaskProcessor());

    verifyZeroInteractions(taskReset);
  }

  @Test
  public void abort_writesALog() {
    final CountDownLatch latch = initSubject();

    subject.process(new TaskProcessor() {
      @Override
      public void process(@NotNull Task task) throws InterruptedException {
        task.abort();
        latch.countDown();
      }
    });

    verify(logger).debug(Log.Level.INFO, "Attempting to abort task " + taskRef.getKey() + " on " + OWNER_ID);
  }

  @Test
  public void abort_ifTaskIsNotFinishedOrCancelled_resetsTheTask() {
    final CountDownLatch latch = initSubject();

    subject.process(new TaskProcessor() {
      @Override
      public void process(@NotNull Task task) throws InterruptedException {
        task.abort();
        latch.countDown();
      }
    });

    verify(taskReset).reset(eq(taskRef), eq(OWNER_ID), eq(taskSpec.getInProgressState()), any(TaskReset.Listener.class));
  }

  @Test
  public void abort_ifTaskIsNotFinishedOrCancelled_finishesTheTask() {
    final CountDownLatch latch = initSubject();

    subject.process(new TaskProcessor() {
      @Override
      public void process(@NotNull Task task) throws InterruptedException {
        task.abort();
        latch.countDown();
      }
    });

    assertThat(subject.isFinished()).isTrue();
  }

  @Test
  public void abort_ifTaskIsNotFinishedOrCancelled_resetsTheTask_andWhenTheResetIsSuccessful_writesALog() {
    final CountDownLatch latch = initSubject();

    subject.process(new TaskProcessor() {
      @Override
      public void process(@NotNull Task task) throws InterruptedException {
        task.abort();
        latch.countDown();
      }
    });

    ArgumentCaptor<TaskReset.Listener> captor = ArgumentCaptor.forClass(TaskReset.Listener.class);
    verify(taskReset).reset(eq(taskRef), eq(OWNER_ID), eq(taskSpec.getInProgressState()), captor.capture());

    TaskReset.Listener listener = captor.getValue();
    listener.onReset();

    verify(logger).debug(Log.Level.INFO, "Successful abort of task " + taskRef.getKey() + " on " + OWNER_ID);
  }

  @Test
  public void abort_ifTaskIsNotFinishedOrCancelled_resetsTheTask_andWhenTheResetIsSuccessful_invokesTheListener() {
    final CountDownLatch latch = initSubject();

    final Task.Listener taskListener = mock(Task.Listener.class);
    subject.process(new TaskProcessor() {
      @Override
      public void process(@NotNull Task task) throws InterruptedException {
        task.abort(taskListener);
        latch.countDown();
      }
    });

    ArgumentCaptor<TaskReset.Listener> captor = ArgumentCaptor.forClass(TaskReset.Listener.class);
    verify(taskReset).reset(eq(taskRef), eq(OWNER_ID), eq(taskSpec.getInProgressState()), captor.capture());

    TaskReset.Listener listener = captor.getValue();
    listener.onReset();

    verify(taskListener).onSuccess();
  }

  @Test
  public void abort_ifTaskIsNotFinishedOrCancelled_resetsTheTask_andWhenTheResetIsNotSuccessful_andTheResetCannotBeRetried_cancelsTheTask() {
    final CountDownLatch latch = initSubject();

    subject.process(new TaskProcessor() {
      @Override
      public void process(@NotNull Task task) throws InterruptedException {
        task.abort();
        latch.countDown();
      }
    });

    ArgumentCaptor<TaskReset.Listener> captor = ArgumentCaptor.forClass(TaskReset.Listener.class);
    verify(taskReset).reset(eq(taskRef), eq(OWNER_ID), eq(taskSpec.getInProgressState()), captor.capture());

    TaskReset.Listener listener = captor.getValue();
    listener.onResetFailed("Error", false);

    assertThat(subject.isCancelled()).isTrue();
    assertThat(subject.isFinished()).isTrue();
  }

  @Test
  public void abort_ifTaskIsNotFinishedOrCancelled_resetsTheTask_andWhenTheResetIsNotSuccessful_andTheResetCannotBeRetried_invokesTheListener() {
    final CountDownLatch latch = initSubject();

    final Task.Listener taskListener = mock(Task.Listener.class);
    subject.process(new TaskProcessor() {
      @Override
      public void process(@NotNull Task task) throws InterruptedException {
        task.abort(taskListener);
        latch.countDown();
      }
    });

    ArgumentCaptor<TaskReset.Listener> captor = ArgumentCaptor.forClass(TaskReset.Listener.class);
    verify(taskReset).reset(eq(taskRef), eq(OWNER_ID), eq(taskSpec.getInProgressState()), captor.capture());

    TaskReset.Listener listener = captor.getValue();
    listener.onResetFailed("Error", false);

    verify(taskListener).onFailure("Error", false);
  }

  @Test
  public void abort_ifTaskIsNotFinishedOrCancelled_resetsTheTask_andWhenTheResetIsNotSuccessful_andTheResetCanBeRetried_doesNotCancelTheTask() {
    final CountDownLatch latch = initSubject();

    subject.process(new TaskProcessor() {
      @Override
      public void process(@NotNull Task task) throws InterruptedException {
        task.abort();
        latch.countDown();
      }
    });

    ArgumentCaptor<TaskReset.Listener> captor = ArgumentCaptor.forClass(TaskReset.Listener.class);
    verify(taskReset).reset(eq(taskRef), eq(OWNER_ID), eq(taskSpec.getInProgressState()), captor.capture());

    TaskReset.Listener listener = captor.getValue();
    listener.onResetFailed("Error", true);

    assertThat(subject.isCancelled()).isFalse();
  }

  @Test
  public void abort_ifTaskIsNotFinishedOrCancelled_resetsTheTask_andWhenTheResetIsNotSuccessful_andTheResetCanBeRetried_unfinishesTheTask() {
    final CountDownLatch latch = initSubject();

    subject.process(new TaskProcessor() {
      @Override
      public void process(@NotNull Task task) throws InterruptedException {
        task.abort();
        latch.countDown();
      }
    });

    ArgumentCaptor<TaskReset.Listener> captor = ArgumentCaptor.forClass(TaskReset.Listener.class);
    verify(taskReset).reset(eq(taskRef), eq(OWNER_ID), eq(taskSpec.getInProgressState()), captor.capture());

    TaskReset.Listener listener = captor.getValue();
    listener.onResetFailed("Error", true);

    assertThat(subject.isFinished()).isFalse();
  }

  @Test
  public void abort_ifTaskIsNotFinishedOrCancelled_resetsTheTask_andWhenTheResetIsNotSuccessful_andTheResetCanBeRetried_invokesTheListener() {
    final CountDownLatch latch = initSubject();

    final Task.Listener taskListener = mock(Task.Listener.class);
    subject.process(new TaskProcessor() {
      @Override
      public void process(@NotNull Task task) throws InterruptedException {
        task.abort(taskListener);
        latch.countDown();
      }
    });

    ArgumentCaptor<TaskReset.Listener> captor = ArgumentCaptor.forClass(TaskReset.Listener.class);
    verify(taskReset).reset(eq(taskRef), eq(OWNER_ID), eq(taskSpec.getInProgressState()), captor.capture());

    TaskReset.Listener listener = captor.getValue();
    listener.onResetFailed("Error", true);

    verify(taskListener).onFailure("Error", true);
  }

  @Test
  public void abort_ifTaskCouldNotBeAborted_cancelsTheTask() {
    final CountDownLatch latch = initSubject();

    Whitebox.setInternalState(subject, "aborted", true);
    subject.process(new TaskProcessor() {
      @Override
      public void process(@NotNull Task task) throws InterruptedException {
        task.abort();
        latch.countDown();
      }
    });

    assertThat(subject.isCancelled()).isTrue();
    assertThat(subject.isFinished()).isTrue();
  }

  @Test
  public void abort_ifTaskCouldNotBeAborted_writesALog() {
    final CountDownLatch latch = initSubject();

    Whitebox.setInternalState(subject, "aborted", true);
    subject.process(new TaskProcessor() {
      @Override
      public void process(@NotNull Task task) throws InterruptedException {
        task.abort();
        latch.countDown();
      }
    });

    verify(logger).debug(Log.Level.INFO, "Couldn't abort task " + taskRef.getKey() + " on " + OWNER_ID + " because it has already been aborted");
  }

  @Test
  public void abort_ifTaskCouldNotBeAborted_invokesTheListener() {
    final CountDownLatch latch = initSubject();

    Whitebox.setInternalState(subject, "aborted", true);
    final Task.Listener taskListener = mock(Task.Listener.class);
    subject.process(new TaskProcessor() {
      @Override
      public void process(@NotNull Task task) throws InterruptedException {
        task.abort(taskListener);
        latch.countDown();
      }
    });

    verify(taskListener).onFailure("Couldn't abort this task because it has already been aborted", false);
  }

  private CountDownLatch initSubject() {
    return initSubject(new HashMap<String, Object>(), false);
  }

  private CountDownLatch initSubject(Map<String, Object> taskData) {
    return initSubject(taskData, false);
  }

  private CountDownLatch initSubject(boolean suppressStack) {
    return initSubject(new HashMap<String, Object>(), suppressStack);
  }

  private CountDownLatch initSubject(Map<String, Object> taskData, boolean suppressStack) {
    subject = new Task(taskRef, OWNER_ID, taskData, taskSpec, taskReset, validityChecker, suppressStack);

    return (CountDownLatch) Whitebox.getInternalState(subject, "completionLatch");
  }

  private static class NoOpTaskProcessor implements TaskProcessor {
    @Override public void process(@NotNull Task task) throws InterruptedException {}
  }

  private static class InterruptedTaskProcessor implements TaskProcessor {
    @Override public void process(@NotNull Task task) throws InterruptedException {
      throw new InterruptedException();
    }
  }

  private static class InterruptingTaskProcessor implements TaskProcessor {
    @Override public void process(@NotNull Task task) throws InterruptedException {
      Thread.currentThread().interrupt();
    }
  }
}
