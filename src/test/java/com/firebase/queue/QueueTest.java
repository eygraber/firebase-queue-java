package com.firebase.queue;

import com.firebase.client.DataSnapshot;
import com.firebase.client.Firebase;
import com.firebase.client.FirebaseError;
import org.assertj.core.data.Offset;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static com.firebase.queue.TestUtils.getBasicQueueTaskWithKey;
import static com.firebase.queue.TestUtils.getBasicTaskSnapshot;
import static com.firebase.queue.TestUtils.getBasicTaskSnapshotWithKey;
import static com.firebase.queue.TestUtils.getTaskSnapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(JUnit4.class)
public abstract class QueueTest {
  protected FirebaseMock firebaseMock;
  protected Log.Logger logger;

  protected QueueHelper queueHelper;

  @Before
  public void setUp() throws Exception {
    firebaseMock = new FirebaseMock();
    logger = mock(Log.Logger.class);

    Log.setLogger(logger);

    queueHelper = new QueueHelper(firebaseMock);
  }

  @Test
  public void buildingAQueue_getsTheTasksChildRef() {
    startQueue();

    verify(firebaseMock.getRoot()).child(Queue.TASK_CHILD);
  }

  @Test
  public void whenANewTaskIsReceived_viaChildAdded_itIsLogged() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");

    verify(logger).log(Log.Level.INFO, "Received new task - %s", snapshot);
  }

  @Test
  public void whenANewTaskIsReceived_viaChildAdded_itIsPassedToTheExecutor() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");

    verify(queueHelper.queueExecutor).execute(isA(QueueTask.class));
  }

  @Test
  public void whenANewTaskIsReceived_viaChildAdded_itIncrementsTheCountOfExecutingTasks() {
    Queue queue = startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    int executingTasksCount = queue.getExecutingTasksCount();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key"));

    assertThat(queue.getExecutingTasksCount()).isEqualTo(executingTasksCount + 1);
  }

  @Test
  public void whenANewTaskIsReceivedAndProcessed_viaChildAdded_itIncrementsThenDecrementsTheCountOfExecutingTasks() {
    Queue queue = startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    int executingTasksCount = queue.getExecutingTasksCount();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key"));

    assertThat(queue.getExecutingTasksCount()).isEqualTo(executingTasksCount + 1);

    taskStateListener.onTaskFinished(getBasicQueueTaskWithKey("task_key"), null);

    assertThat(queue.getExecutingTasksCount()).isEqualTo(executingTasksCount);
  }

  @Test
  public void whenANewTaskIsReceived_viaChildChanged_itIsLogged() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildChanged(snapshot, "");

    verify(logger).log(Log.Level.INFO, "Received new task - %s", snapshot);
  }

  @Test
  public void whenANewTaskIsReceived_viaChildChanged_itIsPassedToTheExecutor() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildChanged(snapshot, "");

    verify(queueHelper.queueExecutor).execute(isA(QueueTask.class));
  }

  @Test
  public void whenANewTaskIsReceived_viaChildChanged_itIncrementsTheCountOfExecutingTasks() {
    Queue queue = startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    int executingTasksCount = queue.getExecutingTasksCount();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildChanged(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key"));

    assertThat(queue.getExecutingTasksCount()).isEqualTo(executingTasksCount + 1);
  }

  @Test
  public void whenANewTaskIsReceivedAndProcessed_viaChildChanged_itIncrementsThenDecrementsTheCountOfExecutingTasks() {
    Queue queue = startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    int executingTasksCount = queue.getExecutingTasksCount();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildChanged(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key"));

    assertThat(queue.getExecutingTasksCount()).isEqualTo(executingTasksCount + 1);

    taskStateListener.onTaskFinished(getBasicQueueTaskWithKey("task_key"), null);

    assertThat(queue.getExecutingTasksCount()).isEqualTo(executingTasksCount);
  }

  @Test
  public void whenANewTaskIsReceived_andThenTheQueueShutsdown_andThenTheTaskIsProcessed_itDoesNotDecrementCountOfExecutingTasks() {
    Queue queue = startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    reset(queueHelper.queueExecutor);

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key_1"));

    QueueTask queueTask2 = getBasicQueueTaskWithKey("task_key_2");
    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), queueTask2);

    int count = queue.getExecutingTasksCount();

    simulateQueueShutdown(queue);

    taskStateListener.onTaskFinished(queueTask2, null);

    assertThat(queue.getExecutingTasksCount()).isEqualTo(count);
  }

  @Test
  public void whenANewTaskIsReceived_viaChildAdded_afterTheQueueHasBeenShutdown_nothingIsLogged() {
    Queue queue = startQueue();

    reset(logger);

    DataSnapshot snapshot = getBasicTaskSnapshot();

    simulateQueueShutdown(queue);

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");

    verifyZeroInteractions(logger);
  }

  @Test
  public void whenANewTaskIsReceived_viaChildAdded_afterTheQueueHasBeenShutdown_nothingIsPassedToTheExecutor() {
    Queue queue = startQueue();

    reset(queueHelper.queueExecutor);

    DataSnapshot snapshot = getBasicTaskSnapshot();

    simulateQueueShutdown(queue);

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");

    verifyZeroInteractions(queueHelper.queueExecutor);
  }

  @Test
  public void whenANewTaskIsReceived_viaChildAdded_afterTheQueueHasBeenShutdown_itDoesNotIncrementCountOfExecutingTasks() {
    Queue queue = startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    reset(queueHelper.queueExecutor);

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key_1"));

    int executingTasksCount = queue.getExecutingTasksCount();

    simulateQueueShutdown(queue);

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key_2"));

    assertThat(queue.getExecutingTasksCount()).isEqualTo(executingTasksCount);
  }

  @Test
  public void whenANewTaskIsReceived_viaChildChanged_afterTheQueueHasBeenShutdown_nothingIsLogged() {
    Queue queue = startQueue();

    reset(logger);

    DataSnapshot snapshot = getBasicTaskSnapshot();

    simulateQueueShutdown(queue);

    firebaseMock.getTasksChildEventListener().onChildChanged(snapshot, "");

    verifyZeroInteractions(logger);
  }

  @Test
  public void whenANewTaskIsReceived_viaChildChanged_afterTheQueueHasBeenShutdown_nothingIsPassedToTheExecutor() {
    Queue queue = startQueue();

    reset(queueHelper.queueExecutor);

    DataSnapshot snapshot = getBasicTaskSnapshot();

    simulateQueueShutdown(queue);

    firebaseMock.getTasksChildEventListener().onChildChanged(snapshot, "");

    verifyZeroInteractions(queueHelper.queueExecutor);
  }

  @Test
  public void whenANewTaskIsReceived_viaChildChanged_afterTheQueueHasBeenShutdown_itDoesNotIncrementCountOfExecutingTasks() {
    Queue queue = startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    reset(queueHelper.queueExecutor);

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildChanged(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key_1"));

    int executingTasksCount = queue.getExecutingTasksCount();

    simulateQueueShutdown(queue);

    firebaseMock.getTasksChildEventListener().onChildChanged(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key_2"));

    assertThat(queue.getExecutingTasksCount()).isEqualTo(executingTasksCount);
  }

  @Test
  public void whenListeningForNewTasksIsCancelled_itIsLogged() {
    startQueue();

    FirebaseError error = FirebaseError.fromException(new RuntimeException("Something went wrong"));

    firebaseMock.getTasksChildEventListener().onCancelled(error);

    verify(logger).log(error, "There was an error listening for children with a " + Task.STATE_KEY + " of " + queueHelper.taskSpec.getStartState());
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildAdded_andThisTimeoutTaskHasNotBeenReceivedBefore_itIsLogged() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey("key");

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    verify(logger).log(Log.Level.INFO, "Received new task to monitor for timeouts - %s (timeout in " + 0 + " ms)", snapshot);
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildAdded_andThisTimeoutTaskHasBeenReceivedBeforeViaChildAdded_itIsLogged() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey("key");

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    verify(logger).log(Log.Level.INFO, "Received updated task to monitor for timeouts - %s (timeout in " + 0 + " ms)", snapshot);
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildAdded_andThisTimeoutTaskHasBeenReceivedBeforeViaChildChanged_itIsLogged() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey("key");

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    verify(logger).log(Log.Level.INFO, "Received updated task to monitor for timeouts - %s (timeout in " + 0 + " ms)", snapshot);
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildAdded_andThisTimeoutTaskHasNotBeenReceivedBefore_itIsScheduledOnTheTimeoutExecutor() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey("key");

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    verify(queueHelper.timeoutExecutor).schedule(isA(Runnable.class), eq(0L), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildAdded_andThisTimeoutTaskHasBeenReceivedBeforeViaChildAdded_itIsRemovedFromTheTimeoutExecutor_thenScheduledOnTheTimeoutExecutor() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey("key");

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    reset(queueHelper.timeoutExecutor);

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    verify(queueHelper.timeoutExecutor).remove(isA(Runnable.class));
    verify(queueHelper.timeoutExecutor).schedule(isA(Runnable.class), eq(0L), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildAdded_andThisTimeoutTaskHasBeenReceivedBeforeViaChildChanged_itIsRemovedFromTheTimeoutExecutor_thenScheduledOnTheTimeoutExecutor() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey("key");

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    reset(queueHelper.timeoutExecutor);

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    verify(queueHelper.timeoutExecutor).remove(isA(Runnable.class));
    verify(queueHelper.timeoutExecutor).schedule(isA(Runnable.class), eq(0L), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildChanged_andThisTimeoutTaskHasNotBeenReceivedBefore_itIsLogged() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey("key");

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    verify(logger).log(Log.Level.INFO, "Received new task to monitor for timeouts - %s (timeout in " + 0 + " ms)", snapshot);
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildChanged_andThisTimeoutTaskHasBeenReceivedBeforeViaChildChanged_itIsLogged() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey("key");

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    verify(logger).log(Log.Level.INFO, "Received updated task to monitor for timeouts - %s (timeout in " + 0 + " ms)",  snapshot);
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildChanged_andThisTimeoutTaskHasBeenReceivedBeforeViaChildAdded_itIsLogged() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey("key");

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    verify(logger).log(Log.Level.INFO, "Received updated task to monitor for timeouts - %s (timeout in " + 0 + " ms)", snapshot);
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildChanged_andThisTimeoutTaskHasNotBeenReceivedBefore_itIsScheduledOnTheTimeoutExecutor() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey("key");

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    verify(queueHelper.timeoutExecutor).schedule(isA(Runnable.class), eq(0L), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildChanged_andThisTimeoutTaskHasBeenReceivedBeforeViaChildChanged_itIsRemovedFromTheTimeoutExecutor_thenScheduledOnTheTimeoutExecutor() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey("key");

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    reset(queueHelper.timeoutExecutor);

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    verify(queueHelper.timeoutExecutor).remove(isA(Runnable.class));
    verify(queueHelper.timeoutExecutor).schedule(isA(Runnable.class), eq(0L), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildChanged_andThisTimeoutTaskHasBeenReceivedBeforeViaChildAdded_itIsRemovedFromTheTimeoutExecutor_thenScheduledOnTheTimeoutExecutor() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey("key");

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    reset(queueHelper.timeoutExecutor);

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    verify(queueHelper.timeoutExecutor).remove(isA(Runnable.class));
    verify(queueHelper.timeoutExecutor).schedule(isA(Runnable.class), eq(0L), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildAdded_itIsScheduledOnTheTimeoutExecutor_withTheDelaySpecifiedInTheSpec_offsetByTheStateChanged() {
    startQueue();

    DataSnapshot snapshot = getTaskSnapshot("key", new HashMap<String, Object>() {{
      put(Task.STATE_CHANGED_KEY, System.currentTimeMillis() - 5000L);
    }});

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(queueHelper.timeoutExecutor).schedule(isA(Runnable.class), captor.capture(), eq(TimeUnit.MILLISECONDS));

    Long scheduleDelay = captor.getValue();

    // allow for some time drift because of System.currentTimeMillis()
    assertThat(scheduleDelay).isCloseTo(queueHelper.taskSpec.getTimeout() - 5000L, Offset.offset(10L));
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildAdded_itIsScheduledOnTheTimeoutExecutorImmediately_ifTheTimeoutMinusTheTimeSinceTheTaskChangedIsLessThan0() {
    startQueue();

    DataSnapshot snapshot = getTaskSnapshot("key", new HashMap<String, Object>() {{
      put(Task.STATE_CHANGED_KEY, 0); // by saying the task changed at time 0, we are ensuring that timeout - taskChangedTime < 0
    }});

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(queueHelper.timeoutExecutor).schedule(isA(Runnable.class), captor.capture(), eq(TimeUnit.MILLISECONDS));

    Long scheduleDelay = captor.getValue();

    assertThat(scheduleDelay).isEqualTo(0L);
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildAdded_itIsScheduledOnTheTimeoutExecutorInTimeoutMilliseconds_ifTheCurrentTimeIsBehindWhenTheTaskChanged() {
    startQueue();

    DataSnapshot snapshot = getTaskSnapshot("key", new HashMap<String, Object>() {{
      put(Task.STATE_CHANGED_KEY, System.currentTimeMillis() * 2);
    }});

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(queueHelper.timeoutExecutor).schedule(isA(Runnable.class), captor.capture(), eq(TimeUnit.MILLISECONDS));

    Long scheduleDelay = captor.getValue();

    assertThat(scheduleDelay).isEqualTo(queueHelper.taskSpec.getTimeout());
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildAdded_itIsScheduledOnTheTimeoutExecutorImmediately_ifStateChangedIsNotSet() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey("key");

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    verify(queueHelper.timeoutExecutor).schedule(isA(Runnable.class), eq(0L), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildChanged_itIsScheduledOnTheTimeoutExecutor_withTheDelaySpecifiedInTheSpec_offsetByTheStateChanged() {
    startQueue();

    DataSnapshot snapshot = getTaskSnapshot("key", new HashMap<String, Object>() {{
      put(Task.STATE_CHANGED_KEY, System.currentTimeMillis() - 5000L);
    }});

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(queueHelper.timeoutExecutor).schedule(isA(Runnable.class), captor.capture(), eq(TimeUnit.MILLISECONDS));

    Long scheduleDelay = captor.getValue();

    // allow for some time drift because of System.currentTimeMillis()
    assertThat(scheduleDelay).isCloseTo(queueHelper.taskSpec.getTimeout() - 5000L, Offset.offset(10L));
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildChanged_itIsScheduledOnTheTimeoutExecutorImmediately_ifTheTimeoutMinusTheTimeSinceTheTaskChangedIsLessThan0() {
    startQueue();

    DataSnapshot snapshot = getTaskSnapshot("key", new HashMap<String, Object>() {{
      put(Task.STATE_CHANGED_KEY, 0); // by saying the task changed at time 0, we are ensuring that timeout - taskChangedTime < 0
    }});

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(queueHelper.timeoutExecutor).schedule(isA(Runnable.class), captor.capture(), eq(TimeUnit.MILLISECONDS));

    Long scheduleDelay = captor.getValue();

    assertThat(scheduleDelay).isEqualTo(0L);
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildChanged_itIsScheduledOnTheTimeoutExecutorInTimeoutMilliseconds_ifTheCurrentTimeIsBehindWhenTheTaskChanged() {
    startQueue();

    DataSnapshot snapshot = getTaskSnapshot("key", new HashMap<String, Object>() {{
      put(Task.STATE_CHANGED_KEY, System.currentTimeMillis() * 2);
    }});

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(queueHelper.timeoutExecutor).schedule(isA(Runnable.class), captor.capture(), eq(TimeUnit.MILLISECONDS));

    Long scheduleDelay = captor.getValue();

    assertThat(scheduleDelay).isEqualTo(queueHelper.taskSpec.getTimeout());
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildChanged_itIsScheduledOnTheTimeoutExecutorImmediately_ifStateChangedIsNotSet() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey("key");

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    verify(queueHelper.timeoutExecutor).schedule(isA(Runnable.class), eq(0L), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildAdded_afterTheQueueHasBeenShutdown_nothingIsPassedToTheLogger() {
    Queue queue = startQueue();

    reset(queueHelper.timeoutExecutor, logger);

    DataSnapshot snapshot = getBasicTaskSnapshot();

    simulateQueueShutdown(queue);

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    verifyZeroInteractions(logger);
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildAdded_afterTheQueueHasBeenShutdown_nothingIsPassedToTheTimeoutExecutor() {
    Queue queue = startQueue();

    reset(queueHelper.timeoutExecutor);

    DataSnapshot snapshot = getBasicTaskSnapshot();

    simulateQueueShutdown(queue);

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    verifyZeroInteractions(queueHelper.timeoutExecutor);
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildChanged_afterTheQueueHasBeenShutdown_nothingIsPassedToTheLogger() {
    Queue queue = startQueue();

    reset(queueHelper.timeoutExecutor, logger);

    DataSnapshot snapshot = getBasicTaskSnapshot();

    simulateQueueShutdown(queue);

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    verifyZeroInteractions(logger);
  }

  @Test
  public void whenANewTimeoutTaskIsReceived_viaChildChanged_afterTheQueueHasBeenShutdown_nothingIsPassedToTheTimeoutExecutor() {
    Queue queue = startQueue();

    reset(queueHelper.timeoutExecutor);

    DataSnapshot snapshot = getBasicTaskSnapshot();

    simulateQueueShutdown(queue);

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    verifyZeroInteractions(queueHelper.timeoutExecutor);
  }

  @Test
  public void whenATimeoutTaskIsRemoved_ifWeDoNotHaveAReferenceToIt_nothingIsRemovedFromTheTimeoutExecutor() {
    startQueue();

    reset(queueHelper.timeoutExecutor);

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTimeoutChildEventListener().onChildRemoved(snapshot);

    verifyZeroInteractions(queueHelper.timeoutExecutor);
  }

  @Test
  public void whenATimeoutTaskIsRemoved_ifWeDoNotHaveAReferenceToIt_nothingIsLogged() {
    startQueue();

    reset(logger);

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTimeoutChildEventListener().onChildRemoved(snapshot);

    verifyZeroInteractions(logger);
  }

  @Test
  public void whenATimeoutTaskIsRemoved_ifWeHaveAReferenceToItViaChildAdded_itIsRemovedFromTheTimeoutExecutor() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    reset(queueHelper.timeoutExecutor);

    firebaseMock.getTimeoutChildEventListener().onChildRemoved(snapshot);

    verify(queueHelper.timeoutExecutor).remove(isA(Runnable.class));
  }

  @Test
  public void whenATimeoutTaskIsRemoved_ifWeHaveAReferenceToItViaChildAdded_itIsLogged() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    firebaseMock.getTimeoutChildEventListener().onChildRemoved(snapshot);

    verify(logger).log(Log.Level.INFO, "Cancelling timeout for %s", snapshot);
  }

  @Test
  public void whenATimeoutTaskIsRemoved_ifWeHaveAReferenceToItViaChildAdded_afterTheQueueHasBeenShutDown_nothingIsRemovedFromTheTimeoutExecutor() {
    Queue queue = startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    simulateQueueShutdown(queue);

    reset(queueHelper.timeoutExecutor);

    firebaseMock.getTimeoutChildEventListener().onChildRemoved(snapshot);

    verifyZeroInteractions(queueHelper.timeoutExecutor);
  }

  @Test
  public void whenATimeoutTaskIsRemoved_ifWeHaveAReferenceToItViaChildAdded_afterTheQueueHasBeenShutDown_nothingIsLogged() {
    Queue queue = startQueue();

    reset(queueHelper.timeoutExecutor);

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    simulateQueueShutdown(queue);

    reset(logger);

    firebaseMock.getTimeoutChildEventListener().onChildRemoved(snapshot);

    verifyZeroInteractions(logger);
  }

  @Test
  public void whenATimeoutTaskIsRemoved_ifWeHaveAReferenceToItViaChildChanged_itIsRemovedFromTheTimeoutExecutor() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    reset(queueHelper.timeoutExecutor);

    firebaseMock.getTimeoutChildEventListener().onChildRemoved(snapshot);

    verify(queueHelper.timeoutExecutor).remove(isA(Runnable.class));
  }

  @Test
  public void whenATimeoutTaskIsRemoved_ifWeHaveAReferenceToItViaChildChanged_itIsLogged() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    firebaseMock.getTimeoutChildEventListener().onChildRemoved(snapshot);

    verify(logger).log(Log.Level.INFO, "Cancelling timeout for %s", snapshot);
  }

  @Test
  public void whenATimeoutTaskIsRemoved_ifWeHaveAReferenceToItViaChildChanged_afterTheQueueHasBeenShutDown_nothingIsRemovedFromTheTimeoutExecutor() {
    Queue queue = startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    simulateQueueShutdown(queue);

    reset(queueHelper.timeoutExecutor);

    firebaseMock.getTimeoutChildEventListener().onChildRemoved(snapshot);

    verifyZeroInteractions(queueHelper.timeoutExecutor);
  }

  @Test
  public void whenATimeoutTaskIsRemoved_ifWeHaveAReferenceToItViaChildChanged_afterTheQueueHasBeenShutDown_nothingIsLogged() {
    Queue queue = startQueue();

    reset(queueHelper.timeoutExecutor);

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTimeoutChildEventListener().onChildChanged(snapshot, "");

    simulateQueueShutdown(queue);

    reset(logger);

    firebaseMock.getTimeoutChildEventListener().onChildRemoved(snapshot);

    verifyZeroInteractions(logger);
  }

  @Test
  public void whenListeningForNewTimeoutTasksIsCancelled_itIsLogged() {
    startQueue();

    FirebaseError error = FirebaseError.fromException(new RuntimeException("Something went wrong"));

    firebaseMock.getTimeoutChildEventListener().onCancelled(error);

    verify(logger).log(error, "There was an error listening for timeouts with a " + Task.STATE_KEY + " of " + queueHelper.taskSpec.getInProgressState());
  }

  @Test
  public void whenANewTaskIsReceived_viaChildAdded_andItTimesOut_whileItIsRunning_itIsLogged() {
    startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key"));

    Runnable timeoutRunnable = verifyTimeoutTaskScheduled("task_key");
    timeoutRunnable.run();

    verify(logger).log(Log.Level.INFO, "Task task_key has timedout while running");
  }

  @Test
  public void whenANewTaskIsReceived_andItTimesOut_whileItIsRunning_itIsCancelled() {
    startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    QueueTask task = getBasicQueueTaskWithKey("task_key");
    taskStateListener.onTaskStart(mock(Thread.class), task);

    Runnable timeoutRunnable = verifyTimeoutTaskScheduled("task_key");
    timeoutRunnable.run();

    verify(task).cancel();
  }

  @Test
  public void whenANewTaskIsReceived_andItTimesOut_beforeItRuns_itIsLogged() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");

    Runnable timeoutRunnable = verifyTimeoutTaskScheduled("task_key");
    timeoutRunnable.run();

    verify(logger).log(Log.Level.INFO, "Task task_key has timedout");
  }

  @Test
  public void whenANewTaskIsReceived_andItTimesOut_whileItIsRunning_itIsReset() {
    startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key"));

    DataSnapshot timeoutSnapshot = getBasicTaskSnapshotWithKey("task_key");
    Firebase mockRef = mock(Firebase.class);
    stub(timeoutSnapshot.getRef()).toReturn(mockRef);
    Runnable timeoutRunnable = verifyTimeoutTaskScheduled(timeoutSnapshot);
    timeoutRunnable.run();

    verify(queueHelper.taskReset).reset(mockRef, queueHelper.taskSpec.getInProgressState());
  }

  @Test
  public void whenANewTaskIsReceived_andItTimesOut_beforeItRuns_itIsReset() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");

    DataSnapshot timeoutSnapshot = getBasicTaskSnapshotWithKey("task_key");
    Firebase mockRef = mock(Firebase.class);
    stub(timeoutSnapshot.getRef()).toReturn(mockRef);
    Runnable timeoutRunnable = verifyTimeoutTaskScheduled(timeoutSnapshot);
    timeoutRunnable.run();

    verify(queueHelper.taskReset).reset(mockRef, queueHelper.taskSpec.getInProgressState());
  }

  @Test
  public void whenANewTaskIsReceived_andTheTimeoutIsRemoved_butThenTheTaskTimesOut_whileItIsRunning_itIsNotLogged() {
    startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key"));

    DataSnapshot timeoutSnapshot = getBasicTaskSnapshotWithKey("task_key");
    Firebase mockRef = mock(Firebase.class);
    stub(timeoutSnapshot.getRef()).toReturn(mockRef);
    Runnable timeoutRunnable = verifyTimeoutTaskScheduled(timeoutSnapshot);

    firebaseMock.getTimeoutChildEventListener().onChildRemoved(timeoutSnapshot);

    reset(logger);

    timeoutRunnable.run();

    verifyZeroInteractions(logger);
  }

  @Test
  public void whenANewTaskIsReceived_andTheTimeoutIsRemoved_butThenTheTaskTimesOut_whileItIsRunning_itIsNotCancelled() {
    startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    QueueTask task = getBasicQueueTaskWithKey("task_key");
    taskStateListener.onTaskStart(mock(Thread.class), task);

    DataSnapshot timeoutSnapshot = getBasicTaskSnapshotWithKey("task_key");
    Firebase mockRef = mock(Firebase.class);
    stub(timeoutSnapshot.getRef()).toReturn(mockRef);
    Runnable timeoutRunnable = verifyTimeoutTaskScheduled(timeoutSnapshot);

    firebaseMock.getTimeoutChildEventListener().onChildRemoved(timeoutSnapshot);

    timeoutRunnable.run();

    verify(task, never()).cancel();
  }

  @Test
  public void whenANewTaskIsReceived_andTheTimeoutIsRemoved_butThenTheTaskTimesOut_whileItIsRunning_itIsNotReset() {
    startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key"));

    DataSnapshot timeoutSnapshot = getBasicTaskSnapshotWithKey("task_key");
    Firebase mockRef = mock(Firebase.class);
    stub(timeoutSnapshot.getRef()).toReturn(mockRef);
    Runnable timeoutRunnable = verifyTimeoutTaskScheduled(timeoutSnapshot);

    firebaseMock.getTimeoutChildEventListener().onChildRemoved(timeoutSnapshot);

    reset(queueHelper.taskReset);

    timeoutRunnable.run();

    verifyZeroInteractions(queueHelper.taskReset);
  }

  @Test
  public void whenANewTaskIsReceived_andTheTimeoutIsRemoved_butThenTheTaskTimesOut_beforeItRuns_itIsNotReset() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");

    DataSnapshot timeoutSnapshot = getBasicTaskSnapshotWithKey("task_key");
    Firebase mockRef = mock(Firebase.class);
    stub(timeoutSnapshot.getRef()).toReturn(mockRef);
    Runnable timeoutRunnable = verifyTimeoutTaskScheduled(timeoutSnapshot);

    firebaseMock.getTimeoutChildEventListener().onChildRemoved(timeoutSnapshot);

    reset(queueHelper.taskReset);

    timeoutRunnable.run();

    verifyZeroInteractions(queueHelper.taskReset);
  }

  @Test
  public void whenANewTaskIsReceived_andTheTimeoutIsRemoved_butThenTheTaskTimesOut_beforeItRuns_itIsNotLogged() {
    startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");

    DataSnapshot timeoutSnapshot = getBasicTaskSnapshotWithKey("task_key");
    Firebase mockRef = mock(Firebase.class);
    stub(timeoutSnapshot.getRef()).toReturn(mockRef);
    Runnable timeoutRunnable = verifyTimeoutTaskScheduled(timeoutSnapshot);

    firebaseMock.getTimeoutChildEventListener().onChildRemoved(timeoutSnapshot);

    reset(logger);

    timeoutRunnable.run();

    verifyZeroInteractions(logger);
  }

  @Test
  public void whenANewTaskIsReceived_andTheQueueIsShutdown_andThenTheTaskTimesOut_whileItIsRunning_itIsNotLogged() {
    Queue queue = startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key"));

    DataSnapshot timeoutSnapshot = getBasicTaskSnapshotWithKey("task_key");
    Firebase mockRef = mock(Firebase.class);
    stub(timeoutSnapshot.getRef()).toReturn(mockRef);
    Runnable timeoutRunnable = verifyTimeoutTaskScheduled(timeoutSnapshot);

    simulateQueueShutdown(queue);

    reset(logger);

    timeoutRunnable.run();

    verifyZeroInteractions(logger);
  }

  @Test
  public void whenANewTaskIsReceived_andTheQueueIsShutdown_andThenTheTaskTimesOut_whileItIsRunning_itIsNotCancelled() {
    Queue queue = startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    QueueTask task = getBasicQueueTaskWithKey("task_key");
    taskStateListener.onTaskStart(mock(Thread.class), task);

    DataSnapshot timeoutSnapshot = getBasicTaskSnapshotWithKey("task_key");
    Firebase mockRef = mock(Firebase.class);
    stub(timeoutSnapshot.getRef()).toReturn(mockRef);
    Runnable timeoutRunnable = verifyTimeoutTaskScheduled(timeoutSnapshot);

    simulateQueueShutdown(queue);

    timeoutRunnable.run();

    verify(task, never()).cancel();
  }

  @Test
  public void whenANewTaskIsReceived_andTheQueueIsShutdown_andThenTheTaskTimesOut_whileItIsRunning_itIsNotReset() {
    Queue queue = startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key"));

    DataSnapshot timeoutSnapshot = getBasicTaskSnapshotWithKey("task_key");
    Firebase mockRef = mock(Firebase.class);
    stub(timeoutSnapshot.getRef()).toReturn(mockRef);
    Runnable timeoutRunnable = verifyTimeoutTaskScheduled(timeoutSnapshot);

    simulateQueueShutdown(queue);

    reset(queueHelper.taskReset);

    timeoutRunnable.run();

    verifyZeroInteractions(queueHelper.taskReset);
  }

  @Test
  public void whenANewTaskIsReceived_andTheQueueIsShutdown_andThenTheTaskTimesOut_beforeItRuns_itIsNotReset() {
    Queue queue = startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");

    DataSnapshot timeoutSnapshot = getBasicTaskSnapshotWithKey("task_key");
    Firebase mockRef = mock(Firebase.class);
    stub(timeoutSnapshot.getRef()).toReturn(mockRef);
    Runnable timeoutRunnable = verifyTimeoutTaskScheduled(timeoutSnapshot);

    simulateQueueShutdown(queue);

    reset(queueHelper.taskReset);

    timeoutRunnable.run();

    verifyZeroInteractions(queueHelper.taskReset);
  }

  @Test
  public void whenANewTaskIsReceived_andTheQueueIsShutdown_andThenTheTaskTimesOut_beforeItRuns_itIsNotLogged() {
    Queue queue = startQueue();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");

    DataSnapshot timeoutSnapshot = getBasicTaskSnapshotWithKey("task_key");
    Firebase mockRef = mock(Firebase.class);
    stub(timeoutSnapshot.getRef()).toReturn(mockRef);
    Runnable timeoutRunnable = verifyTimeoutTaskScheduled(timeoutSnapshot);

    simulateQueueShutdown(queue);

    reset(logger);

    timeoutRunnable.run();

    verifyZeroInteractions(logger);
  }

  @Test
  public void whenTheQueueIsShutdown_theCountOfExecutingTasksIsZero() {
    Queue queue = startQueue();

    QueueExecutor.TaskStateListener taskStateListener = verifyTaskStartedListenerSet();

    DataSnapshot snapshot = getBasicTaskSnapshot();

    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    firebaseMock.getTasksChildEventListener().onChildAdded(snapshot, "");
    firebaseMock.getTasksChildEventListener().onChildChanged(snapshot, "");
    firebaseMock.getTasksChildEventListener().onChildChanged(snapshot, "");
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key_1"));
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key_2"));
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key_3"));
    taskStateListener.onTaskStart(mock(Thread.class), getBasicQueueTaskWithKey("task_key_4"));

    assertThat(queue.getExecutingTasksCount()).isEqualTo(4);

    queue.shutdown();

    assertThat(queue.getExecutingTasksCount()).isEqualTo(0);
  }

  protected abstract Queue instantiateQueue();
  protected abstract Queue startQueue();

  /**
   * Makes the queue think it is shut down, but does not actually remove the listeners and shut down the executors
   */
  protected static void simulateQueueShutdown(Queue queue) {
    Whitebox.setInternalState(queue, "shutdown", true);
  }

  protected QueueExecutor.TaskStateListener verifyTaskStartedListenerSet() {
    ArgumentCaptor<QueueExecutor.TaskStateListener> captor = ArgumentCaptor.forClass(QueueExecutor.TaskStateListener.class);
    verify(queueHelper.queueExecutor).setTaskStateListener(captor.capture());

    return captor.getValue();
  }

  protected Runnable verifyTimeoutTaskScheduled(String key) {
    return verifyTimeoutTaskScheduled(getBasicTaskSnapshotWithKey(key));
  }

  protected Runnable verifyTimeoutTaskScheduled(DataSnapshot snapshot) {
    firebaseMock.getTimeoutChildEventListener().onChildAdded(snapshot, "");

    ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
    verify(queueHelper.timeoutExecutor).schedule(captor.capture(), eq(0L), eq(TimeUnit.MILLISECONDS));
    return captor.getValue();
  }
}
