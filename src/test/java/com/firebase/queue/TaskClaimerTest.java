package com.firebase.queue;

import com.firebase.client.DataSnapshot;
import com.firebase.client.Firebase;
import com.firebase.client.FirebaseError;
import com.firebase.client.MutableData;
import com.firebase.client.ServerValue;
import com.firebase.client.Transaction;
import com.firebase.client.snapshot.Node;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.firebase.queue.TestUtils.getBasicTaskSnapshotWithKey;
import static com.firebase.queue.TestUtils.getDefaultTaskSpecSnapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest(
    Transaction.class
)
public class TaskClaimerTest {
  private static final String OWNER_ID = "owner";
  private static final String TASK_KEY = "task_key";

  private TaskClaimer subject;

  private Firebase taskRef;
  private TaskSpec taskSpec;
  private TaskReset taskReset;

  private Log.Logger logger;

  @Before
  public void setUp() throws Exception {
    taskRef = mock(Firebase.class);
    stub(taskRef.getKey()).toReturn(TASK_KEY);

    taskSpec = spy(new TaskSpec(getDefaultTaskSpecSnapshot()));

    taskReset = mock(TaskReset.class);

    logger = mock(Log.Logger.class);
    stub(logger.shouldLogDebug(any(Log.Level.class))).toReturn(true);
    Log.DEBUG = true;
    Log.setLogger(logger);
  }

  @Test(expected = IllegalStateException.class)
  public void claimTask_calledMoreThanOnce_throwsAnIllegalStateException() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();
    ClaimTaskThread claimTaskThread2 = runClaimTask();
    latch.countDown();

    claimTaskThread.join();
    claimTaskThread2.join();

    claimTaskThread.checkException();
    claimTaskThread2.checkException();
  }

  @Test
  public void claimTask_interrupted_writesALog() throws InterruptedException {
    initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    claimTaskThread.interrupt();
    claimTaskThread.join();

    String key = taskRef.getKey();
    verify(logger).debug(isA(InterruptedException.class), eq("TaskClaimer: Interrupted while waiting for transaction to complete (key=" + key + ", owner= " + OWNER_ID + ")"));
  }

  @Test
  public void claimTask_interrupted_resetsTheTask() throws InterruptedException {
    initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    claimTaskThread.interrupt();
    claimTaskThread.join();

    verify(taskReset).reset(taskRef, OWNER_ID, taskSpec.getInProgressState());
  }

  @Test
  public void claimTask_interrupted_setTheInterruptFlag() throws InterruptedException {
    initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    claimTaskThread.interrupt();

    assertThat(claimTaskThread.isInterrupted()).isEqualTo(true);

    claimTaskThread.join();
  }

  @Test
  public void claimTask_interrupted_doesNotCreateATaskGenerator() throws InterruptedException {
    initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    claimTaskThread.interrupt();
    claimTaskThread.join();

    assertThat(claimTaskThread.getTaskGenerator()).isNull();
  }

  @Test
  public void claimTask_interruptedBeforeTheTransactionRuns_abortsTheTransaction() throws InterruptedException {
    initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    claimTaskThread.interrupt();
    claimTaskThread.join();

    Transaction.Handler handler = verifyTransactionStarted();
    Transaction.Result result = handler.doTransaction(mockTransactionResult(null, false));

    assertThat(result.isSuccess()).isFalse();
  }

  @Test
  public void claimTask_interruptedBeforeTheTransactionRuns_doesNotCreateATaskGenerator() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    claimTaskThread.interrupt();
    claimTaskThread.join();

    Transaction.Handler handler = verifyTransactionStarted();
    handler.doTransaction(mockTransactionResult(null, false));

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();

    assertThat(claimTaskThread.getTaskGenerator()).isNull();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsNull_writesALog() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(null, true);
    handler.doTransaction(data);

    verify(logger).debug(Log.Level.INFO, "TaskClaimer: Can't claim task because someone else removed it (key=" + taskRef.getKey() + ", owner" + OWNER_ID + ")");

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsNull_causesTheTransactionToSucceed() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(null, true);
    Transaction.Result result = handler.doTransaction(data);
    assertThat(result.isSuccess()).isTrue();
    assertThat(result.getNode().getValue()).isNull();

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsNull_doesNotCreateATaskGenerator() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(null, true);
    handler.doTransaction(data);

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();

    assertThat(claimTaskThread.getTaskGenerator()).isNull();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsNotAMap_writesALog() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult("hello", true);
    handler.doTransaction(data);

    verify(logger).debug(Log.Level.WARN, "TaskClaimer: Can't claim task because it was malformed (key=%s, owner=%s, task=%s)", taskRef.getKey(), OWNER_ID, "hello");

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsNotAMap_writesErrorDetailsToTheTaskRef() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult("hello", true);
    Transaction.Result result = handler.doTransaction(data);

    Map<String, Object> value = (Map<String, Object>) result.getNode().getValue();

    assertThat(value.get(Task.STATE_KEY)).isEqualTo(taskSpec.getErrorState());
    assertThat(value.get(Task.STATE_CHANGED_KEY)).isEqualTo(ServerValue.TIMESTAMP);
    assertThat(value.get(Task.ERROR_DETAILS_KEY)).isInstanceOf(Map.class);

    Map<String, Object> errorDetails = (Map<String, Object>) value.get(Task.ERROR_DETAILS_KEY);
    assertThat(errorDetails.get(Task.ERROR_KEY)).isEqualTo("Task was malformed");
    assertThat(errorDetails.get(Task.ORIGINAL_TASK_KEY)).isEqualTo("hello");

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsNotAMap_causesTheTransactionToSucceed() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult("hello", true);
    Transaction.Result result = handler.doTransaction(data);

    assertThat(result.isSuccess()).isTrue();

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsNotAMap_doesNotCreateATaskGenerator() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult("hello", true);
    handler.doTransaction(data);

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();

    assertThat(claimTaskThread.getTaskGenerator()).isNull();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNull_andItMatchesTheTaskSpecStartState_claimsTheTask() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, null);
    }}, true);
    Transaction.Result result = handler.doTransaction(data);

    Map<String, Object> value = (Map<String, Object>) result.getNode().getValue();

    assertThat(value.get(Task.STATE_KEY)).isEqualTo(taskSpec.getInProgressState());
    assertThat(value.get(Task.STATE_CHANGED_KEY)).isEqualTo(ServerValue.TIMESTAMP);
    assertThat(value.get(Task.OWNER_KEY)).isEqualTo(OWNER_ID);
    assertThat(value.get("some_key")).isEqualTo("some_value");
    assertThat(value.get(Task.ERROR_DETAILS_KEY)).isNull();

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNull_andItMatchesTheTaskSpecStartState_causesTheTransactionToSucceed() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, null);
    }}, true);
    Transaction.Result result = handler.doTransaction(data);

    assertThat(result.isSuccess()).isTrue();

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNull_andItMatchesTheTaskSpecStartState_doesNotCreateATaskGenerator() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, null);
    }}, true);
    handler.doTransaction(data);

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();

    assertThat(claimTaskThread.getTaskGenerator()).isNull();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNotNull_andItMatchesTheTaskSpecStartState_claimsTheTask() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, "some_start_state");
    }}, true);
    stub(taskSpec.getStartState()).toReturn("some_start_state");
    Transaction.Result result = handler.doTransaction(data);

    Map<String, Object> value = (Map<String, Object>) result.getNode().getValue();

    assertThat(value.get(Task.STATE_KEY)).isEqualTo(taskSpec.getInProgressState());
    assertThat(value.get(Task.STATE_CHANGED_KEY)).isEqualTo(ServerValue.TIMESTAMP);
    assertThat(value.get(Task.OWNER_KEY)).isEqualTo(OWNER_ID);
    assertThat(value.get("some_key")).isEqualTo("some_value");
    assertThat(value.get(Task.ERROR_DETAILS_KEY)).isNull();

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNotNull_andItMatchesTheTaskSpecStartState_causesTheTransactionToSucceed() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, "some_start_state");
    }}, true);
    stub(taskSpec.getStartState()).toReturn("some_start_state");
    Transaction.Result result = handler.doTransaction(data);

    assertThat(result.isSuccess()).isTrue();

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNotNull_andItMatchesTheTaskSpecStartState_doesNotCreateATaskGenerator() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, "some_start_state");
    }}, true);
    stub(taskSpec.getStartState()).toReturn("some_start_state");
    handler.doTransaction(data);

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();

    assertThat(claimTaskThread.getTaskGenerator()).isNull();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNull_andItDoesNotMatchTheTaskSpecStartState_writesALog() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, null);
    }}, false);
    stub(taskSpec.getStartState()).toReturn("some_start_state");
    handler.doTransaction(data);

    verify(logger).debug(Log.Level.WARN, "TaskClaimer: Can't claim task because its _state (null) did not match our _start_state (some_start_state) (key=" + taskRef.getKey() + ", owner=" + OWNER_ID + ")");

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNull_andItDoesNotMatchTheTaskSpecStartState_doesNotClaimTheTask() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, null);
    }}, false);
    stub(taskSpec.getStartState()).toReturn("some_start_state");
    Transaction.Result result = handler.doTransaction(data);

    Map<String, Object> value = (Map<String, Object>) result.getNode().getValue();
    assertThat(value.get(Task.STATE_KEY)).isNull();

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNull_andItDoesNotMatchTheTaskSpecStartState_abortsTheTransaction() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, null);
    }}, false);
    stub(taskSpec.getStartState()).toReturn("some_start_state");
    Transaction.Result result = handler.doTransaction(data);

    assertThat(result.isSuccess()).isFalse();

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNull_andItDoesNotMatchTheTaskSpecStartState_doesNotCreateATaskGenerator() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, null);
    }}, false);
    stub(taskSpec.getStartState()).toReturn("some_start_state");
    handler.doTransaction(data);

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();

    assertThat(claimTaskThread.getTaskGenerator()).isNull();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNotNull_andItDoesNotMatchTheTaskSpecStartState_writesALog() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, "some_start_state");
    }}, false);
    stub(taskSpec.getStartState()).toReturn("some_other_start_state");
    handler.doTransaction(data);

    verify(logger).debug(Log.Level.WARN, "TaskClaimer: Can't claim task because its _state (some_start_state) did not match our _start_state (some_other_start_state) (key=" + taskRef.getKey() + ", owner=" + OWNER_ID + ")");

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNotNull_andItDoesNotMatchTheTaskSpecStartState_doesNotClaimTheTask() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, "some_start_state");
    }}, false);
    stub(taskSpec.getStartState()).toReturn("some_other_start_state");
    Transaction.Result result = handler.doTransaction(data);

    Map<String, Object> value = (Map<String, Object>) result.getNode().getValue();
    assertThat(value.get(Task.STATE_KEY)).isEqualTo("some_start_state");

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNotNull_andItDoesNotMatchTheTaskSpecStartState_abortsTheTransaction() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, "some_start_state");
    }}, false);
    stub(taskSpec.getStartState()).toReturn("some_other_start_state");
    Transaction.Result result = handler.doTransaction(data);

    assertThat(result.isSuccess()).isFalse();

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNotNull_andItDoesNotMatchTheTaskSpecStartState_doesNotCreateATaskGenerator() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, "some_start_state");
    }}, false);
    stub(taskSpec.getStartState()).toReturn("some_other_start_state");
    handler.doTransaction(data);

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();

    assertThat(claimTaskThread.getTaskGenerator()).isNull();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNotNull_andTheTaskSpecStartStateIsNull_writesALog() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, "some_start_state");
    }}, false);
    stub(taskSpec.getStartState()).toReturn(null);
    handler.doTransaction(data);

    verify(logger).debug(Log.Level.WARN, "TaskClaimer: Can't claim task because its _state (some_start_state) did not match our _start_state (null) (key=" + taskRef.getKey() + ", owner=" + OWNER_ID + ")");

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNotNull_andTheTaskSpecStartStateIsNull_doesNotClaimTheTask() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, "some_start_state");
    }}, false);
    stub(taskSpec.getStartState()).toReturn(null);
    Transaction.Result result = handler.doTransaction(data);

    Map<String, Object> value = (Map<String, Object>) result.getNode().getValue();
    assertThat(value.get(Task.STATE_KEY)).isEqualTo("some_start_state");

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNotNull_andTheTaskSpecStartStateIsNull_abortsTheTransaction() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, "some_start_state");
    }}, false);
    stub(taskSpec.getStartState()).toReturn(null);
    Transaction.Result result = handler.doTransaction(data);

    assertThat(result.isSuccess()).isFalse();

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();
  }

  @Test
  public void claimTask_whenTheDataAtTheRefIsAMap_andTheStateIsNotNull_andTheTaskSpecStartStateIsNull_doesNotCreateATaskGenerator() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();
    MutableData data = mockTransactionResult(new HashMap<String, Object>() {{
      put("some_key", "some_value");
      put(Task.STATE_KEY, "some_start_state");
    }}, false);
    stub(taskSpec.getStartState()).toReturn(null);
    handler.doTransaction(data);

    latch.countDown();

    claimTaskThread.join();

    claimTaskThread.checkException();

    assertThat(claimTaskThread.getTaskGenerator()).isNull();
  }

  @Test
  public void claimTask_whenTheTransactionIsComplete_ifThereIsAnError_andItIsInterrupted_doesNotCreateATaskGenerator() throws Throwable {
    initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    claimTaskThread.interrupt();
    claimTaskThread.join();

    handler.onComplete(mock(FirebaseError.class), false, getBasicTaskSnapshotWithKey(taskRef.getKey()));

    assertThat(claimTaskThread.getTaskGenerator()).isNull();
  }

  @Test
  public void claimTask_whenTheTransactionIsComplete_ifThereIsAnError_andMaxRetriesHasNotBeenReached_writesALog() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    latch.countDown();
    claimTaskThread.join();

    FirebaseError error = mock(FirebaseError.class);
    handler.onComplete(error, false, getBasicTaskSnapshotWithKey(taskRef.getKey()));

    verify(logger).debug(error, "TaskClaimer: Received error while claiming task...retrying (key=" + taskRef.getKey() + ", owner" + OWNER_ID + ")");
  }

  @Test
  public void claimTask_whenTheTransactionIsComplete_ifThereIsAnError_andMaxRetriesHasNotBeenReached_runsTheTransactionAgain() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    latch.countDown();
    claimTaskThread.join();

    FirebaseError error = mock(FirebaseError.class);
    handler.onComplete(error, false, getBasicTaskSnapshotWithKey(taskRef.getKey()));

    verify(taskRef).runTransaction(handler, false);
  }

  @Test
  public void claimTask_whenTheTransactionIsComplete_ifThereIsAnError_andMaxRetriesHasBeenReached_writesALog() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    latch.countDown();
    claimTaskThread.join();

    FirebaseError error = mock(FirebaseError.class);
    for(int i = 0; i < Queue.MAX_TRANSACTION_RETRIES; i++) {
      handler.onComplete(error, false, getBasicTaskSnapshotWithKey(taskRef.getKey()));
      handler = verifyTransactionStartedAtLeastOnce();
    }

    verify(logger).debug(error, "TaskClaimer: Can't claim task because transaction errored too many times, no longer retrying (key=" + taskRef.getKey() + ", owner=" + OWNER_ID + ")");
  }

  @Test
  public void claimTask_whenTheTransactionIsComplete_ifThereIsNoError_andItIsInterrupted_resetsTheTask() throws Throwable {
    initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    claimTaskThread.interrupt();
    claimTaskThread.join();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(true);
    handler.onComplete(null, true, snapshot);

    verify(taskReset).reset(taskRef, OWNER_ID, taskSpec.getInProgressState());
  }

  @Test
  public void claimTask_whenTheTransactionIsComplete_ifThereIsNoError_andItIsCommittedAndTheSnapshotExists_andItIsInterrupted_doesNotCreateATaskGenerator() throws Throwable {
    initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    claimTaskThread.interrupt();
    claimTaskThread.join();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(true);
    handler.onComplete(null, true, snapshot);

    assertThat(claimTaskThread.getTaskGenerator()).isNull();
  }

  @Test
  public void claimTask_whenTheTransactionIsComplete_ifThereIsNoError_andItIsCommittedAndTheSnapshotExists_andItIsMalformed_doesNotWriteALogNorResetTheTask() throws Throwable {
    CountDownLatch latch = initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    latch.countDown();
    claimTaskThread.join();

    MutableData data = mockTransactionResult("hello", true);
    handler.doTransaction(data);

    reset(logger, taskReset);

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(true);
    handler.onComplete(null, true, snapshot);

    verifyZeroInteractions(logger, taskReset);
  }

  @Test
  public void claimTask_whenTheTransactionIsComplete_ifThereIsNoError_andItIsCommittedAndTheSnapshotExists_andItIsMalformed_doesNotCreateATaskGenerator() throws Throwable {
    initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    MutableData data = mockTransactionResult("hello", true);
    handler.doTransaction(data);

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(true);
    handler.onComplete(null, true, snapshot);

    claimTaskThread.join();

    assertThat(claimTaskThread.getTaskGenerator()).isNull();
  }

  @Test
  public void claimTask_whenTheTransactionIsComplete_ifThereIsNoError_andItIsCommittedAndTheSnapshotExists_writesALog() throws Throwable {
    initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(true);
    handler.onComplete(null, true, snapshot);

    claimTaskThread.join();

    verify(logger).debug(Log.Level.INFO, "TaskClaimer: Claimed task (key=" + taskRef.getKey() + ", owner=" + OWNER_ID + ")");
  }

  @Test
  public void claimTask_whenTheTransactionIsComplete_ifThereIsNoError_andItIsCommittedAndTheSnapshotExists_createsATaskGenerator() throws Throwable {
    initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(true);
    handler.onComplete(null, true, snapshot);

    claimTaskThread.join();

    assertThat(claimTaskThread.getTaskGenerator()).isNotNull();
  }

  @Test
  public void claimTask_sanitized_whenTheTransactionIsComplete_ifThereIsNoError_andItIsCommittedAndTheSnapshotExists_createsATaskGenerator_withASanitizedMap() throws Throwable {
    Map<String, Object> values = getTaskData();

    initSubject(values, true);
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(true);
    stub(snapshot.getValue(Map.class)).toReturn(values);
    handler.onComplete(null, true, snapshot);

    claimTaskThread.join();

    assertThat(values.get(Task.STATE_KEY)).isNull();
    assertThat(values.get(Task.STATE_CHANGED_KEY)).isNull();
    assertThat(values.get(Task.OWNER_KEY)).isNull();
    assertThat(values.get(Task.ERROR_DETAILS_KEY)).isNull();
    assertThat(values.get("some_other_key")).isEqualTo("some_value");

    assertThat(claimTaskThread.getTaskGenerator()).isNotNull();
  }

  @Test
  public void claimTask_notSanitized_whenTheTransactionIsComplete_ifThereIsNoError_andItIsCommittedAndTheSnapshotExists_createsATaskGenerator_withTheOriginalMap() throws Throwable {
    Map<String, Object> values = getTaskData();

    initSubject(values, false);
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(true);
    stub(snapshot.getValue(Map.class)).toReturn(values);
    handler.onComplete(null, true, snapshot);

    claimTaskThread.join();

    assertThat(values.get(Task.STATE_KEY)).isEqualTo("state");
    assertThat(values.get(Task.STATE_CHANGED_KEY)).isEqualTo("state_changed");
    assertThat(values.get(Task.OWNER_KEY)).isEqualTo("owner");
    assertThat(values.get(Task.ERROR_DETAILS_KEY)).isEqualTo("error_details");
    assertThat(values.get("some_other_key")).isEqualTo("some_value");

    assertThat(claimTaskThread.getTaskGenerator()).isNotNull();
  }

  @Test
  public void claimTask_whenTheTransactionIsComplete_ifThereIsNoError_andItIsCommittedButTheSnapshotDoesNotExist_doesNotCreateATaskGenerator() throws Throwable {
    initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(false);
    handler.onComplete(null, true, snapshot);

    claimTaskThread.join();

    assertThat(claimTaskThread.getTaskGenerator()).isNull();
  }

  @Test
  public void claimTask_whenTheTransactionIsComplete_ifThereIsNoError_andItIsNotCommittedButTheSnapshotDoesExist_doesNotCreateATaskGenerator() throws Throwable {
    initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(true);
    handler.onComplete(null, false, snapshot);

    claimTaskThread.join();

    assertThat(claimTaskThread.getTaskGenerator()).isNull();
  }

  @Test
  public void claimTask_whenTheTransactionIsComplete_ifThereIsNoError_butItIsNotCommittedAndTheSnapshotDoesNotExist_doesNotCreateATaskGenerator() throws Throwable {
    initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(false);
    handler.onComplete(null, false, snapshot);

    claimTaskThread.join();

    assertThat(claimTaskThread.getTaskGenerator()).isNull();
  }

  @Test
  public void claimTask_interrupted_butAlreadyClaimed_doesNotWrite3Logs() throws Throwable {
    initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(true);
    handler.onComplete(null, true, snapshot);

    claimTaskThread.interrupt();
    claimTaskThread.join();

    verify(logger, never()).debug(Log.Level.INFO, "Failed to claim task " + taskRef.getKey() + " on " + OWNER_ID);
    verify(logger, never()).debug(Log.Level.INFO, "Interrupted while trying to claim a task (" + taskRef.getKey() + ") for " + OWNER_ID);
    verify(logger, never()).debug(Log.Level.INFO, "Tried claiming task " + taskRef.getKey() + " on " + OWNER_ID + " but we were interrupted");
  }

  @Test
  public void claimTask_interrupted_butAlreadyClaimed_doesCreateATaskGenerator() throws Throwable {
    initSubject();
    ClaimTaskThread claimTaskThread = runClaimTask();

    Transaction.Handler handler = verifyTransactionStarted();

    DataSnapshot snapshot = getBasicTaskSnapshotWithKey(taskRef.getKey());
    stub(snapshot.exists()).toReturn(true);
    handler.onComplete(null, true, snapshot);

    claimTaskThread.interrupt();
    claimTaskThread.join();

    assertThat(claimTaskThread.getTaskGenerator()).isNotNull();
  }

  private CountDownLatch initSubject() {
    return initSubject(null, true);
  }

  private CountDownLatch initSubject(Map<String, Object> taskData, boolean sanitize) {
    subject = spy(new TaskClaimer(OWNER_ID, taskRef, taskSpec, taskReset, sanitize));

    TaskClaimer.TaskGenerator generator = mock(TaskClaimer.TaskGenerator.class);
    if(taskData == null) {
      doReturn(generator)
          .when(subject).getTaskGenerator(eq(taskRef), anyMap());
    }
    else {
      doReturn(generator)
          .when(subject).getTaskGenerator(eq(taskRef), eq(taskData));
    }
    return (CountDownLatch) Whitebox.getInternalState(subject, "taskLatch");
  }

  private Transaction.Handler verifyTransactionStarted() {
    ArgumentCaptor<Transaction.Handler> captor = ArgumentCaptor.forClass(Transaction.Handler.class);
    verify(taskRef).runTransaction(captor.capture(), eq(false));
    return captor.getValue();
  }

  private Transaction.Handler verifyTransactionStartedAtLeastOnce() {
    ArgumentCaptor<Transaction.Handler> captor = ArgumentCaptor.forClass(Transaction.Handler.class);
    verify(taskRef, atLeastOnce()).runTransaction(captor.capture(), eq(false));
    return captor.getValue();
  }

  private MutableData mockTransactionResult(Object value, boolean success) {
    mockStatic(Transaction.class);
    final MutableData data = mock(MutableData.class);
    stub(data.getValue()).toReturn(value);
    if(value instanceof Map) {
      stub(data.getValue(Map.class)).toReturn((Map) value);
    }
    Transaction.Result result = mock(Transaction.Result.class);
    stub(result.isSuccess()).toReturn(success);
    final Node node = mock(Node.class);
    stub(node.getValue()).toReturn(value);
    stub(result.getNode()).toReturn(node);
    if(success) {
      when(Transaction.success(data)).thenReturn(result);
    }
    else {
      when(Transaction.abort()).thenReturn(result);
    }

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        stub(data.getValue()).toReturn(args[0]);
        stub(node.getValue()).toReturn(args[0]);
        return null;
      }
    }).when(data).setValue(any());

    return data;
  }

  private ClaimTaskThread runClaimTask() {
    CountDownLatch taskClaimerStartedLatch = new CountDownLatch(1);
    ClaimTaskThread claimTaskThread = new ClaimTaskThread(subject, taskClaimerStartedLatch);

    try {
      taskClaimerStartedLatch.await();
    }
    catch(InterruptedException ignore) {
      // it looks like we can get interrupted here on occasion
      // we could propogate it, but wouldn't that make these tests flaky???
    }

    while(!claimTaskThread.isTaskClaimerStarted()) {
      try {
        Thread.sleep(1);
      }
      catch(InterruptedException ignore) {
        // it looks like we can get interrupted here on occasion
        // we could propogate it, but wouldn't that make these tests flaky???
      }
    }

    return claimTaskThread;
  }

  private static Map<String, Object> getTaskData() {
    return new HashMap<String, Object>() {{
      put(Task.STATE_KEY, "state");
      put(Task.STATE_CHANGED_KEY, "state_changed");
      put(Task.OWNER_KEY, "owner");
      put(Task.ERROR_DETAILS_KEY, "error_details");
      put("some_other_key", "some_value");
    }};
  }

  private static class ClaimTaskThread extends Thread {
    private final TaskClaimer taskClaimer;
    private final CountDownLatch taskClaimerStartedLatch;

    private TaskClaimer.TaskGenerator taskGenerator = null;
    private Throwable throwable = null;

    public ClaimTaskThread(final TaskClaimer taskClaimer, CountDownLatch taskClaimerStartedLatch) {
      this.taskClaimer = taskClaimer;
      this.taskClaimerStartedLatch = taskClaimerStartedLatch;

      start();
    }

    @Override
    public void run() {
      try {
        taskClaimerStartedLatch.countDown();
        taskGenerator = taskClaimer.claimTask();
      }
      catch(Throwable t) {
        throwable = t;
      }
    }

    public TaskClaimer.TaskGenerator getTaskGenerator() {
      return taskGenerator;
    }

    public void checkException() throws Throwable {
      if(throwable != null) {
        throw throwable;
      }
    }

    public boolean isTaskClaimerStarted() {
      return taskClaimer.isClaiming();
    }
  }
}
