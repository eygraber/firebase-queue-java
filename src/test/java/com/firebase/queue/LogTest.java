package com.firebase.queue;

import com.firebase.client.FirebaseError;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.stub;
import static org.mockito.Mockito.verify;

@RunWith(JUnit4.class)
public class LogTest {
  private static final String LOG = "log";

  private Log.Logger logger;

  @Before
  public void setup() {
    logger = mock(Log.Logger.class);
    Log.setLogger(logger);
  }

  @Test
  public void log_withoutALevel_usesLevelInfo() {
    Log.log(LOG);

    verify(logger).log(Log.Level.INFO, LOG);
  }

  @Test
  public void debug_withoutALevel_usesLevelInfo_ifItCanBeUsedWithDebug() {
    stub(logger.shouldLogDebug(Log.Level.INFO)).toReturn(true);

    Log.debug(LOG);

    verify(logger).debug(Log.Level.INFO, LOG);
  }

  @Test
  public void debug_withoutALevel_doesNotLog_ifInfoCannotBeUsedWithDebug() {
    stub(logger.shouldLogDebug(Log.Level.INFO)).toReturn(false);

    Log.debug(LOG);

    verify(logger, never()).debug(Log.Level.INFO, LOG);
  }

  @Test
  public void log_withALevel_usesThatLevel() {
    Log.log(Log.Level.ERROR, LOG);

    verify(logger).log(Log.Level.ERROR, LOG);
  }

  @Test
  public void debug_withALevel_usesThatLevel_ifItCanBeUsedWithDebug() {
    stub(logger.shouldLogDebug(Log.Level.ERROR)).toReturn(true);

    Log.debug(Log.Level.ERROR, LOG);

    verify(logger).debug(Log.Level.ERROR, LOG);
  }

  @Test
  public void debug_withALevel_doesNotLog_ifItCannotBeUsedWithDebug() {
    stub(logger.shouldLogDebug(Log.Level.ERROR)).toReturn(false);

    Log.debug(Log.Level.ERROR, LOG);

    verify(logger, never()).debug(Log.Level.ERROR, LOG);
  }

  @Test
  public void log_withAThrowable_logsIt() {
    Throwable error = mock(Throwable.class);

    Log.log(error, LOG);

    verify(logger).log(error, LOG);
  }

  @Test
  public void debug_withAThrowable_logs_ifErrorCanBeUsedWithDebug() {
    stub(logger.shouldLogDebug(Log.Level.ERROR)).toReturn(true);

    Throwable error = mock(Throwable.class);

    Log.debug(error, LOG);

    verify(logger).debug(error, LOG);
  }

  @Test
  public void debug_withAThrowable_doesNotLog_ifErrorCannotBeUsedWithDebug() {
    stub(logger.shouldLogDebug(Log.Level.ERROR)).toReturn(false);

    Throwable error = mock(Throwable.class);

    Log.debug(error, LOG);

    verify(logger, never()).debug(error, LOG);
  }

  @Test
  public void log_withAFirebaseError_logsIt() {
    FirebaseError error = mock(FirebaseError.class);

    Log.log(error, LOG);

    verify(logger).log(error, LOG);
  }

  @Test
  public void debug_withAFirebaseError_logs_ifErrorCanBeUsedWithDebug() {
    stub(logger.shouldLogDebug(Log.Level.ERROR)).toReturn(true);

    FirebaseError error = mock(FirebaseError.class);

    Log.debug(error, LOG);

    verify(logger).debug(error, LOG);
  }

  @Test
  public void debug_withAFirebaseError_doesNotLog_ifErrorCannotBeUsedWithDebug() {
    stub(logger.shouldLogDebug(Log.Level.ERROR)).toReturn(false);

    FirebaseError error = mock(FirebaseError.class);

    Log.debug(error, LOG);

    verify(logger, never()).debug(error, LOG);
  }
}
