package com.firebase.queue;

import com.firebase.client.FirebaseError;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static com.firebase.queue.Log.DefaultLogger.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;

@RunWith(JUnit4.class)
public class DefaultLoggerTest {
  private static final String LOG = "log";
  private static final String LOG_WITH_ARGS = "log %s";
  private static final String ARGS = "args";
  private static final String LOG_WITH_ARGS_FORMATTED = "log args";

  private static final int ERROR_CODE = 1;
  private static final String ERROR_MESSAGE = "error";
  private static final String ERROR_DETAILS = "details";

  private static final String DATE = "date";

  @Rule public final SystemOutRule systemOutRule = new SystemOutRule().enableLog().muteForSuccessfulTests();
  @Rule public final SystemErrRule systemErrRule = new SystemErrRule().enableLog().muteForSuccessfulTests();

  private static class TestDefaultLogger extends Log.DefaultLogger {
    @Override
    protected String getDateTimeStamp() {
      return DATE;
    }
  }

  private TestDefaultLogger logger;

  @Before
  public void setup() {
    logger = new TestDefaultLogger();
    Log.setLogger(logger);
  }

  @Test
  public void shouldLogDebug_returnsTrueIfDebugIsSetToTrue() {
    Log.DEBUG = true;

    assertThat(logger.shouldLogDebug(Log.Level.INFO)).isEqualTo(true);
    assertThat(logger.shouldLogDebug(Log.Level.WARN)).isEqualTo(true);
    assertThat(logger.shouldLogDebug(Log.Level.ERROR)).isEqualTo(true);
  }

  @Test
  public void shouldLogDebug_returnsFalseIfDebugIsSetToFalse() {
    Log.DEBUG = false;

    assertThat(logger.shouldLogDebug(Log.Level.INFO)).isEqualTo(false);
    assertThat(logger.shouldLogDebug(Log.Level.WARN)).isEqualTo(false);
    assertThat(logger.shouldLogDebug(Log.Level.ERROR)).isEqualTo(false);
  }

  @Test
  public void infoLog_writesAnInfoLogToOut() {
    logger.log(Log.Level.INFO, LOG);

    assertThat(systemOutRule.getLogWithNormalizedLineSeparator()).isEqualTo(ANSI_WHITE + DATE + LABEL_INFO + LOG + ANSI_RESET + "\n");
  }

  @Test
  public void warnLog_writesAWarnLogToOut() {
    logger.log(Log.Level.WARN, LOG);

    assertThat(systemOutRule.getLogWithNormalizedLineSeparator()).isEqualTo(ANSI_YELLOW + DATE + LABEL_WARN + LOG + ANSI_RESET + "\n");
  }

  @Test
  public void errorLog_writesAnErrorLogToOut() {
    logger.log(Log.Level.ERROR, LOG);

    assertThat(systemOutRule.getLogWithNormalizedLineSeparator()).isEqualTo(ANSI_RED + DATE + LABEL_ERROR + LOG + ANSI_RESET + "\n");
  }

  @Test
  public void logWithArgs_writesALogWithTheStringFormatted() {
    logger.log(Log.Level.INFO, LOG_WITH_ARGS, ARGS);

    assertThat(systemOutRule.getLogWithNormalizedLineSeparator()).isEqualTo(ANSI_WHITE + DATE + LABEL_INFO + LOG_WITH_ARGS_FORMATTED + ANSI_RESET + "\n");
  }

  @Test
  public void infoDebug_writesAnInfoLogToOut() {
    logger.debug(Log.Level.INFO, LOG);

    assertThat(systemOutRule.getLogWithNormalizedLineSeparator()).isEqualTo(ANSI_WHITE + DATE + LABEL_DEBUG + LOG + ANSI_RESET + "\n");
  }

  @Test
  public void warnDebug_writesAWarnLogToOut() {
    logger.debug(Log.Level.WARN, LOG);

    assertThat(systemOutRule.getLogWithNormalizedLineSeparator()).isEqualTo(ANSI_YELLOW + DATE + LABEL_DEBUG + LOG + ANSI_RESET + "\n");
  }

  @Test
  public void errorDebug_writesAnErrorLogToOut() {
    logger.debug(Log.Level.ERROR, LOG);

    assertThat(systemOutRule.getLogWithNormalizedLineSeparator()).isEqualTo(ANSI_RED + DATE + LABEL_DEBUG + LOG + ANSI_RESET + "\n");
  }

  @Test
  public void debugWithArgs_writesALogWithTheStringFormatted() {
    logger.debug(Log.Level.INFO, LOG_WITH_ARGS, ARGS);

    assertThat(systemOutRule.getLogWithNormalizedLineSeparator()).isEqualTo(ANSI_WHITE + DATE + LABEL_DEBUG + LOG_WITH_ARGS_FORMATTED + ANSI_RESET + "\n");
  }

  @Test
  public void logAThrowable_writesAnErrorLogToErr() {
    Throwable error = mock(Throwable.class);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        System.err.println(ERROR_MESSAGE);
        return null;
      }
    }).when(error).printStackTrace(System.err);

    logger.log(error, LOG);

    assertThat(systemErrRule.getLogWithNormalizedLineSeparator()).isEqualTo(ANSI_RED + DATE + LABEL_ERROR + LOG + "\n" + ERROR_MESSAGE+ "\n" + ANSI_RESET + "\n");
  }

  @Test
  public void logAThrowableWithArgs_writesAnErrorLogToErrWithTheStringFormatted() {
    Throwable error = mock(Throwable.class);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        System.err.println(ERROR_MESSAGE);
        return null;
      }
    }).when(error).printStackTrace(System.err);

    logger.log(error, LOG_WITH_ARGS, ARGS);

    assertThat(systemErrRule.getLogWithNormalizedLineSeparator()).isEqualTo(ANSI_RED + DATE + LABEL_ERROR + LOG_WITH_ARGS_FORMATTED + "\n" + ERROR_MESSAGE+ "\n" + ANSI_RESET + "\n");
  }

  @Test
  public void logAThrowable_doesNotWriteAnErrorLogToOut() {
    logger.log(mock(Throwable.class), LOG);

    assertThat(systemOutRule.getLogWithNormalizedLineSeparator()).isEqualTo("");
  }

  @Test
  public void debugAThrowable_writesADebugLogToErr() {
    Throwable error = mock(Throwable.class);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        System.err.println(ERROR_MESSAGE);
        return null;
      }
    }).when(error).printStackTrace(System.err);

    logger.debug(error, LOG);

    assertThat(systemErrRule.getLogWithNormalizedLineSeparator()).isEqualTo(ANSI_RED + DATE + LABEL_DEBUG + LOG + "\n" + ERROR_MESSAGE+ "\n" + ANSI_RESET + "\n");
  }

  @Test
  public void debugAThrowableWithArgs_writesADebugLogToErrWithTheStringFormatted() {
    Throwable error = mock(Throwable.class);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        System.err.println(ERROR_MESSAGE);
        return null;
      }
    }).when(error).printStackTrace(System.err);

    logger.debug(error, LOG_WITH_ARGS, ARGS);

    assertThat(systemErrRule.getLogWithNormalizedLineSeparator()).isEqualTo(ANSI_RED + DATE + LABEL_DEBUG + LOG_WITH_ARGS_FORMATTED + "\n" + ERROR_MESSAGE+ "\n" + ANSI_RESET + "\n");
  }

  @Test
  public void debugAThrowable_doesNotWriteADebugLogToOut() {
    logger.debug(mock(Throwable.class), LOG);

    assertThat(systemOutRule.getLogWithNormalizedLineSeparator()).isEqualTo("");
  }

  @Test
  public void logAFirebaseError_writesAnErrorLogToErr() {
    FirebaseError error = mock(FirebaseError.class);
    stub(error.getCode()).toReturn(ERROR_CODE);
    stub(error.getMessage()).toReturn(ERROR_MESSAGE);
    stub(error.getDetails()).toReturn(ERROR_DETAILS);

    logger.log(error, LOG);

    String errorResult = "\tcode=" + error.getCode() + " message=" + error.getMessage() + " details=" + error.getDetails();

    assertThat(systemErrRule.getLogWithNormalizedLineSeparator()).isEqualTo(ANSI_RED + DATE + LABEL_ERROR + LOG + "\n" + errorResult + "\n" + ANSI_RESET + "\n");
  }

  @Test
  public void logAFirebaseErrorWithArgs_writesAnErrorLogToErrWithTheStringFormatted() {
    FirebaseError error = mock(FirebaseError.class);
    stub(error.getCode()).toReturn(ERROR_CODE);
    stub(error.getMessage()).toReturn(ERROR_MESSAGE);
    stub(error.getDetails()).toReturn(ERROR_DETAILS);

    logger.log(error, LOG_WITH_ARGS, ARGS);

    String errorResult = "\tcode=" + error.getCode() + " message=" + error.getMessage() + " details=" + error.getDetails();

    assertThat(systemErrRule.getLogWithNormalizedLineSeparator()).isEqualTo(ANSI_RED + DATE + LABEL_ERROR + LOG_WITH_ARGS_FORMATTED + "\n" + errorResult + "\n" + ANSI_RESET + "\n");
  }

  @Test
  public void logAFirebase_doesNotWriteAnErrorLogToOut() {
    logger.log(mock(FirebaseError.class), LOG);

    assertThat(systemOutRule.getLogWithNormalizedLineSeparator()).isEqualTo("");
  }

  @Test
  public void debugAFirebaseError_writesADebugLogToErr() {
    FirebaseError error = mock(FirebaseError.class);
    stub(error.getCode()).toReturn(ERROR_CODE);
    stub(error.getMessage()).toReturn(ERROR_MESSAGE);
    stub(error.getDetails()).toReturn(ERROR_DETAILS);

    logger.debug(error, LOG);

    String errorResult = "\tcode=" + error.getCode() + " message=" + error.getMessage() + " details=" + error.getDetails();

    assertThat(systemErrRule.getLogWithNormalizedLineSeparator()).isEqualTo(ANSI_RED + DATE + LABEL_DEBUG + LOG + "\n" + errorResult + "\n" + ANSI_RESET + "\n");
  }

  @Test
  public void debugAFirebaseErrorWithArgs_writesADebugLogToErrWithTheStringFormatted() {
    FirebaseError error = mock(FirebaseError.class);
    stub(error.getCode()).toReturn(ERROR_CODE);
    stub(error.getMessage()).toReturn(ERROR_MESSAGE);
    stub(error.getDetails()).toReturn(ERROR_DETAILS);

    logger.debug(error, LOG_WITH_ARGS, ARGS);

    String errorResult = "\tcode=" + error.getCode() + " message=" + error.getMessage() + " details=" + error.getDetails();

    assertThat(systemErrRule.getLogWithNormalizedLineSeparator()).isEqualTo(ANSI_RED + DATE + LABEL_DEBUG + LOG_WITH_ARGS_FORMATTED + "\n" + errorResult + "\n" + ANSI_RESET + "\n");
  }

  @Test
  public void debugAFirebaseError_doesNotWriteADebugLogToOut() {
    logger.debug(mock(FirebaseError.class), LOG);

    assertThat(systemOutRule.getLogWithNormalizedLineSeparator()).isEqualTo("");
  }
}
