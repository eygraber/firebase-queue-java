package com.firebase.queue;

import com.firebase.client.FirebaseError;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Date;

public class Log {
  public interface Logger {
    void log(@NotNull Level level, @NotNull String log, Object... args);
    void debug(@NotNull Level level, @NotNull String log, Object... args);
    void log(@NotNull Throwable error, @NotNull String log, Object... args);
    void debug(@NotNull Throwable error, @NotNull String log, Object... args);
    void log(@NotNull FirebaseError error, @NotNull String log, Object... args);
    void debug(@NotNull FirebaseError error, @NotNull String log, Object... args);
    boolean shouldLogDebug(@NotNull Level level);
  }

  public static boolean DEBUG = true;

  enum Level {
    INFO, WARN, ERROR
  }

  private Log() {}

  private static Logger instance = new DefaultLogger();

  public static void setLogger(@Nullable Logger logger) {
    instance = logger;
  }

  public static void useDefaultLogger() {
    instance = new DefaultLogger();
  }

  static void log(String log, Object... args) {
    if(instance != null) instance.log(Level.INFO, log, args);
  }

  static void debug(String log, Object... args) {
    if(instance != null && instance.shouldLogDebug(Level.INFO)) {
      instance.debug(Level.INFO, log, args);
    }
  }

  static void log(Level level, String log, Object... args) {
    if(instance != null) instance.log(level, log, args);
  }

  static void debug(Level level, String log, Object... args) {
    if(instance != null && instance.shouldLogDebug(level)) {
      instance.debug(level, log, args);
    }
  }

  static void log(Throwable error, String log, Object... args) {
    if(instance != null) instance.log(error, log, args);
  }

  static void debug(Throwable error, String log, Object... args) {
    if(instance != null && instance.shouldLogDebug(Level.ERROR)) {
      instance.debug(error, log, args);
    }
  }

  static void log(FirebaseError error, String log, Object... args) {
    if(instance != null) instance.log(error, log, args);
  }

  static void debug(FirebaseError error, String log, Object... args) {
    if(instance != null && instance.shouldLogDebug(Level.ERROR)) {
      instance.debug(error, log, args);
    }
  }

  static class DefaultLogger implements Logger {
    static final String ANSI_RESET = "\u001B[0m";
    static final String ANSI_RED = "\u001B[31m";
    static final String ANSI_YELLOW = "\u001B[33m";
    static final String ANSI_WHITE = "\u001B[37m";

    static final String LABEL_DEBUG = " [DEBUG] ";
    static final String LABEL_INFO = " [INFO] ";
    static final String LABEL_WARN = " [WARN] ";
    static final String LABEL_ERROR = " [ERROR] ";

    @Override
    public void log(@NotNull Level level, @NotNull String log, Object... args) {
      System.out.println(getAnsiColor(level) + getDateTimeStamp() + getLabel(level) + String.format(log, args) + ANSI_RESET);
    }

    @Override
    public void debug(@NotNull Level level, @NotNull String log, Object... args) {
      System.out.println(getAnsiColor(level) + getDateTimeStamp() + LABEL_DEBUG + String.format(log, args) + ANSI_RESET);
    }

    @Override
    public void log(@NotNull Throwable error, @NotNull String log, Object... args) {
      System.err.println(ANSI_RED + getDateTimeStamp() + LABEL_ERROR + String.format(log, args));
      error.printStackTrace(System.err);
      System.err.println(ANSI_RESET);
    }

    @Override
    public void debug(@NotNull Throwable error, @NotNull String log, Object... args) {
      System.err.println(ANSI_RED + getDateTimeStamp() + LABEL_DEBUG + String.format(log, args));
      error.printStackTrace(System.err);
      System.err.println(ANSI_RESET);
    }

    @Override
    public void log(@NotNull FirebaseError error, @NotNull String log, Object... args) {
      System.err.println(ANSI_RED + getDateTimeStamp() + LABEL_ERROR + String.format(log, args));
      System.err.println("\tcode=" + error.getCode() + " message=" + error.getMessage() + " details=" + error.getDetails());
      System.err.println(ANSI_RESET);
    }

    @Override
    public void debug(@NotNull FirebaseError error, @NotNull String log, Object... args) {
      System.err.println(ANSI_RED + getDateTimeStamp() + LABEL_DEBUG + String.format(log, args));
      System.err.println("\tcode=" + error.getCode() + " message=" + error.getMessage() + " details=" + error.getDetails());
      System.err.println(ANSI_RESET);
    }

    @Override
    public boolean shouldLogDebug(@NotNull Level level) {
      return DEBUG;
    }

    protected String getDateTimeStamp() {
      return new Date().toString();
    }

    private String getAnsiColor(Level level) {
      switch(level) {
        case INFO:
          return ANSI_WHITE;
        case WARN:
          return ANSI_YELLOW;
        case ERROR:
          return ANSI_RED;
        default:
          return ANSI_RESET;
      }
    }

    private String getLabel(Level level) {
      switch(level) {
        case INFO:
          return LABEL_INFO;
        case WARN:
          return LABEL_WARN;
        case ERROR:
          return LABEL_ERROR;
      }

      return " ";
    }
  }
}
