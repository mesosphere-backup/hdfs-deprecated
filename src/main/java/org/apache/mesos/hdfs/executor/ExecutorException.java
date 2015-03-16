package org.apache.mesos.hdfs.executor;

/**
 * thrown when there is a runtime exception during the life of an executor.  Usually a fatal issue resulting
 * in a executor process shutdown.
 */
public class ExecutorException extends RuntimeException {

  public ExecutorException() {
  }

  public ExecutorException(String message) {
    super(message);
  }
}
