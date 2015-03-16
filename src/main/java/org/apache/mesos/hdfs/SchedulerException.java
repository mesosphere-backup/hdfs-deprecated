package org.apache.mesos.hdfs;

/**
 * Exceptions in the scheduler which likely result in the scheduler being shutdown.
 */
public class SchedulerException extends RuntimeException {

  public SchedulerException(String msg) {
    super(msg);
  }

  public SchedulerException() {
  }
}
