package org.apache.mesos.hdfs.executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;

/**
 * Given a task with an underlying process, blocks until the process is completed, 
 * updates the task state accordingly, and exits the system executor with the same
 * underlying process exit code.
 */
public class TaskSupervisor implements Runnable {

  private final Log log = LogFactory.getLog(TaskSupervisor.class);
  private final ExecutorDriver driver;
  private final Task task;

  public TaskSupervisor(ExecutorDriver driver, Task task) {
    this.driver = driver;
    this.task = task;
  }

  public void run() {
    int exitCode = 255;
    try {
      exitCode = task.getProcess().waitFor();
      switch (exitCode) {
      case 0:
        sendStatusUpdate(TaskState.TASK_FINISHED);
        break;
      default:
        sendStatusUpdate(TaskState.TASK_FAILED);
        break;
      }
    } catch (InterruptedException e) {
      log.error(String.format("Interrupted while supervising %s: %s", 
        task.getTaskInfo().getName(), e.getMessage()));
      sendStatusUpdate(TaskState.TASK_FAILED);
    }
    System.exit(exitCode);
  }

  private void sendStatusUpdate(TaskState state) {
    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(task.getTaskInfo().getTaskId())
        .setState(state)
        .build());
  }

}