package org.apache.mesos.hdfs.executor;

import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The task class for use within the executor.
 */
public class Task {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private Protos.TaskInfo taskInfo;
  private String cmd;
  private Process process;

  public Protos.TaskInfo getTaskInfo() {
    return taskInfo;
  }

  public void setTaskInfo(Protos.TaskInfo taskInfo) {
    this.taskInfo = taskInfo;
  }

  public String getCmd() {
    return cmd;
  }

  public void setCmd(String cmd) {
    this.cmd = cmd;
  }

  public Process getProcess() {
    return process;
  }

  public void setProcess(Process process) {
    this.process = process;
  }

  Task(Protos.TaskInfo taskInfo) {
    this.taskInfo = taskInfo;
    this.cmd = taskInfo.getData().toStringUtf8();
    logger.info(String.format("Launching task, taskId=%s cmd='%s'",
        taskInfo.getTaskId().getValue(), cmd));
  }
}
