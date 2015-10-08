package org.apache.mesos.hdfs.executor;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.file.FileUtils;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.io.File;
import java.nio.charset.Charset;

/**
 * The executor for the Primary Name Node Machine.
 */
public class NameNodeExecutor extends AbstractNodeExecutor {
  private final Log log = LogFactory.getLog(NameNodeExecutor.class);

  private Task nameNodeTask;
  // TODO (elingg) better handling in livestate and persistent state of zkfc task. Right now they are
  // chained.
  private Task zkfcNodeTask;

  /**
   * The constructor for the primary name node which saves the configuration.
   */
  @Inject
  NameNodeExecutor(HdfsFrameworkConfig hdfsFrameworkConfig) {
    super(hdfsFrameworkConfig);
  }

  /**
   * Main method for executor, which injects the configuration and state and starts the driver.
   */
  public static void main(String[] args) {
    Injector injector = Guice.createInjector();
    final NameNodeExecutor executor = injector.getInstance(NameNodeExecutor.class);
    MesosExecutorDriver driver = new MesosExecutorDriver(executor);
    Runtime.getRuntime().addShutdownHook(new Thread(new TaskShutdownHook(executor, driver)));
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  /**
   * Add tasks to the task list and then start the tasks in the following order.
   * 1) Start Journal Node
   * 2) Receive Activate Message
   * 3) Start Name Node
   * 4) Start ZKFC Node
   */
  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo taskInfo) {
    executorInfo = taskInfo.getExecutor();
    Task task = new Task(taskInfo);
    log.info(String.format("Launching task, taskId=%s cmd='%s'", taskInfo.getTaskId().getValue(), task.getCmd()));
    if (taskInfo.getTaskId().getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      nameNodeTask = task;
      driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(nameNodeTask.getTaskInfo().getTaskId())
        .setState(TaskState.TASK_RUNNING)
        .build());
      TimedHealthCheck healthCheckNN = new TimedHealthCheck(driver, nameNodeTask);
      healthCheckTimer.scheduleAtFixedRate(healthCheckNN,
        hdfsFrameworkConfig.getHealthCheckWaitingPeriod(),
        hdfsFrameworkConfig.getHealthCheckFrequency());
    } else if (taskInfo.getTaskId().getValue().contains(HDFSConstants.ZKFC_NODE_ID)) {
      zkfcNodeTask = task;
      driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(zkfcNodeTask.getTaskInfo().getTaskId())
        .setState(TaskState.TASK_RUNNING)
        .build());
      TimedHealthCheck healthCheckZN = new TimedHealthCheck(driver, zkfcNodeTask);
      healthCheckTimer.scheduleAtFixedRate(healthCheckZN,
        hdfsFrameworkConfig.getHealthCheckWaitingPeriod(),
        hdfsFrameworkConfig.getHealthCheckFrequency());
    }
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {
    log.info("Killing task : " + taskId.getValue());
    Task task = null;
    if (taskId.getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      task = nameNodeTask;
    } else if (taskId.getValue().contains(HDFSConstants.ZKFC_NODE_ID)) {
      task = zkfcNodeTask;
    }

    if (task != null && task.getProcess() != null) {
      task.getProcess().destroy();
      task.setProcess(null);
    }
    driver.sendStatusUpdate(TaskStatus.newBuilder()
      .setTaskId(taskId)
      .setState(TaskState.TASK_KILLED)
      .build());
  }

  @Override
  public void shutdown(ExecutorDriver d) {
    // TODO(elingg) let's shut down the driver more gracefully
    log.info("Executor asked to shutdown");
    if (nameNodeTask != null) {
      killTask(d, nameNodeTask.getTaskInfo().getTaskId());
    }
    if (zkfcNodeTask != null) {
      killTask(d, zkfcNodeTask.getTaskInfo().getTaskId());
    }
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] msg) {
    super.frameworkMessage(driver, msg);
    String messageStr = new String(msg, Charset.defaultCharset());

    log.info(String.format("Received framework message: %s", messageStr));

    if (processRunning(nameNodeTask) && messageStr.equals(HDFSConstants.JOURNAL_NODE_INIT_MESSAGE)) {
      log.info("Ignoring message " + messageStr + " while NN is running");

      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(nameNodeTask.getTaskInfo().getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .setMessage(messageStr)
          .build());
      return;
    }

    File nameDir = new File(hdfsFrameworkConfig.getDataDir() + "/name");
    File backupDir = hdfsFrameworkConfig.getBackupDir() != null
        ? new File(hdfsFrameworkConfig.getBackupDir() + "/" + getNodeId())
        : null;

    if (messageStr.equals(HDFSConstants.NAME_NODE_INIT_MESSAGE)
        || messageStr.equals(HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE)
        || messageStr.equals(HDFSConstants.JOURNAL_NODE_INIT_MESSAGE)) {
      FileUtils.deleteDirectory(nameDir);
      if (!nameDir.mkdirs()) {
        final String errorMsg = "unable to make directory: " + nameDir;
        log.error(errorMsg);
        throw new ExecutorException(errorMsg);
      }

      boolean backupExists = backupDir != null && backupDir.exists();
      if (backupDir != null && !backupExists && !backupDir.mkdirs()) {
        final String errorMsg = "unable to make directory: " + backupDir;
        log.error(errorMsg);
        throw new ExecutorException(errorMsg);
      }

      runCommand(driver, nameNodeTask, "bin/hdfs-mesos-namenode " + messageStr);

      // todo:  (kgs) we need to separate out the launching of these tasks
      if (!processRunning(nameNodeTask)) {
        startProcess(driver, nameNodeTask);
      }
      if (!processRunning(zkfcNodeTask)) {
        startProcess(driver, zkfcNodeTask);
      }
      driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(nameNodeTask.getTaskInfo().getTaskId())
        .setState(TaskState.TASK_RUNNING)
        .setMessage(messageStr)
        .build());
    }
  }

  private String getNodeId() {
    String id = null;

    for (Protos.CommandInfo.URI uri : nameNodeTask.getTaskInfo().getExecutor().getCommand().getUrisList()) {
      String value = uri.getValue();
      String param = "node=";

      int paramIdx = value.indexOf(param);
      if (paramIdx != -1) {
        int paramEnd = value.indexOf('&', paramIdx);
        if (paramEnd == -1) {
          paramEnd = value.length();
        }

        id = value.substring(paramIdx + param.length(), paramEnd);
      }
    }

    if (id == null) {
      throw new ExecutorException("Can't find node id from executor.command.uris");
    }

    return id;
  }

  private boolean processRunning(Task task) {
    boolean running = false;

    if (task.getProcess() != null) {
      try {
        task.getProcess().exitValue();
      } catch (IllegalThreadStateException e) {
        // throws exception if still running
        running = true;
      }
    }
    return running;
  }
}
