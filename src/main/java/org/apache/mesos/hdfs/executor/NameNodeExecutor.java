package org.apache.mesos.hdfs.executor;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.*;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.executor.AbstractNodeExecutor.TimedHealthCheck;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.io.File;
import java.util.Timer;

/**
 * The executor for the Primary Name Node Machine.
 **/
public class NameNodeExecutor extends AbstractNodeExecutor {
  public static final Log log = LogFactory.getLog(NameNodeExecutor.class);

  // Node tasks run by the executor
  private Task nameNodeTask;
  // TODO better handling in livestate and persistent state of zkfc task. Right now they are
  // chained.
  private Task zkfcNodeTask;
  private Task journalNodeTask;

  // Timed Health Check for node health monitoring
  private TimedHealthCheck healthCheckJN;
  private TimedHealthCheck healthCheckNN;
  private TimedHealthCheck healthCheckZN;
  private Timer timer;

  /**
   * The constructor for the primary name node which saves the configuration.
   **/
  @Inject
  NameNodeExecutor(SchedulerConf schedulerConf) {
    super(schedulerConf);
    timer = new Timer(true);
  }

  /**
   * Main method for executor, which injects the configuration and state and starts the driver.
   */
  public static void main(String[] args) {
    Injector injector = Guice.createInjector();
    MesosExecutorDriver driver = new MesosExecutorDriver(
        injector.getInstance(NameNodeExecutor.class));
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  /**
   * Add tasks to the task list and then start the tasks in the following order : 1) Start Journal
   * Node 2) Receive Activate Message 3) Start Name Node 4) Start ZKFC Node
   **/
  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo taskInfo) {
    executorInfo = taskInfo.getExecutor();
    Task task = new Task(taskInfo);
    if (taskInfo.getTaskId().getValue().contains(HDFSConstants.JOURNAL_NODE_ID)) {
      journalNodeTask = task;
      // Start the journal node
      startProcess(driver, journalNodeTask);
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(journalNodeTask.taskInfo.getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .build());
      healthCheckJN = new TimedHealthCheck(driver, journalNodeTask);
      timer.scheduleAtFixedRate(healthCheckJN, 120000, 60000);
    } else if (taskInfo.getTaskId().getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      nameNodeTask = task;
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(nameNodeTask.taskInfo.getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .build());
      healthCheckNN = new TimedHealthCheck(driver, nameNodeTask);
      timer.scheduleAtFixedRate(healthCheckNN, 120000, 60000);
    } else if (taskInfo.getTaskId().getValue().contains(HDFSConstants.ZKFC_NODE_ID)) {
      zkfcNodeTask = task;
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(zkfcNodeTask.taskInfo.getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .build());
      healthCheckZN = new TimedHealthCheck(driver, zkfcNodeTask);
      timer.scheduleAtFixedRate(healthCheckZN, 120000, 60000);
    }
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {
    log.info("Killing task : " + taskId.getValue());
    Task task = null;
    if (taskId.getValue().contains(HDFSConstants.JOURNAL_NODE_ID)) {
      task = journalNodeTask;
    } else if (taskId.getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      task = nameNodeTask;
    } else if (taskId.getValue().contains(HDFSConstants.ZKFC_NODE_ID)) {
      task = zkfcNodeTask;
    }

    if (task != null && task.process != null) {
      task.process.destroy();
      task.process = null;
    }
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] msg) {
    super.frameworkMessage(driver, msg);
    String messageStr = new String(msg);
    File nameDir = new File(schedulerConf.getDataDir() + "/name");
    if (messageStr.equals(HDFSConstants.NAME_NODE_INIT_MESSAGE)
        || messageStr.equals(HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE)) {
      if (nameDir.exists() && messageStr.equals(HDFSConstants.NAME_NODE_INIT_MESSAGE)) {
        log.info(String
            .format("NameNode data directory %s already exists, not formatting",
                nameDir));
      } else {
        deleteFile(nameDir);
        nameDir.mkdirs();
        runCommand(driver, nameNodeTask, "bin/hdfs-mesos-namenode " + messageStr);
      }
      startProcess(driver, nameNodeTask);
      startProcess(driver, zkfcNodeTask);
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(nameNodeTask.taskInfo.getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .setMessage(messageStr)
          .build());
    }
  }
}
