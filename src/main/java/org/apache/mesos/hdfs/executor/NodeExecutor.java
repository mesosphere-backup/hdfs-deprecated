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

import java.io.IOException;
import java.net.Socket;
import java.util.Timer;
import java.util.TimerTask;

/**
 * The executor for a Basic Node (either a Journal Node or Data Node).
 * 
 **/
public class NodeExecutor extends AbstractNodeExecutor {
  public static final Log log = LogFactory.getLog(NodeExecutor.class);
  // Node task run by the executor
  private Task task;

  /**
   * The constructor for the node which saves the configuration.
   * 
   **/
  @Inject
  NodeExecutor(SchedulerConf schedulerConf) {
    super(schedulerConf);
  }

  /**
   * Main method for executor, which injects the configuration and state and starts the driver.
   */
  public static void main(String[] args) {
    Injector injector = Guice.createInjector();

    MesosExecutorDriver driver = new MesosExecutorDriver(injector.getInstance(NodeExecutor.class));
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  /**
   * Add tasks to the task list and then start the tasks.
   */
  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo taskInfo) {
    executorInfo = taskInfo.getExecutor();
    task = new Task(taskInfo);
    if (taskInfo.getTaskId().getValue().contains(HDFSConstants.JOURNAL_NODE_ID)) {
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(taskInfo.getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .setData(taskInfo.getData()).build());
      startProcess(driver, task);
    } else if (taskInfo.getTaskId().getValue().contains(HDFSConstants.DATA_NODE_ID)) {
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(taskInfo.getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .setData(taskInfo.getData()).build());
      if (schedulerConf.usingMesosDns()) {
        PreDNInitTask checker = new PreDNInitTask(driver, task);
        timer.scheduleAtFixedRate(checker, 0, 15000);
      } else {
        startProcess(driver, task);
      }
    }
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {
    log.info("Killing task : " + taskId.getValue());
    if (task.process != null && taskId.equals(task.taskInfo.getTaskId())) {
      task.process.destroy();
      task.process = null;
      sendTaskFailed(driver, task);
    }
  }

  private class PreDNInitTask extends TimerTask {
    ExecutorDriver driver;
    Task task;

    public PreDNInitTask(ExecutorDriver driver, Task task) {
      this.driver = driver;
      this.task = task;
    }

    @Override
    public void run() {
      boolean success = true;
      for (int i = HDFSConstants.TOTAL_NAME_NODES; i > 0; i--) {
        String host = HDFSConstants.NAME_NODE_ID + i + "." + schedulerConf.getFrameworkName() + "." + schedulerConf.getMesosDnsDomain();
        log.info("Checking for " + host);
        try (Socket connected = new Socket(host, HDFSConstants.NAME_NODE_HEALTH_PORT)) {
          log.info("Successfully found " + host + " at port " + HDFSConstants.NAME_NODE_HEALTH_PORT);
        } catch (SecurityException | IOException e) {
          log.info("Couldn't resolve host " + host + " at port " + HDFSConstants.NAME_NODE_HEALTH_PORT);
          success = false;
          break;
        }
      }
      if (success) {
        log.info("Successfully found all nodes needed to continue.");
        this.cancel();
        startProcess(driver, task);
      }
    }
  }
}
