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
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

/**
 * The executor for a Basic Node (either a Journal Node or Data Node).
 * 
 **/
public class NodeExecutor extends AbstractNodeExecutor {
  public static final Log log = LogFactory.getLog(NodeExecutor.class);
  private Task task;

  private Task dataNodeTask;

  /**
   * The constructor for the node which saves the configuration.
   * 
   **/
  @Inject
  NodeExecutor(SchedulerConf schedulerConf, LiveState liveState) {
    super(schedulerConf, liveState);
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
    log.info(taskInfo.getTaskId().getValue());
    executorInfo = taskInfo.getExecutor();
    task = new Task(taskInfo);
    if (taskInfo.getTaskId().getValue().contains(HDFSConstants.DATA_NODE_ID)) {
      // Initialize the data node and wait for activate message
      dataNodeTask = task;
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(taskInfo.getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .setData(taskInfo.getData()).build());
    } else {
      // for other tasks on this node, just start them normally
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(taskInfo.getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .setData(taskInfo.getData()).build());
      startProcess(driver, task);
    }
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {
    log.info("Killing task : " + taskId.getValue());
    if (task.process != null && taskId.equals(task.taskInfo.getTaskId())) {
      task.process.destroy();
      task.process = null;
    }
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] msg) {
    log.info("Executor received framework message of length: " + msg.length + " bytes");
    String messageStr = new String(msg);
    // launch tasks on init (primary NN), launch tasks on run (secondary NN)
    if (messageStr.equals(HDFSConstants.DATA_NODE_INIT_MESSAGE)) {
      // start the data node after name nodes are visible
      Timer timer = new Timer(true);
      TimerTask waitForNameNodes = new NNPortCheckTask(
          driver,
          liveState.getNameNodeDomainNames(),
          HDFSConstants.NAME_NODE_HTTP_PORT);
      timer.scheduleAtFixedRate(waitForNameNodes, 0, 15000);
    }
  }

  private class NNPortCheckTask extends TimerTask {
    ExecutorDriver driver;
    Set<String> hosts;
    int port;

    public NNPortCheckTask(ExecutorDriver driver, Set<String> hosts, int port) {
      this.driver = driver;
      this.hosts = hosts;
      this.port = port;
    }

    @Override
    public void run() {
      boolean success = true;
      for (String host : hosts) {
        log.info("Checking for " + host + " at port " + port);
        try (Socket connected = new Socket(host, port)) {
          log.info("Successfully found " + host + " at port " + port);
        } catch (SecurityException | IOException e) {
          log.info("Couldn't resolve host " + host + " at port " + port);
          success = false;
          break;
        }
      }
      if (success) {
        log.info("Successfully reached name nodes. Starting process: " + dataNodeTask.taskInfo.getName());
        startProcess(driver, dataNodeTask);
        this.cancel();
      }
    }
  }
}
