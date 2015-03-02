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
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.net.InetAddress;
import java.net.Socket;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * The executor for the Primary Name Node Machine.
 **/
public class NameNodeExecutor extends AbstractNodeExecutor {
  public static final Log log = LogFactory.getLog(NameNodeExecutor.class);

  private LiveState liveState;

  private Task nameNodeTask;
  private Task zkfcNodeTask;
  private Task journalNodeTask;

  /**
   * The constructor for the primary name node which saves the configuration.
   **/
  @Inject
  NameNodeExecutor(SchedulerConf schedulerConf, LiveState liveState) {
    super(schedulerConf);
    this.liveState = liveState;
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
    } else if (taskInfo.getTaskId().getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      // Add the name node task and wait for activate message
      nameNodeTask = task;
    } else if (taskInfo.getTaskId().getValue().contains(HDFSConstants.ZKFC_NODE_ID)) {
      // Add the zkfc node task and wait for activate message
      zkfcNodeTask = task;
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
    log.info("Executor received framework message of length: " + msg.length + " bytes");
    String messageStr = new String(msg);
    // launch tasks on init (primary NN), launch tasks on run (secondary NN)
    if (messageStr.equals(HDFSConstants.NAME_NODE_INIT_MESSAGE)
        || messageStr.equals(HDFSConstants.NAME_NODE_RUN_MESSAGE)) {
      // Initialize the journal node and name node
      // Start the name node
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(nameNodeTask.taskInfo.getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .build());
      // Start the zkfc node
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(zkfcNodeTask.taskInfo.getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .build());
    }
    if (messageStr.equals(HDFSConstants.NAME_NODE_INIT_MESSAGE)) {
      waitUntilJournalNodesAvailable();
      runCommand(driver, nameNodeTask, "bin/hdfs-mesos-namenode " + messageStr);
      startProcess(driver, nameNodeTask);
      startProcess(driver, zkfcNodeTask);
    }
    // send init message (primary NN), send bootstrap message (secondary NN)
    if (messageStr.equals(HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE)) {
      waitUntilNameNodesAvailable();
      runCommand(driver, nameNodeTask, "bin/hdfs-mesos-namenode " + messageStr);
      startProcess(driver, nameNodeTask);
      startProcess(driver, zkfcNodeTask);
    }
  }

  private void waitUntilJournalNodesAvailable() {
    Set<String> hosts = new TreeSet<>();
    if (schedulerConf.usingMesosDns()) {
      for (int i = schedulerConf.getJournalNodeCount() + HDFSConstants.TOTAL_NAME_NODES; i > 0; i--)
        hosts.add(HDFSConstants.JOURNAL_NODE_ID + i +
            "." + schedulerConf.getFrameworkName() +
            "." + schedulerConf.getMesosDnsDomain());
    } else {
      hosts.addAll(liveState.getJournalNodeHosts());
      hosts.addAll(liveState.getNameNodeHosts());
    }
    boolean failed = true;
    while (failed) {
      failed = false;
      for (String host : hosts) {
        try {
          InetAddress.getByName(host);
        } catch (Exception e) {
          log.info("Couldn't resolve host " + host);
          failed = true;
          break;
        }
      }
      if (failed) {
        try {
          log.info("Sleeping for 30 seconds.. zzzz");
          Thread.sleep(30000);
        } catch (Exception e) {
          log.info("Sleep interrupted");
        }
      }
    }
  }
  private void waitUntilNameNodesAvailable() {
    Set<String> hosts = new TreeSet<>();
    if (schedulerConf.usingMesosDns()) {
      for (int i = HDFSConstants.TOTAL_NAME_NODES; i > 0; i--)
        hosts.add(HDFSConstants.NAME_NODE_ID + i +
            "." + schedulerConf.getFrameworkName() +
            "." + schedulerConf.getMesosDnsDomain());
    } else {
      hosts.addAll(liveState.getNameNodeHosts());
    }
    boolean failed = true;
    while (failed) {
      failed = false;
      for (String host : hosts) {
        try {
          InetAddress.getByName(host);
        } catch (Exception e) {
          log.info("Couldn't resolve host " + host);
          failed = true;
          break;
        }
      }
      if (failed) {
        try {
          log.info("Sleeping for 30 seconds.. zzzz");
          Thread.sleep(30000);
        } catch (Exception e) {
          log.info("Sleep interrupted");
        }
      }
    }
  }
}
