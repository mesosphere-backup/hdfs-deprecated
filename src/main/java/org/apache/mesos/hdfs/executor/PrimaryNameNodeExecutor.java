package org.apache.mesos.hdfs.executor;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.*;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.ProdConfigModule;

import java.io.IOException;

/**
 * The executor for the Primary Name Node Machine.
 * 
 **/
public class PrimaryNameNodeExecutor extends AbstractNodeExecutor {
  public static final Log log = LogFactory.getLog(PrimaryNameNodeExecutor.class);

  /**
   * The constructor for the primary name node which saves the configuration.
   * 
   **/
  @Inject
  PrimaryNameNodeExecutor(SchedulerConf schedulerConf) {
    super(schedulerConf);
  }

  /**
   * Main method for executor, which injects the configuration and state and
   * starts the driver.
   */
  public static void main(String[] args) {
    Injector injector = Guice.createInjector(new ProdConfigModule());
    MesosExecutorDriver driver = new MesosExecutorDriver(
        injector.getInstance(PrimaryNameNodeExecutor.class));
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  /**
   * Add tasks to the task list and then start the tasks in the following order
   * : 1) Start Journal Node 2) Start Name Node 3) Start ZKFC Node
   **/
  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo taskInfo) {
    executorInfo = taskInfo.getExecutor();
    Task task = new Task(taskInfo);
    tasks.put(taskInfo.getTaskId(), task);
    driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId())
        .setState(TaskState.TASK_RUNNING).setData(taskInfo.getData()).build());

    if (tasks.size() == 3) {
      for (TaskID taskId : tasks.keySet()) {
        // Start journal node
        if (taskId.getValue().contains(".journalnode.")) {
          reloadConfig();
          startProcess(driver, tasks.get(taskId));
        }
      }
      for (TaskID taskId : tasks.keySet()) {
        // Initialize and format the primary name node and journal node
        if (taskId.getValue().contains(".namenode.namenode.")) {
          runCommand(driver, tasks.get(taskId), "bin/hdfs-mesos-namenode -i");
          // Start the primary name node
          startProcess(driver, tasks.get(taskId));
        }
      }
      for (TaskID taskId : tasks.keySet()) {
        // Start the zkfc node
        if (taskId.getValue().contains(".zkfc.")) {
          startProcess(driver, tasks.get(taskId));
        }
      }
    }
  }

}
