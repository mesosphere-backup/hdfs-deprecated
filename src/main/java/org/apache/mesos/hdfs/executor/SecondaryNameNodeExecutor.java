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

/**
 * The executor for the Secondary Name Node Machine.
 * 
 **/
public class SecondaryNameNodeExecutor extends AbstractNodeExecutor {
  public static final Log log = LogFactory.getLog(SecondaryNameNodeExecutor.class);

  /**
   * The constructor for the secondary name node which saves the configuration.
   * 
   **/
  @Inject
  SecondaryNameNodeExecutor(SchedulerConf schedulerConf) {
    super(schedulerConf);
  }

  /**
   * Main method for executor, which injects the configuration and state and
   * starts the driver.
   */
  public static void main(String[] args) {
    Injector injector = Guice.createInjector(new ProdConfigModule());
    MesosExecutorDriver driver = new MesosExecutorDriver(
        injector.getInstance(SecondaryNameNodeExecutor.class));
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
      // Start journal node
      for (TaskID taskId : tasks.keySet()) {
        if (taskId.getValue().contains(".journalnode.")) {
          startProcess(driver, tasks.get(taskId));
        }
      }

      for (TaskID taskId : tasks.keySet()) {
        if (taskId.getValue().contains(".namenode.namenode.")) {
          // TODO add trigger for this event after other events have
          // completed.
          // Boostrap the secondary name node
          runCommand(driver, tasks.get(taskId), "bin/hdfs-mesos-namenode -b");
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
