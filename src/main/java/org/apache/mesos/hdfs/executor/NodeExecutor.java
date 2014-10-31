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
 * The executor for a Basic Node (either a Journal Node or Data Node).
 * 
 **/
public class NodeExecutor extends AbstractNodeExecutor {
  public static final Log log = LogFactory.getLog(NodeExecutor.class);

  /**
   * The constructor for the node which saves the configuration.
   * 
   **/
  @Inject
  NodeExecutor(SchedulerConf schedulerConf) {
    super(schedulerConf);
  }

  /**
   * Main method for executor, which injects the configuration and state and
   * starts the driver.
   */
  public static void main(String[] args) {
    Injector injector = Guice.createInjector(new ProdConfigModule());

    MesosExecutorDriver driver = new MesosExecutorDriver(injector.getInstance(NodeExecutor.class));
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  /**
   * Add tasks to the task list and then start the tasks.
   */
  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo taskInfo) {
    executorInfo = taskInfo.getExecutor();
    Task task = new Task(taskInfo);
    tasks.put(taskInfo.getTaskId(), task);
    driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(taskInfo.getTaskId())
        .setState(TaskState.TASK_RUNNING).setData(taskInfo.getData()).build());
    startProcess(driver, task);

  }

}
