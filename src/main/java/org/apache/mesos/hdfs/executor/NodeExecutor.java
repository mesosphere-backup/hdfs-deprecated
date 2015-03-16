package org.apache.mesos.hdfs.executor;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;

/**
 * The executor for a Basic Node (either a Journal Node or Data Node).
 */
public class NodeExecutor extends AbstractNodeExecutor {
  public final Log log = LogFactory.getLog(NodeExecutor.class);
  private Task task;

  /**
   * The constructor for the node which saves the configuration.
   */
  @Inject
  NodeExecutor(HdfsFrameworkConfig hdfsConfig) {
    super(hdfsConfig);
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
    log.info(String.format("Launching task, taskId=%s cmd='%s'", taskInfo.getTaskId().getValue(), task.getCmd()));
    startProcess(driver, task);
    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(taskInfo.getTaskId())
        .setState(TaskState.TASK_RUNNING)
        .setData(taskInfo.getData()).build());
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {
    log.info("Killing task : " + taskId.getValue());
    if (task.getProcess() != null && taskId.equals(task.getTaskInfo().getTaskId())) {
      task.getProcess().destroy();
      task.setProcess(null);
    }
  }
}
