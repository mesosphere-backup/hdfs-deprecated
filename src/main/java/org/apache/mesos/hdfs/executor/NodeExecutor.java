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
import java.util.Timer;

/**
 * The executor for a Basic Node (either a Journal Node or Data Node).
 * 
 **/
public class NodeExecutor extends AbstractNodeExecutor {
  public static final Log log = LogFactory.getLog(NodeExecutor.class);

  // Node task run by the executor
  private Task task;

  // Timed Health Check for node health monitoring
  private TimedHealthCheck timedHealthCheck;
  private Timer timer;

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
    startProcess(driver, task);
    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(taskInfo.getTaskId())
        .setState(TaskState.TASK_RUNNING)
        .setData(taskInfo.getData()).build());
    timedHealthCheck = new TimedHealthCheck(driver, task);
    timer = new Timer(true);
    timer.scheduleAtFixedRate(timedHealthCheck, schedulerConf.getHealthCheckWaitingPeriod(),
        schedulerConf.getHealthCheckFrequency());
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {
    log.info("Killing task : " + taskId.getValue());
    if (task.process != null && taskId.equals(task.taskInfo.getTaskId())) {
      task.process.destroy();
      task.process = null;
    }
  }
}
