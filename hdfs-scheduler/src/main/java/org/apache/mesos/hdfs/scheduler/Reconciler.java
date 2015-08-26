package org.apache.mesos.hdfs.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hdfs.state.IPersistentStateStore;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;

public class Reconciler implements Observer {
  private final Log log = LogFactory.getLog(HdfsScheduler.class);
  private IPersistentStateStore store;
  private Set<String> pendingTasks;

  public Reconciler(IPersistentStateStore pStore) {
    store = pStore;
    pendingTasks = new HashSet<String>();
  }

  public void reconcile(SchedulerDriver driver) {
    pendingTasks = store.getAllTaskIds();
    logPendingTasks();
    ExplicitlyReconcileTasks(driver);
    ImplicitlyReconcileTasks(driver);
  }

  public void update(Observable obs, Object obj) {
    TaskStatus status = (TaskStatus)obj;

    String taskId = status.getTaskId().getValue();
    log.info("Received task update for: " + taskId);

    if(!complete()) {
      log.info("Reconciliation is NOT complete");

      if (taskIsPending(taskId)) {
        log.info(String.format("Reconciling Task '%s'.", taskId));
        pendingTasks.remove(taskId);
      } else {
        log.info(String.format("Task '%s' is not pending reconciliation.", taskId));
      }

      logPendingTasks();

      if (complete()) {
        log.info("Reconciliation is complete");
      }
    }
  }

  private boolean taskIsPending(String taskId) {
    for (String t : pendingTasks) {
      if (t.equals(taskId)) {
        return true;
      }
    }

    return false;
  }

  public boolean complete() {
    if (pendingTasks.size() > 0) {
      return false;
    }

    return true;
  }

  private void logPendingTasks() {
    log.info("=========================================");
    log.info("pendingTasks size: " + pendingTasks.size());
    for (String t : pendingTasks) {
        log.info(t);
    }
    log.info("=========================================");
  }

  private void ImplicitlyReconcileTasks(SchedulerDriver driver) {
    log.info("Implicitly Reconciling Tasks");
    driver.reconcileTasks(Collections.<TaskStatus>emptyList());
  }

  private void ExplicitlyReconcileTasks(SchedulerDriver driver) {
    log.info("Explicitly Reconciling Tasks");
    List<TaskStatus> tasks  = new ArrayList<TaskStatus>();

    for (String id : pendingTasks) {
      Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(id).build();
      TaskStatus taskStatus = TaskStatus.newBuilder()
        .setTaskId(taskId)
        .setState(TaskState.TASK_RUNNING).build();

      tasks.add(taskStatus);
    }

    driver.reconcileTasks(tasks);
  }
}
