package org.apache.mesos.hdfs.state;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.google.inject.Singleton;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Manages the "Live" state of running tasks.
 */
@Singleton
public class LiveState {
  private final Log log = LogFactory.getLog(LiveState.class);

  private Set<Protos.TaskID> stagingTasks = new HashSet<>();
  private AcquisitionPhase currentAcquisitionPhase = AcquisitionPhase.RECONCILING_TASKS;
  // TODO (nicgrayson) Might need to split this out to jns, nns, and dns if dns too big
  //TODO (elingg) we need to also track ZKFC's state
  private Map<String, Protos.TaskStatus> runningTasks = new LinkedHashMap<>();
  private Map<Protos.TaskStatus, Boolean> nameNode1TaskMap = new HashMap<>();
  private Map<Protos.TaskStatus, Boolean> nameNode2TaskMap = new HashMap<>();

  public boolean isNameNode1Initialized() {
    return !nameNode1TaskMap.isEmpty() && nameNode1TaskMap.values().iterator().next();
  }

  public boolean isNameNode2Initialized() {
    return !nameNode2TaskMap.isEmpty() && nameNode2TaskMap.values().iterator().next();
  }

  public void addStagingTask(Protos.TaskID taskId) {
    stagingTasks.add(taskId);
  }

  public int getStagingTasksSize() {
    return stagingTasks.size();
  }

  public void removeStagingTask(final Protos.TaskID taskID) {
    stagingTasks.remove(taskID);
  }

  public Map<String, Protos.TaskStatus> getRunningTasks() {
    return runningTasks;
  }

  public void removeRunningTask(Protos.TaskID taskId) {
    if (!nameNode1TaskMap.isEmpty()
        && nameNode1TaskMap.keySet().iterator().next().getTaskId().equals(taskId)) {
      nameNode1TaskMap.clear();
    } else if (!nameNode2TaskMap.isEmpty()
        && nameNode2TaskMap.keySet().iterator().next().getTaskId().equals(taskId)) {
      nameNode2TaskMap.clear();
    }
    runningTasks.remove(taskId.getValue());
  }

  @SuppressWarnings("PMD")
  public void updateTaskForStatus(Protos.TaskStatus status) {
    // todo:  (kgs) refactor to remove the pmd challenges with this code
    // TODO (elingg) Use Starting Status when the task is running, but not initialized. Use running
    // status when the task is initialized so that we can differentiate during the reconciliation
    // phase. Also, add the health checks which will kill the task if it doesn't properly
    // initialize or if it reaches an error state.
    // Case of name node, update the task map
    if (status.getTaskId().getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      // If initializing the first NN or reconciling the first NN or bootstrapping the first NN
      // set the status to initialized
      if (status.getMessage().equals(HDFSConstants.NAME_NODE_INIT_MESSAGE)
          || (currentAcquisitionPhase.equals(AcquisitionPhase.RECONCILING_TASKS) && !isNameNode1Initialized())
          || (status.getMessage().equals(HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE) && !isNameNode1Initialized())) {
        nameNode1TaskMap.clear();
        nameNode1TaskMap.put(status, true);
      } else if ((status.getMessage().equals(HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE)
          && !isNameNode2Initialized())
          || (currentAcquisitionPhase.equals(AcquisitionPhase.RECONCILING_TASKS)
          && !isNameNode2Initialized())) {
        // If bootstrapping the second NN or reconciling the second NN,
        // set the status to initialized
        nameNode2TaskMap.clear();
        nameNode2TaskMap.put(status, true);
      } else if (nameNode1TaskMap.isEmpty()) {
        // If the first NN is not running, set the status to running
        nameNode1TaskMap.put(status, false);
      } else if (nameNode2TaskMap.isEmpty()) {
        // If the second NN is not running, set the status to running
        nameNode2TaskMap.put(status, false);
      }
    }
    runningTasks.put(status.getTaskId().getValue(), status);
  }

  public AcquisitionPhase getCurrentAcquisitionPhase() {
    return currentAcquisitionPhase;
  }

  public void transitionTo(AcquisitionPhase phase) {
    if (currentAcquisitionPhase.equals(phase)) {
      log.info(String.format("Acquisition phase is already '%s'", currentAcquisitionPhase));
      return;
    }

    log.info(String.format("Transitioning from acquisition phase '%s' to '%s'", currentAcquisitionPhase, phase));
    this.currentAcquisitionPhase = phase;
  }

  public int getJournalNodeSize() {
    return countOfRunningTasksWith(HDFSConstants.JOURNAL_NODE_ID);
  }

  public int getNameNodeSize() {
    return countOfRunningTasksWith(HDFSConstants.NAME_NODE_TASKID);
  }

  public Protos.TaskID getFirstNameNodeTaskId() {
    if (nameNode1TaskMap.isEmpty()) {
      return null;
    }
    return nameNode1TaskMap.keySet().iterator().next().getTaskId();
  }

  public Protos.TaskID getSecondNameNodeTaskId() {
    if (nameNode2TaskMap.isEmpty()) {
      return null;
    }
    return nameNode2TaskMap.keySet().iterator().next().getTaskId();
  }

  public Protos.SlaveID getFirstNameNodeSlaveId() {
    if (nameNode1TaskMap.isEmpty()) {
      return null;
    }
    return nameNode1TaskMap.keySet().iterator().next().getSlaveId();
  }

  public Protos.SlaveID getSecondNameNodeSlaveId() {
    if (nameNode2TaskMap.isEmpty()) {
      return null;
    }
    return nameNode2TaskMap.keySet().iterator().next().getSlaveId();
  }

  private int countOfRunningTasksWith(final String nodeId) {
    return Sets.filter(runningTasks.keySet(), new Predicate<String>() {
      @Override
      public boolean apply(String taskId) {
        return taskId.contains(nodeId);
      }
    }).size();
  }
}
