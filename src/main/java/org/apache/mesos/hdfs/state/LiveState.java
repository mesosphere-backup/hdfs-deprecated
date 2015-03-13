package org.apache.mesos.hdfs.state;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.google.inject.Singleton;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.util.HDFSConstants;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

@Singleton
public class LiveState {
  public static final Log log = LogFactory.getLog(LiveState.class);
  private Set<Protos.TaskID> stagingTasks = new HashSet<>();
  private AcquisitionPhase currentAcquisitionPhase = AcquisitionPhase.RECONCILING_TASKS;
  // TODO (nicgrayson) Might need to split this out to jns, nns, and dns if dns too big
  private LinkedHashMap<String, Protos.TaskStatus> runningTasks = new LinkedHashMap<>();
  private HashMap<String, String> journalNodeNames = new HashMap<>();
  private HashMap<String, String> nameNodeNames = new HashMap<>();
  private HashMap<Protos.TaskStatus, Boolean> nameNode1TaskMap = new HashMap<>();
  private HashMap<Protos.TaskStatus, Boolean> nameNode2TaskMap = new HashMap<>();

  public boolean isNameNode1Initialized() {
    return !nameNode1TaskMap.isEmpty() && nameNode1TaskMap.values().iterator().next();
  }

  public boolean isNameNode2Initialized() {
    return !nameNode2TaskMap.isEmpty() && nameNode2TaskMap.values().iterator().next();
  }

  public HashMap<String, String> getJournalNodeNames() { return journalNodeNames; }

  public HashMap<String, String> getNameNodeNames() { return nameNodeNames; }

  public void addStagingTask(Protos.TaskID taskId, String taskName) {
    stagingTasks.add(taskId);
    if (taskId.getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      nameNodeNames.put(taskId.getValue(), taskName);
    }
    if (taskId.getValue().contains(HDFSConstants.JOURNAL_NODE_ID)) {
      journalNodeNames.put(taskId.getValue(), taskName);
    }
  }

  public int getStagingTasksSize() {
    return stagingTasks.size();
  }

  public void removeStagingTask(final Protos.TaskID taskID) {
    stagingTasks.remove(taskID);
  }

  public HashMap<String, Protos.TaskStatus> getRunningTasks() {
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
    journalNodeNames.remove(taskId.getValue());
    nameNodeNames.remove(taskId.getValue());
  }

  public void updateTaskForStatus(Protos.TaskStatus status) {
    // TODO (elingg) Use Starting Status when the task is running, but not initialized. Use running
    // status when the task is initialized so that we can differentiate during the reconciliation
    // phase. Also, add the health checks which will kill the task if it doesn't properly
    // initialize or if it reaches an error state.
    // Case of name node, update the task map
    if (status.getTaskId().getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      // If initializing the first NN or reconciling the first NN or bootstrapping the first NN
      // set the status to initialized
      if (status.getMessage().equals(HDFSConstants.NAME_NODE_INIT_MESSAGE)
          || (currentAcquisitionPhase.equals(AcquisitionPhase.RECONCILING_TASKS)
              && !isNameNode1Initialized())
          || (status.getMessage().equals(HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE)
              && !isNameNode1Initialized())) {
        nameNode1TaskMap.clear();
        nameNode1TaskMap.put(status, true);
      } // If bootstrapping the second NN or reconciling the second NN,
        // set the status to initialized
      else if ((status.getMessage().equals(HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE)
          && !isNameNode2Initialized())
          || (currentAcquisitionPhase.equals(AcquisitionPhase.RECONCILING_TASKS)
              && !isNameNode2Initialized())) {
        nameNode2TaskMap.clear();
        nameNode2TaskMap.put(status, true);
      } // If the first NN is not running, set the status to running
       else if (nameNode1TaskMap.isEmpty()) {
        nameNode1TaskMap.put(status, false);
      } // If the second NN is not running, set the status to running
       else if (nameNode2TaskMap.isEmpty()) {
        nameNode2TaskMap.put(status, false);
      }
    }
    runningTasks.put(status.getTaskId().getValue(), status);
  }

  public AcquisitionPhase getCurrentAcquisitionPhase() {
    return currentAcquisitionPhase;
  }

  public void transitionTo(AcquisitionPhase phase) {
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
