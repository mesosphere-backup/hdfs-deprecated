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
  private LinkedHashMap<Protos.TaskID, Protos.TaskStatus> runningTasks = new LinkedHashMap<>();
  private HashMap<Protos.TaskStatus, Boolean> nameNode1TaskMap = new HashMap<>();
  private HashMap<Protos.TaskStatus, Boolean> nameNode2TaskMap = new HashMap<>();

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

  public LinkedHashMap<Protos.TaskID, Protos.TaskStatus> getRunningTasks() {
    return runningTasks;
  }

  public void removeRunningTask(Protos.TaskID taskId) {
    if (!nameNode1TaskMap.isEmpty()
        && nameNode1TaskMap.keySet().iterator().next().getTaskId().equals(taskId)) {
      nameNode1TaskMap.clear();
    }
    if (!nameNode2TaskMap.isEmpty()
       && nameNode2TaskMap.keySet().iterator().next().getTaskId().equals(taskId)) {
      nameNode2TaskMap.clear();
    }
    runningTasks.remove(taskId);
  }

  public void updateTaskForStatus(Protos.TaskStatus status) {
    //Case of name node, update the task map
    if (status.getTaskId().getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      if (status.getMessage().equals(HDFSConstants.NAME_NODE_INIT_MESSAGE)
          || (currentAcquisitionPhase.equals(AcquisitionPhase.RECONCILING_TASKS)
          && !isNameNode1Initialized())) {
        nameNode1TaskMap.clear();
        nameNode1TaskMap.put(status, true);
      } else if (status.getMessage().equals(HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE)
          || currentAcquisitionPhase.equals(AcquisitionPhase.RECONCILING_TASKS)) {
        nameNode2TaskMap.clear();
        nameNode2TaskMap.put(status, true);
      } else if (nameNode1TaskMap.isEmpty()) {
        nameNode1TaskMap.put(status, false);
      } else if (nameNode2TaskMap.isEmpty()) {
        nameNode2TaskMap.put(status, false);
      }
    }
    runningTasks.put(status.getTaskId(), status);
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
    return Sets.filter(runningTasks.keySet(), new Predicate<Protos.TaskID>() {
      @Override
      public boolean apply(Protos.TaskID taskID) {
        return taskID.getValue().contains(nodeId);
      }
    }).size();
  }
}
