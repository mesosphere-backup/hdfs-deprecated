package org.apache.mesos.hdfs.state;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.google.inject.Singleton;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.hdfs.util.HDFSConstants;

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
  private NameNodeState nameNode1State = new NameNodeState("NameNode1");
  private NameNodeState nameNode2State = new NameNodeState("NameNode2");

  public boolean isNameNode1Initialized() {
    return nameNode1State.initialized();
  }

  public boolean isNameNode2Initialized() {
    return nameNode2State.initialized();
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
    log.info(String.format("Removing running task: %s", taskId));

    nameNode1State.clear(taskId);
    nameNode2State.clear(taskId);
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
          || status.getMessage().equals(HDFSConstants.JOURNAL_NODE_INIT_MESSAGE)
          || (currentAcquisitionPhase.equals(AcquisitionPhase.RECONCILING_TASKS) && !isNameNode1Initialized())
          || (status.getMessage().equals(HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE) && !isNameNode1Initialized())) {
        setNewNameNodeInitialized(status);
      } else if ((status.getMessage().equals(HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE)
          && !isNameNode2Initialized())
          || (currentAcquisitionPhase.equals(AcquisitionPhase.RECONCILING_TASKS)
          && !isNameNode2Initialized())) {
        // If bootstrapping the second NN or reconciling the second NN,
        // set the status to initialized
        setNewNameNodeInitialized(status);
      } else if (!nameNode1State.hasStatus()) {
        // If the first NN is not running, set the status to running
        nameNode1State.set(status, false);
      } else if (!nameNode2State.hasStatus()) {
        // If the second NN is not running, set the status to running
        nameNode2State.set(status, false);
      }
    }
    runningTasks.put(status.getTaskId().getValue(), status);
  }

  private void setNewNameNodeInitialized(TaskStatus status) {
    TaskID taskId = status.getTaskId();

    if (taskId.equals(nameNode1State.getTaskId()) || nameNode1State.status == null) {
      nameNode1State.set(status, true);
    } else if (taskId.equals(nameNode2State.getTaskId()) || nameNode2State.status == null) {
      nameNode2State.set(status, true);
    } 
  }

  public AcquisitionPhase getCurrentAcquisitionPhase() {
    return currentAcquisitionPhase;
  }

  public void transitionTo(AcquisitionPhase phase) {
    if (currentAcquisitionPhase.equals(phase)) {
      log.info(String.format("Acquisition phase is already '%s'", currentAcquisitionPhase));
    } else {
      log.info(String.format("Transitioning from acquisition phase '%s' to '%s'", currentAcquisitionPhase, phase));
      this.currentAcquisitionPhase = phase;
    }
  }

  public int getJournalNodeSize() {
    return countOfRunningTasksWith(HDFSConstants.JOURNAL_NODE_ID);
  }

  public int getNameNodeSize() {
    return countOfRunningTasksWith(HDFSConstants.NAME_NODE_TASKID);
  }

  public Protos.TaskID getFirstNameNodeTaskId() {
    return nameNode1State.getTaskId();
  }

  public Protos.TaskID getSecondNameNodeTaskId() {
    return nameNode2State.getTaskId();
  }

  public Protos.SlaveID getFirstNameNodeSlaveId() {
    return nameNode1State.getSlaveId();
  }

  public Protos.SlaveID getSecondNameNodeSlaveId() {
    return nameNode2State.getSlaveId();
  }

  private int countOfRunningTasksWith(final String nodeId) {
    return Sets.filter(runningTasks.keySet(), new Predicate<String>() {
      @Override
      public boolean apply(String taskId) {
        return taskId.contains(nodeId);
      }
    }).size();
  }

  private class NameNodeState {
    private String name;
    private TaskStatus status;
    private Boolean initialized;

    public NameNodeState(String name) {
      this.name = name;
    }

    public Boolean hasStatus() {
      return status != null;
    }

    public Boolean initialized() {
      return status != null && initialized;
    }

    public void set(TaskStatus status, Boolean initialized) {
      this.status = status;
      this.initialized = initialized;
      log.info(String.format("Set Node '%s' state to status: '%s', initialized: '%s'", name, status, initialized));
    }

    public void clear(TaskID taskId) {
      if (hasStatus() && status.getTaskId().equals(taskId)) {
        set(null, false);
      }
    }

    public TaskID getTaskId() {
      if (hasStatus()) {
        return status.getTaskId();
      } else {
        return null;
      } 
    }

    public SlaveID getSlaveId() {
      if (hasStatus()) {
        return status.getSlaveId();
      } else {
        return null;
      } 
    }
  }
}
