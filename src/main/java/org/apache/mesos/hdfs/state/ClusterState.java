package org.apache.mesos.hdfs.state;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.hdfs.Scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ClusterState {
  private static final Log log = LogFactory.getLog(ClusterState.class);
  private final State state;
  private final Map<Protos.TaskID, Scheduler.DfsTask> tasks;
  private final Set<Protos.TaskID> journalnodes;
  private final Set<Protos.TaskID> namenodes;
  private final Set<String> dfsHosts;
  private final Set<String> journalnodeHosts;
  private final Set<String> namenodeHosts;

  public ClusterState(State state) {
    this.state = state;
    tasks = new HashMap<>();
    journalnodes = new HashSet<>();
    namenodes = new HashSet<>();
    dfsHosts = new HashSet<>();
    journalnodeHosts = new HashSet<>();
    namenodeHosts = new HashSet<>();
  }

  public State getState() {
    return state;
  }

  public final Map<Protos.TaskID, Scheduler.DfsTask> getTasks() {
    return tasks;
  }

  public final Scheduler.DfsTask getDfsTask(Protos.TaskID taskID) {
    return tasks.get(taskID);
  }

  public final Set<Protos.TaskID> getJournalnodes() {
    return journalnodes;
  }

  public final Set<Protos.TaskID> getNamenodes() {
    return namenodes;
  }

  public final Set<String> getJournalnodeHosts() {
    return journalnodeHosts;
  }

  public final Set<String> getNamenodeHosts() {
    return namenodeHosts;
  }

  public boolean notInDfsHosts(String host) {
    return !dfsHosts.contains(host);
  }

  public void addTask(Protos.TaskID taskId, Scheduler.DfsTask dfsTask) {
    tasks.put(taskId, dfsTask);
    Scheduler.DfsTask.Type type = dfsTask.type;
    switch (type) {
      case NN :
        namenodeHosts.add(dfsTask.hostname);
        break;
      case JN :
        journalnodeHosts.add(dfsTask.hostname);
        break;
      default :
        break;
    }
  }

  public void updateTask(Protos.TaskStatus taskStatus) {
    if (tasks.containsKey(taskStatus.getTaskId())) {
      tasks.get(taskStatus.getTaskId()).taskStatus = taskStatus;
      Scheduler.DfsTask.Type type = tasks.get(taskStatus.getTaskId()).type;
      switch (type) {
        case DN :
        case ZKFC :
          break;
        case NN :
          namenodes.add(taskStatus.getTaskId());
          break;
        case JN :
          journalnodes.add(taskStatus.getTaskId());
          break;
        default :
          Scheduler.log.error("Unknown task type: " + type);
          break;
      }
    } else {
      log.error("Asked to update unknown task, taskId=" + taskStatus.getTaskId().getValue());
    }
  }

  public void removeTask(Protos.TaskStatus taskStatus) {
    Protos.TaskID taskId = taskStatus.getTaskId();
    if (tasks.containsKey(taskId) && tasks.get(taskId).hostname != null) {
      journalnodeHosts.remove(tasks.get(taskId).hostname);
      namenodeHosts.remove(tasks.get(taskId).hostname);
    }
    tasks.remove(taskId);
    namenodes.remove(taskId);
    journalnodes.remove(taskId);
    dfsHosts.remove(taskStatus.getSlaveId().getValue());
  }
}
