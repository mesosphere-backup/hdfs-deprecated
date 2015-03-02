package org.apache.mesos.hdfs.state;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.util.*;

@Singleton
public class LiveState {

  public final SchedulerConf conf;

  private static final Log log = LogFactory.getLog(LiveState.class);
  private final Map<Protos.TaskID, String> taskHostMap;
  private final Map<Protos.TaskID, String> taskSlaveMap;
  private final Map<Protos.TaskID, String> taskNameMap;
  private final Map<Protos.TaskID, String> journalNodes;
  private final Map<Protos.TaskID, String> nameNodes;
  private final Set<String> journalNodeHosts;
  private final Set<String> nameNodeHosts;
  private final HashSet<Protos.TaskID> secondaryNameNodes;
  private final HashSet<String> unusedNameNodeNames;
  private final HashSet<String> unusedJournalNodeNames;
  private final HashSet<Protos.TaskID> stagingTasks;

  @Inject
  public LiveState(SchedulerConf conf) {
    this.conf = conf;
    taskHostMap = new HashMap<>();
    taskSlaveMap = new HashMap<>();
    taskNameMap = new HashMap<>();
    journalNodes = new HashMap<>();
    nameNodes = new HashMap<>();
    journalNodeHosts = new HashSet<>();
    nameNodeHosts = new HashSet<>();
    secondaryNameNodes = new HashSet<>();
    unusedNameNodeNames = new HashSet<>();
    for (int i = HDFSConstants.TOTAL_NAME_NODES; i > 0; i--)
      unusedNameNodeNames.add(HDFSConstants.NAME_NODE_ID + i);
    unusedJournalNodeNames = new HashSet<>();
    for (int i = HDFSConstants.TOTAL_NAME_NODES + conf.getJournalNodeCount(); i > 0; i--)
      unusedJournalNodeNames.add(HDFSConstants.JOURNAL_NODE_ID + i);
    stagingTasks = new HashSet<>();
  }
  public Map<Protos.TaskID, String> getTaskHostMap() {
    return taskHostMap;
  }

  public Map<Protos.TaskID, String> getTaskSlaveMap() {
    return taskSlaveMap;
  }

  public Set<Protos.TaskID> getJournalNodes() {
    return journalNodes.keySet();
  }

  public Set<Protos.TaskID> getNameNodes() {
    return nameNodes.keySet();
  }

  public Set<Protos.TaskID> getSecondaryNameNodes() {
    return secondaryNameNodes;
  }

  public Set<String> getJournalNodeHosts() {
    return journalNodeHosts;
  }

  public Set<String> getNameNodeHosts() {
    return nameNodeHosts;
  }

  public Set<Protos.TaskID> getStagingTasks() {
    return stagingTasks;
  }

  public boolean notInDfsHosts(String slaveId) {
    return !taskSlaveMap.values().contains(slaveId);
  }

  public String getUnusedNameFor(String taskType) {
    Set<String> names;
    switch (taskType) {
      case HDFSConstants.NAME_NODE_ID :
        names = unusedNameNodeNames;
        break;
      case HDFSConstants.JOURNAL_NODE_ID :
        names = unusedJournalNodeNames;
        break;
      default :
        return taskType;
    }
    Iterator<String> iter = names.iterator();
    String name = iter.next();
    names.remove(name);
    return name;
  }

  public void addTask(Protos.TaskID taskId, String taskName, String hostname, String slaveId) {
    taskHostMap.put(taskId, hostname);
    taskSlaveMap.put(taskId, slaveId);
    taskNameMap.put(taskId, taskName);
    if (taskId.getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      System.out.println("Registering name node host:");
      System.out.println(hostname);
      nameNodeHosts.add(hostname);
    } else if (taskId.getValue().contains(HDFSConstants.JOURNAL_NODE_ID)) {
      System.out.println("Registering journal node host:");
      System.out.println(hostname);
      journalNodeHosts.add(hostname);
    }
  }

  public void updateTask(Protos.TaskStatus taskStatus) {
    String name = taskNameMap.get(taskStatus.getTaskId());
    if (name.isEmpty())
      return; // can't update a task that never got named.
    if (taskStatus.getTaskId().getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      nameNodes.put(taskStatus.getTaskId(), name);
      unusedNameNodeNames.remove(name);
    } else if (taskStatus.getTaskId().getValue().contains(HDFSConstants.JOURNAL_NODE_ID)) {
      journalNodes.put(taskStatus.getTaskId(), name);
      unusedJournalNodeNames.remove(name);
    }
  }

  public void removeTask(Protos.TaskStatus taskStatus) {
    Protos.TaskID taskId = taskStatus.getTaskId();
    journalNodeHosts.remove(taskHostMap.get(taskId));
    nameNodeHosts.remove(taskHostMap.get(taskId));
    taskHostMap.remove(taskId);
    taskSlaveMap.remove(taskId);
    taskNameMap.remove(taskId);
    if (nameNodes.get(taskId) != null)
      unusedNameNodeNames.add(nameNodes.get(taskId));
    nameNodes.remove(taskId);
    if (journalNodes.get(taskId) != null)
      unusedJournalNodeNames.add(journalNodes.get(taskId));
    journalNodes.remove(taskId);
  }

  public void addStagingTask(Protos.TaskID taskId) {
    stagingTasks.add(taskId);
  }

  public void removeStagingTask(Protos.TaskID taskId) {
    stagingTasks.remove(taskId);
  }

  public void addSecondaryNameNode(Protos.TaskID taskId) {
    secondaryNameNodes.add(taskId);
  }
}
