package org.apache.mesos.hdfs.state;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.MesosNativeLibrary;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.state.Variable;
import org.apache.mesos.state.ZooKeeperState;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Singleton
public class PersistentState {
  public static final Log log = LogFactory.getLog(PersistentState.class);
  private static String FRAMEWORK_ID_KEY = "frameworkId";
  private static String NAMENODES_KEY = "nameNodes";
  private static String JOURNALNODES_KEY = "journalNodes";
  private static String DATANODES_KEY = "dataNodes";
  private ZooKeeperState zkState;

  private HashSet<String> unusedNameNodeNames = new HashSet<>();
  private HashMap<String, String> nameNodeNames = new HashMap<>();
  private HashSet<String> unusedJournalNodeNames = new HashSet<>();
  private HashMap<String, String> journalNodeNames = new HashMap<>();

  @Inject
  public PersistentState(SchedulerConf conf) {
    MesosNativeLibrary.load(conf.getNativeLibrary());
    this.zkState = new ZooKeeperState(conf.getStateZkServers(),
        conf.getStateZkTimeout(), TimeUnit.MILLISECONDS, "/hdfs-mesos/" + conf.getFrameworkName());
    for (int i = conf.getJournalNodeCount(); i > 0; i--) {
      unusedJournalNodeNames.add(HDFSConstants.JOURNAL_NODE_ID + i);
    }
    for (int i = HDFSConstants.TOTAL_NAME_NODES; i > 0; i--) {
      unusedNameNodeNames.add(HDFSConstants.NAME_NODE_ID + i);
    }
  }

  public FrameworkID getFrameworkID() throws InterruptedException, ExecutionException,
      InvalidProtocolBufferException {
    byte[] existingFrameworkId = zkState.fetch(FRAMEWORK_ID_KEY).get().value();
    if (existingFrameworkId.length > 0) {
      return FrameworkID.parseFrom(existingFrameworkId);

    } else {
      return null;
    }
  }

  public void setFrameworkId(FrameworkID frameworkId) throws InterruptedException,
      ExecutionException {
    Variable value = zkState.fetch(FRAMEWORK_ID_KEY).get();
    value = value.mutate(frameworkId.toByteArray());
    zkState.store(value).get();
  }

  public List<String> getDeadJournalNodes() {
    HashMap<String, String> journalNodes = getJournalNodes();
    Set<String> journalHosts = journalNodes.keySet();
    List<String> deadJournalHosts = new ArrayList<>();

    for (String journalHost: journalHosts) {
      if (journalNodes.get(journalHost) == null) {
        deadJournalHosts.add(journalHost);
      }
    }
    return deadJournalHosts;
  }

  public List<String> getDeadNameNodes() {
    HashMap<String, String> nameNodes = getNameNodes();
    Set<String> nameHosts = nameNodes.keySet();
    List<String> deadNameHosts = new ArrayList<>();

    for (String nameHost : nameHosts) {
      if (nameNodes.get(nameHost) == null) {
        deadNameHosts.add(nameHost);
      }
    }
    return deadNameHosts;
  }

  public List<String> getDeadDataNodes() {
    HashMap<String, String> dataNodes = getDataNodes();
    Set<String> dataHosts = dataNodes.keySet();
    List<String> deadDataHosts = new ArrayList<>();

    for (String dataHost : dataHosts) {
      if (dataNodes.get(dataHost) == null) {
        deadDataHosts.add(dataHost);
      }
    }
    return deadDataHosts;
  }

  // TODO (nicgrayson) add tests with in-memory state implementation for zookeeper
  public HashMap<String, String> getJournalNodes() {
    return getHashMap(JOURNALNODES_KEY);
  }

  public HashMap<String, String> getNameNodes() {
    return getHashMap(NAMENODES_KEY);
  }

  public HashMap<String, String> getDataNodes() {
    return getHashMap(DATANODES_KEY);
  }

  public Collection<String> getAllTaskIds() {
    HashMap<String, String> allTasksIds = getJournalNodes();
    allTasksIds.putAll(getNameNodes());
    allTasksIds.putAll(getDataNodes());
    return allTasksIds.values();
  }

  public String chooseNodeName(Protos.TaskID taskId) {
    if (taskId.getValue().contains(HDFSConstants.JOURNAL_NODE_ID)) {
      Iterator<String> iter = unusedJournalNodeNames.iterator();
      String name = iter.next();
      unusedJournalNodeNames.remove(name);
      journalNodeNames.put(taskId.getValue(), name);
      return name;
    } else {
      Iterator<String> iter = unusedNameNodeNames.iterator();
      String name = iter.next();
      unusedNameNodeNames.remove(name);
      nameNodeNames.put(taskId.getValue(), name);
      return name;
    }
  }

  public void addHdfsNode(Protos.TaskID taskId, String hostname, String taskName) {
    switch (taskName) {
      case HDFSConstants.NAME_NODE_ID :
        HashMap<String, String> nameNodes = getNameNodes();
        nameNodes.put(hostname, taskId.getValue());
        System.out.println("Saving the name node " + hostname + " " + taskId.getValue());
        setNameNodes(nameNodes);
        break;
      case HDFSConstants.JOURNAL_NODE_ID :
        HashMap<String, String> journalNodes = getJournalNodes();
        journalNodes.put(hostname, taskId.getValue());
        setJournalNodes(journalNodes);
        break;
      case HDFSConstants.DATA_NODE_ID :
        HashMap<String, String> dataNodes = getDataNodes();
        dataNodes.put(hostname, taskId.getValue());
        setDataNodes(dataNodes);
        break;
      case HDFSConstants.ZKFC_NODE_ID :
        break;
      default :
        log.error("Task name unknown");
    }
  }

  // TODO (elingg) optimize this method/ Possibly index by task id instead of hostname
  public void removeTaskId(String taskId) {
    log.info("removing task with id " + taskId);
    HashMap<String, String> journalNodes = getJournalNodes();
    if (journalNodes.values().contains(taskId)) {
      if (journalNodeNames.containsKey(taskId)) {
        String name = journalNodeNames.get(taskId);
        log.info(unusedJournalNodeNames);
        unusedJournalNodeNames.add(name);
        journalNodeNames.remove(taskId);
      }
      for (Map.Entry<String, String> entry : journalNodes.entrySet()) {
        if (entry.getValue() != null && entry.getValue().equals(taskId)) {
          journalNodes.put(entry.getKey(), null);
          setJournalNodes(journalNodes);
          return;
        }
      }
    }
    HashMap<String, String> nameNodes = getNameNodes();
    if (nameNodes.values().contains(taskId)) {
      // Give the name back to use.
      if (nameNodeNames.containsKey(taskId)) {
        String name = nameNodeNames.get(taskId);
        unusedNameNodeNames.add(name);
        nameNodeNames.remove(taskId);
      }
      for (Map.Entry<String, String> entry : nameNodes.entrySet()) {
        if (entry.getValue() != null && entry.getValue().equals(taskId)) {
          nameNodes.put(entry.getKey(), null);
          setNameNodes(nameNodes);
          return;
        }
      }
    }
    HashMap<String, String> dataNodes = getDataNodes();
    if (dataNodes.values().contains(taskId)) {
      for (Map.Entry<String, String> entry : dataNodes.entrySet()) {
        if (entry.getValue() != null && entry.getValue().equals(taskId)) {
          dataNodes.put(entry.getKey(), null);
          setDataNodes(dataNodes);
          return;
        }
      }
    }
  }

  public boolean journalNodeRunningOnSlave(String hostname) {
    return getJournalNodes().keySet().contains(hostname);
  }

  public boolean nameNodeRunningOnSlave(String hostname) {
    return getNameNodes().keySet().contains(hostname);
  }

  public boolean dataNodeRunningOnSlave(String hostname) {
    return getDataNodes().keySet().contains(hostname);
  }

  private void setNameNodes(HashMap<String, String> nameNodes) {
    try {
      set(NAMENODES_KEY, nameNodes);
    } catch (Exception e) {
      log.error("Error while setting namenodes in persistent state", e);
    }
  }

  private void setJournalNodes(HashMap<String, String> journalNodes) {
    try {
      set(JOURNALNODES_KEY, journalNodes);
    } catch (Exception e) {
      log.error("Error while setting journalnodes in persistent state", e);
    }
  }

  private void setDataNodes(HashMap<String, String> dataNodes) {
    try {
      set(DATANODES_KEY, dataNodes);
    } catch (Exception e) {
      log.error("Error while setting datanodes in persistent state", e);
    }
  }

  private HashMap<String, String> getHashMap(String key) {
    try {
      HashMap<String, String> nodesMap = get(key);
      if (nodesMap == null) {
        return new HashMap<>();
      }
      return nodesMap;
    } catch (Exception e) {
      log.error(String.format("Error while getting %s in persistent state", key), e);
      return new HashMap<>();
    }
  }

  /**
   * Get serializable object from store.
   * 
   * @return serialized object or null if none
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  @SuppressWarnings("unchecked")
  private <T extends Object> T get(String key) throws InterruptedException, ExecutionException,
      IOException, ClassNotFoundException {
    byte[] existingNodes = zkState.fetch(key).get().value();
    if (existingNodes.length > 0) {
      ByteArrayInputStream bis = new ByteArrayInputStream(existingNodes);
      ObjectInputStream in = null;
      try {
        in = new ObjectInputStream(bis);
        return (T) in.readObject();
      } finally {
        try {
          bis.close();
        } finally {
          if (in != null) {
            in.close();
          }
        }
      }
    } else {
      return null;
    }
  }

  /**
   * Set serializable object in store
   * 
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws IOException
   */
  private <T extends Object> void set(String key, T object) throws InterruptedException,
      ExecutionException, IOException {
    Variable value = zkState.fetch(key).get();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream out = null;
    try {
      out = new ObjectOutputStream(bos);
      out.writeObject(object);
      value = value.mutate(bos.toByteArray());
      zkState.store(value).get();
    } finally {
      try {
        if (out != null) {
          out.close();
        }
      } finally {
        bos.close();
      }
    }
  }
}
