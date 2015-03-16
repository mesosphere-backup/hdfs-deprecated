package org.apache.mesos.hdfs.state;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.MesosNativeLibrary;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.state.Variable;
import org.apache.mesos.state.ZooKeeperState;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Provides the state of the framework as stored in zk. This is the expected state of the services.
 */
public class PersistentState {
  public final Log log = LogFactory.getLog(PersistentState.class);
  private static final String FRAMEWORK_ID_KEY = "frameworkId";
  private static final String NAMENODES_KEY = "nameNodes";
  private static final String JOURNALNODES_KEY = "journalNodes";
  private static final String DATANODES_KEY = "dataNodes";
  private ZooKeeperState zkState;
  private HdfsFrameworkConfig frameworkConfig;

  private Timestamp deadJournalNodeTimeStamp = null;
  private Timestamp deadNameNodeTimeStamp = null;
  private Timestamp deadDataNodeTimeStamp = null;

  public PersistentState(HdfsFrameworkConfig frameworkConfig) {
    MesosNativeLibrary.load(frameworkConfig.getNativeLibrary());
    this.zkState = new ZooKeeperState(frameworkConfig.getStateZkServers(),
        frameworkConfig.getStateZkTimeout(), TimeUnit.MILLISECONDS, "/hdfs-mesos/"
        + frameworkConfig.getFrameworkName());
    this.frameworkConfig = frameworkConfig;
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

  private void removeDeadJournalNodes() {
    deadJournalNodeTimeStamp = null;
    Map<String, String> journalNodes = getJournalNodes();
    List<String> deadJournalHosts = getDeadJournalNodes();
    for (String deadJournalHost : deadJournalHosts) {
      journalNodes.remove(deadJournalHost);
      log.info("Removing JN Host: " + deadJournalHost);
    }
    setJournalNodes(journalNodes);
  }

  private void removeDeadNameNodes() {
    deadNameNodeTimeStamp = null;
    Map<String, String> nameNodes = getNameNodes();
    List<String> deadNameHosts = getDeadNameNodes();
    for (String deadNameHost : deadNameHosts) {
      nameNodes.remove(deadNameHost);
      log.info("Removing NN Host: " + deadNameHost);
    }
    setNameNodes(nameNodes);
  }

  private void removeDeadDataNodes() {
    deadDataNodeTimeStamp = null;
    Map<String, String> dataNodes = getDataNodes();
    List<String> deadDataHosts = getDeadDataNodes();
    for (String deadDataHost : deadDataHosts) {
      dataNodes.remove(deadDataHost);
      log.info("Removing DN Host: " + deadDataHost);
    }
    setDataNodes(dataNodes);
  }

  public List<String> getDeadJournalNodes() {
    if (deadJournalNodeTimeStamp != null && deadJournalNodeTimeStamp.before(new Date())) {
      removeDeadJournalNodes();
      return new ArrayList<>();
    } else {
      Map<String, String> journalNodes = getJournalNodes();
      Set<String> journalHosts = journalNodes.keySet();
      List<String> deadJournalHosts = new ArrayList<>();
      for (String journalHost : journalHosts) {
        if (journalNodes.get(journalHost) == null) {
          deadJournalHosts.add(journalHost);
        }
      }
      return deadJournalHosts;
    }
  }

  public List<String> getDeadNameNodes() {
    if (deadNameNodeTimeStamp != null && deadNameNodeTimeStamp.before(new Date())) {
      removeDeadNameNodes();
      return new ArrayList<>();
    } else {
      Map<String, String> nameNodes = getNameNodes();
      Set<String> nameHosts = nameNodes.keySet();
      List<String> deadNameHosts = new ArrayList<>();
      for (String nameHost : nameHosts) {
        if (nameNodes.get(nameHost) == null) {
          deadNameHosts.add(nameHost);
        }
      }
      return deadNameHosts;
    }
  }

  public List<String> getDeadDataNodes() {
    if (deadDataNodeTimeStamp != null && deadDataNodeTimeStamp.before(new Date())) {
      removeDeadDataNodes();
      return new ArrayList<>();
    } else {
      Map<String, String> dataNodes = getDataNodes();
      Set<String> dataHosts = dataNodes.keySet();
      List<String> deadDataHosts = new ArrayList<>();
      for (String dataHost : dataHosts) {
        if (dataNodes.get(dataHost) == null) {
          deadDataHosts.add(dataHost);
        }
      }
      return deadDataHosts;
    }
  }

  // TODO (nicgrayson) add tests with in-memory state implementation for zookeeper
  public Map<String, String> getJournalNodes() {
    return getMap(JOURNALNODES_KEY);
  }

  public Map<String, String> getNameNodes() {
    return getMap(NAMENODES_KEY);
  }

  public Map<String, String> getDataNodes() {
    return getMap(DATANODES_KEY);
  }

  public Collection<String> getAllTaskIds() {
    Map<String, String> allTasksIds = getJournalNodes();
    allTasksIds.putAll(getNameNodes());
    allTasksIds.putAll(getDataNodes());
    return allTasksIds.values();
  }

  public void addHdfsNode(Protos.TaskID taskId, String hostname, String taskName) {
    switch (taskName) {
      case HDFSConstants.NAME_NODE_ID:
        Map<String, String> nameNodes = getNameNodes();
        nameNodes.put(hostname, taskId.getValue());
        log.debug("Saving the name node " + hostname + " " + taskId.getValue());
        setNameNodes(nameNodes);
        break;
      case HDFSConstants.JOURNAL_NODE_ID:
        Map<String, String> journalNodes = getJournalNodes();
        journalNodes.put(hostname, taskId.getValue());
        setJournalNodes(journalNodes);
        log.debug("Adding the journal node " + hostname + " " + taskId.getValue());
        break;
      case HDFSConstants.DATA_NODE_ID:
        Map<String, String> dataNodes = getDataNodes();
        dataNodes.put(hostname, taskId.getValue());
        setDataNodes(dataNodes);
        log.debug("Adding the data node " + hostname + " " + taskId.getValue());
        break;
      case HDFSConstants.ZKFC_NODE_ID:
        log.debug("ZKFC node " + hostname + " " + taskId.getValue());
        break;
      default:
        log.error("Task name unknown for adding hdfs node");
    }
  }

  // TODO (elingg) optimize this method/ Possibly index by task id instead of hostname/
  // Possibly call removeTask(slaveId, taskId) to avoid iterating through all maps
  public void removeTaskId(String taskId) {
    Map<String, String> journalNodes = getJournalNodes();
    if (journalNodes.values().contains(taskId)) {
      for (Map.Entry<String, String> entry : journalNodes.entrySet()) {
        if (entry.getValue() != null && entry.getValue().equals(taskId)) {
          journalNodes.put(entry.getKey(), null);
          setJournalNodes(journalNodes);
          Date date = DateUtils.addSeconds(new Date(), frameworkConfig.getDeadNodeTimeout());
          deadJournalNodeTimeStamp = new Timestamp(date.getTime());
          return;
        }
      }
    }
    Map<String, String> nameNodes = getNameNodes();
    if (nameNodes.values().contains(taskId)) {
      for (Map.Entry<String, String> entry : nameNodes.entrySet()) {
        if (entry.getValue() != null && entry.getValue().equals(taskId)) {
          nameNodes.put(entry.getKey(), null);
          setNameNodes(nameNodes);
          Date date = DateUtils.addSeconds(new Date(), frameworkConfig.getDeadNodeTimeout());
          deadNameNodeTimeStamp = new Timestamp(date.getTime());
          return;
        }
      }
    }
    Map<String, String> dataNodes = getDataNodes();
    if (dataNodes.values().contains(taskId)) {
      for (Map.Entry<String, String> entry : dataNodes.entrySet()) {
        if (entry.getValue() != null && entry.getValue().equals(taskId)) {
          dataNodes.put(entry.getKey(), null);
          setDataNodes(dataNodes);
          Date date = DateUtils.addSeconds(new Date(), frameworkConfig.getDeadNodeTimeout());
          deadDataNodeTimeStamp = new Timestamp(date.getTime());
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

  private void setNameNodes(Map<String, String> nameNodes) {
    try {
      set(NAMENODES_KEY, nameNodes);
    } catch (InterruptedException | ExecutionException | IOException e) {
      log.error("Error while setting namenodes in persistent state", e);
    }
  }

  private void setJournalNodes(Map<String, String> journalNodes) {
    try {
      set(JOURNALNODES_KEY, journalNodes);
    } catch (InterruptedException | ExecutionException | IOException e) {
      log.error("Error while setting journalnodes in persistent state", e);
    }
  }

  private void setDataNodes(Map<String, String> dataNodes) {
    try {
      set(DATANODES_KEY, dataNodes);
    } catch (InterruptedException | ExecutionException | IOException e) {
      log.error("Error while setting datanodes in persistent state", e);
    }
  }

  private Map<String, String> getMap(String key) {
    try {
      Map<String, String> nodesMap = get(key);
      if (nodesMap == null) {
        return new HashMap<>();
      }
      return nodesMap;
      // todo:  (kensipe) remove ClassNotFoundException exception and probably not use generics in return of get(key)
    } catch (InterruptedException | ExecutionException | IOException | ClassNotFoundException e) {
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
   * Set serializable object in store.
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
