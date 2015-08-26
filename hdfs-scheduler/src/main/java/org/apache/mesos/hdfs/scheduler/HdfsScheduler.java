package org.apache.mesos.hdfs.scheduler;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.Credential;
import org.apache.mesos.Protos.Environment;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.AcquisitionPhase;
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.state.PersistenceException;
import org.apache.mesos.hdfs.state.IPersistentStateStore;
import org.apache.mesos.hdfs.util.DnsResolver;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

/**
 * HDFS Mesos Framework Scheduler class implementation.
 */
public class HdfsScheduler implements org.apache.mesos.Scheduler, Runnable {
  // TODO (elingg) remove as much logic as possible from Scheduler to clean up code
  private final Log log = LogFactory.getLog(HdfsScheduler.class);

  private static final int SECONDS_FROM_MILLIS = 1000;

  private final HdfsFrameworkConfig hdfsFrameworkConfig;
  private final LiveState liveState;
  private final IPersistentStateStore persistenceStore;
  private final DnsResolver dnsResolver;

  @Inject
  public HdfsScheduler(HdfsFrameworkConfig hdfsFrameworkConfig,
    LiveState liveState, IPersistentStateStore persistenceStore) {

    this.hdfsFrameworkConfig = hdfsFrameworkConfig;
    this.liveState = liveState;
    this.persistenceStore = persistenceStore;
    this.dnsResolver = new DnsResolver(this, hdfsFrameworkConfig);
  }

  @Override
  public void disconnected(SchedulerDriver driver) {
    log.info("Scheduler driver disconnected");
  }

  @Override
  public void error(SchedulerDriver driver, String message) {
    log.error("Scheduler driver error: " + message);
  }

  @Override
  public void executorLost(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID,
    int status) {
    log.info("Executor lost: executorId=" + executorID.getValue() + " slaveId="
      + slaveID.getValue() + " status=" + status);
  }

  @Override
  public void frameworkMessage(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID,
    byte[] data) {
    log.info("Framework message: executorId=" + executorID.getValue() + " slaveId="
      + slaveID.getValue() + " data='" + Arrays.toString(data) + "'");
  }

  @Override
  public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
    log.info("Offer rescinded: offerId=" + offerId.getValue());
  }

  @Override
  public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {
    try {
      persistenceStore.setFrameworkId(frameworkId);
    } catch (PersistenceException e) {
      // these are zk exceptions... we are unable to maintain state.
      final String msg = "Error setting framework id in persistent state";
      log.error(msg, e);
      throw new SchedulerException(msg, e);
    }
    log.info("Registered framework frameworkId=" + frameworkId.getValue());
    // reconcile tasks upon registration
    reconcileTasks(driver);
  }

  @Override
  public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
    log.info("Reregistered framework: starting task reconciliation");
    // reconcile tasks upon reregistration
    reconcileTasks(driver);
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    log.info(String.format(
      "Received status update for taskId=%s state=%s message='%s' stagingTasks.size=%d",
      status.getTaskId().getValue(),
      status.getState().toString(),
      status.getMessage(),
      liveState.getStagingTasksSize()));

    if (!isStagingState(status)) {
      liveState.removeStagingTask(status.getTaskId());
    }

    if (isTerminalState(status)) {
      liveState.removeRunningTask(status.getTaskId());
      persistenceStore.removeTaskId(status.getTaskId().getValue());
      // Correct the phase when a task dies after the reconcile period is over
      if (!liveState.getCurrentAcquisitionPhase().equals(AcquisitionPhase.RECONCILING_TASKS)) {
        correctCurrentPhase();
      }
    } else if (isRunningState(status)) {
      liveState.updateTaskForStatus(status);

      log.info(String.format("Current Acquisition Phase: %s", liveState
        .getCurrentAcquisitionPhase().toString()));

      switch (liveState.getCurrentAcquisitionPhase()) {
        case RECONCILING_TASKS:
          break;
        case JOURNAL_NODES:
          if (liveState.getJournalNodeSize() == hdfsFrameworkConfig.getJournalNodeCount()) {
            // TODO (elingg) move the reload to correctCurrentPhase and make it idempotent
            reloadConfigsOnAllRunningTasks(driver);
            correctCurrentPhase();
          }
          break;
        case START_NAME_NODES:
          if (liveState.getNameNodeSize() == HDFSConstants.TOTAL_NAME_NODES) {
            // TODO (elingg) move the reload to correctCurrentPhase and make it idempotent
            reloadConfigsOnAllRunningTasks(driver);
            correctCurrentPhase();
          }
          break;
        case FORMAT_NAME_NODES:
          if (!liveState.isNameNode1Initialized()
            && !liveState.isNameNode2Initialized()) {
            dnsResolver.sendMessageAfterNNResolvable(
              driver,
              liveState.getFirstNameNodeTaskId(),
              liveState.getFirstNameNodeSlaveId(),
              HDFSConstants.NAME_NODE_INIT_MESSAGE);
          } else if (!liveState.isNameNode1Initialized()) {
            dnsResolver.sendMessageAfterNNResolvable(
              driver,
              liveState.getFirstNameNodeTaskId(),
              liveState.getFirstNameNodeSlaveId(),
              HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE);
          } else if (!liveState.isNameNode2Initialized()) {
            dnsResolver.sendMessageAfterNNResolvable(
              driver,
              liveState.getSecondNameNodeTaskId(),
              liveState.getSecondNameNodeSlaveId(),
              HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE);
          } else {
            correctCurrentPhase();
          }
          break;
        // TODO (elingg) add a configurable number of data nodes
        case DATA_NODES:
          break;
      }
    } else {
      log.warn(String.format("Don't know how to handle state=%s for taskId=%s",
        status.getState(), status.getTaskId().getValue()));
    }
  }

  @Override
  public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
    log.info(String.format("Received %d offers", offers.size()));

    // TODO (elingg) within each phase, accept offers based on the number of nodes you need
    boolean acceptedOffer = false;
    boolean journalNodesResolvable = false;
    if (liveState.getCurrentAcquisitionPhase() == AcquisitionPhase.START_NAME_NODES) {
      journalNodesResolvable = dnsResolver.journalNodesResolvable();
    }
    for (Offer offer : offers) {
      if (acceptedOffer) {
        driver.declineOffer(offer.getId());
      } else {
        switch (liveState.getCurrentAcquisitionPhase()) {
          case RECONCILING_TASKS:
            log.info("Declining offers while reconciling tasks");
            driver.declineOffer(offer.getId());
            break;
          case JOURNAL_NODES:
            if (tryToLaunchJournalNode(driver, offer)) {
              acceptedOffer = true;
            } else {
              driver.declineOffer(offer.getId());
            }
            break;
          case START_NAME_NODES:
            if (journalNodesResolvable && tryToLaunchNameNode(driver, offer)) {
              acceptedOffer = true;
            } else {
              driver.declineOffer(offer.getId());
            }
            break;
          case FORMAT_NAME_NODES:
            driver.declineOffer(offer.getId());
            break;
          case DATA_NODES:
            if (tryToLaunchDataNode(driver, offer)) {
              acceptedOffer = true;
            } else {
              driver.declineOffer(offer.getId());
            }
            break;
        }
      }
    }
  }

  @Override
  public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {
    log.info("Slave lost slaveId=" + slaveId.getValue());
  }

  @Override
  public void run() {
    FrameworkInfo.Builder frameworkInfo = FrameworkInfo.newBuilder()
      .setName(hdfsFrameworkConfig.getFrameworkName())
      .setFailoverTimeout(hdfsFrameworkConfig.getFailoverTimeout())
      .setUser(hdfsFrameworkConfig.getHdfsUser())
      .setRole(hdfsFrameworkConfig.getHdfsRole())
      .setCheckpoint(true);

    try {
      FrameworkID frameworkID = persistenceStore.getFrameworkId();
      if (frameworkID != null) {
        frameworkInfo.setId(frameworkID);
      }
    } catch (PersistenceException e) {
      final String msg = "Error recovering framework id";
      log.error(msg, e);
      throw new SchedulerException(msg, e);
    }

    registerFramework(this, frameworkInfo.build(), hdfsFrameworkConfig.getMesosMasterUri());
  }

  private void registerFramework(HdfsScheduler sched, FrameworkInfo fInfo, String masterUri) {
    Credential cred = getCredential();

    if (cred != null) {
      log.info("Registering with credentials.");
      new MesosSchedulerDriver(sched, fInfo, masterUri, cred).run();
    } else {
      log.info("Registering without authentication");
      new MesosSchedulerDriver(sched, fInfo, masterUri).run();
    }
  }

  private Credential getCredential() {
    if (hdfsFrameworkConfig.cramCredentialsEnabled()) {
      try {
        Credential.Builder credentialBuilder = Credential.newBuilder()
          .setPrincipal(hdfsFrameworkConfig.getPrincipal())
          .setSecret(ByteString.copyFrom(hdfsFrameworkConfig.getSecret().getBytes("UTF-8")));

        return credentialBuilder.build();

      } catch (UnsupportedEncodingException ex) {
        log.error("Failed to encode secret when creating Credential.");
      }
    }

    return null;
  }

  private boolean launchNode(SchedulerDriver driver, Offer offer,
    String nodeName, List<String> taskTypes, String executorName) {
    // nodeName is the type of executor to launch
    // executorName is to distinguish different types of nodes
    // taskType is the type of task in mesos to launch on the node
    // taskName is a name chosen to identify the task in mesos and mesos-dns (if used)
    log.info(String.format("Launching node of type %s with tasks %s", nodeName,
      taskTypes.toString()));
    String taskIdName = String.format("%s.%s.%d", nodeName, executorName,
      System.currentTimeMillis());
    List<Resource> resources = getExecutorResources();
    ExecutorInfo executorInfo = createExecutor(taskIdName, nodeName, executorName, resources);
    List<TaskInfo> tasks = new ArrayList<>();
    for (String taskType : taskTypes) {
      List<Resource> taskResources = getTaskResources(taskType);
      String taskName = getNextTaskName(taskType);
      TaskID taskId = TaskID.newBuilder()
        .setValue(String.format("task.%s.%s", taskType, taskIdName))
        .build();
      TaskInfo task = TaskInfo.newBuilder()
        .setExecutor(executorInfo)
        .setName(taskName)
        .setTaskId(taskId)
        .setSlaveId(offer.getSlaveId())
        .addAllResources(taskResources)
        .setData(ByteString.copyFromUtf8(
          String.format("bin/hdfs-mesos-%s", taskType)))
        .build();
      tasks.add(task);

      liveState.addStagingTask(task.getTaskId());
      persistenceStore.addHdfsNode(taskId, offer.getHostname(), taskType, taskName);
    }
    driver.launchTasks(Arrays.asList(offer.getId()), tasks);
    return true;
  }

  private String getNextTaskName(String taskType) {

    if (taskType.equals(HDFSConstants.NAME_NODE_ID)) {
      Collection<String> nameNodeTaskNames = persistenceStore.getNameNodeTaskNames().values();
      for (int i = 1; i <= HDFSConstants.TOTAL_NAME_NODES; i++) {
        if (!nameNodeTaskNames.contains(HDFSConstants.NAME_NODE_ID + i)) {
          return HDFSConstants.NAME_NODE_ID + i;
        }
      }
      String errorStr = "Cluster is in inconsistent state. " +
        "Trying to launch more namenodes, but they are all already running.";
      log.error(errorStr);
      throw new SchedulerException(errorStr);
    }
    if (taskType.equals(HDFSConstants.JOURNAL_NODE_ID)) {
      Collection<String> journalNodeTaskNames = persistenceStore.getJournalNodeTaskNames().values();
      for (int i = 1; i <= hdfsFrameworkConfig.getJournalNodeCount(); i++) {
        if (!journalNodeTaskNames.contains(HDFSConstants.JOURNAL_NODE_ID + i)) {
          return HDFSConstants.JOURNAL_NODE_ID + i;
        }
      }
      String errorStr = "Cluster is in inconsistent state. " +
        "Trying to launch more journalnodes, but they all are already running.";
      log.error(errorStr);
      throw new SchedulerException(errorStr);
    }
    return taskType;
  }

  private ExecutorInfo createExecutor(String taskIdName, String nodeName, String executorName,
    List<Resource> resources) {
    int confServerPort = hdfsFrameworkConfig.getConfigServerPort();

    String cmd = "export JAVA_HOME=$MESOS_DIRECTORY/" + hdfsFrameworkConfig.getJreVersion()
      + " && env ; cd hdfs-mesos-* && "
      + "exec `if [ -z \"$JAVA_HOME\" ]; then echo java; "
      + "else echo $JAVA_HOME/bin/java; fi` "
      + "$HADOOP_OPTS "
      + "$EXECUTOR_OPTS "
      + "-cp lib/*.jar org.apache.mesos.hdfs.executor." + executorName;

    return ExecutorInfo
      .newBuilder()
      .setName(nodeName + " executor")
      .setExecutorId(ExecutorID.newBuilder().setValue("executor." + taskIdName).build())
      .addAllResources(resources)
      .setCommand(
        CommandInfo
          .newBuilder()
          .addAllUris(
            Arrays.asList(
              CommandInfo.URI
                .newBuilder()
                .setValue(
                  String.format("http://%s:%d/%s", hdfsFrameworkConfig.getFrameworkHostAddress(),
                    confServerPort,
                    HDFSConstants.HDFS_BINARY_FILE_NAME))
                .build(),
              CommandInfo.URI
                .newBuilder()
                .setValue(
                  String.format("http://%s:%d/%s", hdfsFrameworkConfig.getFrameworkHostAddress(),
                    confServerPort,
                    HDFSConstants.HDFS_CONFIG_FILE_NAME))
                .build(),
              CommandInfo.URI
                .newBuilder()
                .setValue(hdfsFrameworkConfig.getJreUrl())
                .build()))
          .setEnvironment(Environment.newBuilder()
            .addAllVariables(Arrays.asList(
              Environment.Variable.newBuilder()
                .setName("LD_LIBRARY_PATH")
                .setValue(hdfsFrameworkConfig.getLdLibraryPath()).build(),
              Environment.Variable.newBuilder()
                .setName("HADOOP_OPTS")
                .setValue(hdfsFrameworkConfig.getJvmOpts()).build(),
              Environment.Variable.newBuilder()
                .setName("HADOOP_HEAPSIZE")
                .setValue(String.format("%d", hdfsFrameworkConfig.getHadoopHeapSize())).build(),
              Environment.Variable.newBuilder()
                .setName("HADOOP_NAMENODE_OPTS")
                .setValue("-Xmx" + hdfsFrameworkConfig.getNameNodeHeapSize()
                  + "m -Xms" + hdfsFrameworkConfig.getNameNodeHeapSize() + "m").build(),
              Environment.Variable.newBuilder()
                .setName("HADOOP_DATANODE_OPTS")
                .setValue("-Xmx" + hdfsFrameworkConfig.getDataNodeHeapSize()
                  + "m -Xms" + hdfsFrameworkConfig.getDataNodeHeapSize() + "m").build(),
              Environment.Variable.newBuilder()
                .setName("EXECUTOR_OPTS")
                .setValue("-Xmx" + hdfsFrameworkConfig.getExecutorHeap()
                  + "m -Xms" + hdfsFrameworkConfig.getExecutorHeap() + "m").build())))
          .setValue(cmd).build())
      .build();
  }

  private List<Resource> getExecutorResources() {
    return Arrays.asList(
      Resource.newBuilder()
        .setName("cpus")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder()
          .setValue(hdfsFrameworkConfig.getExecutorCpus()).build())
        .setRole(hdfsFrameworkConfig.getHdfsRole())
        .build(),
      Resource.newBuilder()
        .setName("mem")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder()
          .setValue(hdfsFrameworkConfig.getExecutorHeap() * hdfsFrameworkConfig.getJvmOverhead()).build())
        .setRole(hdfsFrameworkConfig.getHdfsRole())
        .build());
  }

  private List<Resource> getTaskResources(String taskName) {
    return Arrays.asList(
      Resource.newBuilder()
        .setName("cpus")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder()
          .setValue(hdfsFrameworkConfig.getTaskCpus(taskName)).build())
        .setRole(hdfsFrameworkConfig.getHdfsRole())
        .build(),
      Resource.newBuilder()
        .setName("mem")
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder()
          .setValue(hdfsFrameworkConfig.getTaskHeapSize(taskName) *
            hdfsFrameworkConfig.getJvmOverhead()).build())
        .setRole(hdfsFrameworkConfig.getHdfsRole())
        .build());
  }

  private boolean tryToLaunchJournalNode(SchedulerDriver driver, Offer offer) {
    if (offerNotEnoughResources(offer, hdfsFrameworkConfig.getJournalNodeCpus(),
      hdfsFrameworkConfig.getJournalNodeHeapSize())) {
      log.info("Offer does not have enough resources");
      return false;
    }

    boolean launch = false;
    List<String> deadJournalNodes = persistenceStore.getDeadJournalNodes();

    log.info(deadJournalNodes);

    if (deadJournalNodes.isEmpty()) {
      if (persistenceStore.getJournalNodes().size() == hdfsFrameworkConfig.getJournalNodeCount()) {
        log.info(String.format("Already running %s journalnodes", hdfsFrameworkConfig.getJournalNodeCount()));
      } else if (persistenceStore.journalNodeRunningOnSlave(offer.getHostname())) {
        log.info(String.format("Already running journalnode on %s", offer.getHostname()));
      } else if (persistenceStore.dataNodeRunningOnSlave(offer.getHostname())) {
        log.info(String.format("Cannot colocate journalnode and datanode on %s",
          offer.getHostname()));
      } else {
        launch = true;
      }
    } else if (deadJournalNodes.contains(offer.getHostname())) {
      launch = true;
    }
    if (launch) {
      return launchNode(
        driver,
        offer,
        HDFSConstants.JOURNAL_NODE_ID,
        Arrays.asList(HDFSConstants.JOURNAL_NODE_ID),
        HDFSConstants.NODE_EXECUTOR_ID);
    }
    return false;
  }

  private boolean tryToLaunchNameNode(SchedulerDriver driver, Offer offer) {
    if (offerNotEnoughResources(offer,
      (hdfsFrameworkConfig.getNameNodeCpus() + hdfsFrameworkConfig.getZkfcCpus()),
      (hdfsFrameworkConfig.getNameNodeHeapSize() + hdfsFrameworkConfig.getZkfcHeapSize()))) {
      log.info("Offer does not have enough resources");
      return false;
    }

    boolean launch = false;
    List<String> deadNameNodes = persistenceStore.getDeadNameNodes();

    if (deadNameNodes.isEmpty()) {
      if (persistenceStore.getNameNodes().size() == HDFSConstants.TOTAL_NAME_NODES) {
        log.info(String.format("Already running %s namenodes", HDFSConstants.TOTAL_NAME_NODES));
      } else if (persistenceStore.nameNodeRunningOnSlave(offer.getHostname())) {
        log.info(String.format("Already running namenode on %s", offer.getHostname()));
      } else if (persistenceStore.dataNodeRunningOnSlave(offer.getHostname())) {
        log.info(String.format("Cannot colocate namenode and datanode on %s", offer.getHostname()));
      } else if (!persistenceStore.journalNodeRunningOnSlave(offer.getHostname())) {
        log.info(String.format("We need to coloate the namenode with a journalnode and there is"
          + "no journalnode running on this host. %s", offer.getHostname()));
      } else {
        launch = true;
      }
    } else if (deadNameNodes.contains(offer.getHostname())) {
      launch = true;
    }
    if (launch) {
      return launchNode(
        driver,
        offer,
        HDFSConstants.NAME_NODE_ID,
        Arrays.asList(HDFSConstants.NAME_NODE_ID, HDFSConstants.ZKFC_NODE_ID),
        HDFSConstants.NAME_NODE_EXECUTOR_ID);
    }
    return false;
  }

  private boolean tryToLaunchDataNode(SchedulerDriver driver, Offer offer) {
    if (offerNotEnoughResources(offer, hdfsFrameworkConfig.getDataNodeCpus(),
      hdfsFrameworkConfig.getDataNodeHeapSize())) {
      log.info("Offer does not have enough resources");
      return false;
    }

    boolean launch = false;
    List<String> deadDataNodes = persistenceStore.getDeadDataNodes();
    // TODO (elingg) Relax this constraint to only wait for DN's when the number of DN's is small
    // What number of DN's should we try to recover or should we remove this constraint
    // entirely?
    if (deadDataNodes.isEmpty()) {
      if (persistenceStore.dataNodeRunningOnSlave(offer.getHostname())) {
        log.info(String.format("Already running datanode hdfs task on %s", offer.getHostname()));
      } else if ( hdfsFrameworkConfig.getRunDatanodeExclusively() && 
            (persistenceStore.nameNodeRunningOnSlave(offer.getHostname())
           || persistenceStore.journalNodeRunningOnSlave(offer.getHostname()))) {
        log.info(String.format("Already running namenode/journal node hdfs task on %s", offer.getHostname()));
      } else {
        launch = true;
      }
    } else if (deadDataNodes.contains(offer.getHostname())) {
      launch = true;
    }
    if (launch) {
      return launchNode(
        driver,
        offer,
        HDFSConstants.DATA_NODE_ID,
        Arrays.asList(HDFSConstants.DATA_NODE_ID),
        HDFSConstants.NODE_EXECUTOR_ID);
    }
    return false;
  }

  public void sendMessageTo(SchedulerDriver driver, TaskID taskId,
    SlaveID slaveID, String message) {
    log.info(String.format("Sending message '%s' to taskId=%s, slaveId=%s", message,
      taskId.getValue(), slaveID.getValue()));
    String postfix = taskId.getValue();
    postfix = postfix.substring(postfix.indexOf('.') + 1, postfix.length());
    postfix = postfix.substring(postfix.indexOf('.') + 1, postfix.length());
    driver.sendFrameworkMessage(
      ExecutorID.newBuilder().setValue("executor." + postfix).build(),
      slaveID,
      message.getBytes(Charset.defaultCharset()));
  }

  private boolean isTerminalState(TaskStatus taskStatus) {
    return taskStatus.getState().equals(TaskState.TASK_FAILED)
      || taskStatus.getState().equals(TaskState.TASK_FINISHED)
      || taskStatus.getState().equals(TaskState.TASK_KILLED)
      || taskStatus.getState().equals(TaskState.TASK_LOST)
      || taskStatus.getState().equals(TaskState.TASK_ERROR);
  }

  private boolean isRunningState(TaskStatus taskStatus) {
    return taskStatus.getState().equals(TaskState.TASK_RUNNING);
  }

  private boolean isStagingState(TaskStatus taskStatus) {
    return taskStatus.getState().equals(TaskState.TASK_STAGING);
  }

  private void reloadConfigsOnAllRunningTasks(SchedulerDriver driver) {
    if (hdfsFrameworkConfig.usingNativeHadoopBinaries()) {
      return;
    }
    for (Protos.TaskStatus taskStatus : liveState.getRunningTasks().values()) {
      sendMessageTo(driver, taskStatus.getTaskId(), taskStatus.getSlaveId(),
        HDFSConstants.RELOAD_CONFIG);
    }
  }

  private void correctCurrentPhase() {
    if (liveState.getJournalNodeSize() < hdfsFrameworkConfig.getJournalNodeCount()) {
      liveState.transitionTo(AcquisitionPhase.JOURNAL_NODES);
    } else if (liveState.getNameNodeSize() < HDFSConstants.TOTAL_NAME_NODES) {
      liveState.transitionTo(AcquisitionPhase.START_NAME_NODES);
    } else if (!liveState.isNameNode1Initialized()
      || !liveState.isNameNode2Initialized()) {
      liveState.transitionTo(AcquisitionPhase.FORMAT_NAME_NODES);
    } else {
      liveState.transitionTo(AcquisitionPhase.DATA_NODES);
    }
  }

  private boolean offerNotEnoughResources(Offer offer, double cpus, int mem) {
    for (Resource offerResource : offer.getResourcesList()) {
      if (offerResource.getName().equals("cpus") &&
        cpus + hdfsFrameworkConfig.getExecutorCpus() > offerResource.getScalar().getValue()) {
        return true;
      }
      if (offerResource.getName().equals("mem") &&
        (mem * hdfsFrameworkConfig.getJvmOverhead())
          + (hdfsFrameworkConfig.getExecutorHeap() * hdfsFrameworkConfig.getJvmOverhead())
          > offerResource.getScalar().getValue()) {
        return true;
      }
    }
    return false;
  }

  private void reconcileTasks(SchedulerDriver driver) {
    // TODO (elingg) run this method repeatedly with exponential backoff in the case that it takes
    // time for
    // different slaves to reregister upon master failover.
    driver.reconcileTasks(Collections.<Protos.TaskStatus>emptyList());
    Timer timer = new Timer();
    timer.schedule(new ReconcileStateTask(), hdfsFrameworkConfig.getReconciliationTimeout() * SECONDS_FROM_MILLIS);
  }

  private class ReconcileStateTask extends TimerTask {

    @Override
    public void run() {
      log.info("Current persistent state:");
      log.info(String.format("JournalNodes: %s, %s", persistenceStore.getJournalNodes(),
        persistenceStore.getJournalNodeTaskNames()));
      log.info(String.format("NameNodes: %s, %s", persistenceStore.getNameNodes(),
        persistenceStore.getNameNodeTaskNames()));
      log.info(String.format("DataNodes: %s", persistenceStore.getDataNodes()));

      Set<String> taskIds = persistenceStore.getAllTaskIds();
      Set<String> runningTaskIds = liveState.getRunningTasks().keySet();

      for (String taskId : taskIds) {
        if (taskId != null && !runningTaskIds.contains(taskId)) {
          log.info("Removing task id: " + taskId);
          persistenceStore.removeTaskId(taskId);
        }
      }
      correctCurrentPhase();
    }
  }
}
