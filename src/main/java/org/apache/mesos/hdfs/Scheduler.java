package org.apache.mesos.hdfs;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.state.AcquisitionPhase;
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.state.PersistentState;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.hdfs.util.DnsResolver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

//TODO remove as much logic as possible from Scheduler to clean up code
public class Scheduler implements org.apache.mesos.Scheduler, Runnable {

  public static final Log log = LogFactory.getLog(Scheduler.class);
  private final SchedulerConf conf;
  private final LiveState liveState;
  private final PersistentState persistentState;
  private final DnsResolver dnsResolver;
  private boolean reconciliationCompleted;

  @Inject
  public Scheduler(SchedulerConf conf, LiveState liveState, PersistentState persistentState) {
    this.conf = conf;
    this.liveState = liveState;
    this.persistentState = persistentState;
    this.dnsResolver = new DnsResolver(this, conf);
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
      persistentState.setFrameworkId(frameworkId);
    } catch (InterruptedException | ExecutionException e) {
      log.error("Error setting framework id in persistent state", e);
      throw new RuntimeException(e);
    }
    log.info("Registered framework frameworkId=" + frameworkId.getValue());
    //reconcile tasks upon registration
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
      persistentState.removeTaskId(status.getTaskId().getValue());
      // Correct the phase when a task dies after the reconcile period is over
      if (reconciliationComplete()) {
        correctCurrentPhase();
      }
    } else if (isRunningState(status)) {
      liveState.updateTaskForStatus(status);

      log.info(String.format("Current Acquisition Phase: %s", liveState
          .getCurrentAcquisitionPhase().toString()));

      switch (liveState.getCurrentAcquisitionPhase()) {
        case RECONCILING_TASKS :
          if (reconciliationComplete()) {
            correctCurrentPhase();
          }
          break;
        case JOURNAL_NODES :
          if (liveState.getJournalNodeSize() == conf.getJournalNodeCount()) {
            // TODO move the reload to correctCurrentPhase and make it idempotent
            reloadConfigsOnAllRunningTasks(driver);
            correctCurrentPhase();
          }
          break;
        case START_NAME_NODES :
          if (liveState.getNameNodeSize() == (HDFSConstants.TOTAL_NAME_NODES)) {
            // TODO move the reload to correctCurrentPhase and make it idempotent
            reloadConfigsOnAllRunningTasks(driver);
            correctCurrentPhase();
          }
          break;
        case FORMAT_NAME_NODES :
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
        case DATA_NODES :
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
    if (liveState.getCurrentAcquisitionPhase().equals(AcquisitionPhase.RECONCILING_TASKS)
        && reconciliationComplete()) {
      correctCurrentPhase();
    }
    // TODO within each phase, accept offers based on the number of nodes you need
    boolean acceptedOffer = false;
    for (Offer offer : offers) {
      if (acceptedOffer) {
        driver.declineOffer(offer.getId());
      } else {
        switch (liveState.getCurrentAcquisitionPhase()) {
          case RECONCILING_TASKS :
            log.info("Declining offers while reconciling tasks");
            driver.declineOffer(offer.getId());
            break;
          case JOURNAL_NODES :
            if (tryToLaunchJournalNode(driver, offer)) {
              acceptedOffer = true;
            } else {
              driver.declineOffer(offer.getId());
            }
            break;
          case START_NAME_NODES :
            if (dnsResolver.journalNodesResolvable()
                && tryToLaunchNameNode(driver, offer)) {
              acceptedOffer = true;
            } else {
              driver.declineOffer(offer.getId());
            }
            break;
          case FORMAT_NAME_NODES :
            driver.declineOffer(offer.getId());
            break;
          case DATA_NODES :
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
        .setName(conf.getFrameworkName())
        .setFailoverTimeout(conf.getFailoverTimeout())
        .setUser(conf.getHdfsUser())
        .setRole(conf.getHdfsRole())
        .setCheckpoint(true);

    try {
      FrameworkID frameworkID = persistentState.getFrameworkID();
      if (frameworkID != null) {
        frameworkInfo.setId(frameworkID);
      }
    } catch (InterruptedException | ExecutionException | InvalidProtocolBufferException e) {
      log.error("Error recovering framework id", e);
      throw new RuntimeException(e);
    }

    MesosSchedulerDriver driver = new MesosSchedulerDriver(this, frameworkInfo.build(),
        conf.getMesosMasterUri());
    driver.run();
  }

  private boolean launchNode(SchedulerDriver driver, Offer offer,
        String nodeName, List<String> taskTypes, String executorName) {
    //nodeName is the type of executor to launch
    //executorName is to distinguish different types of nodes
    //taskType is the type of task in mesos to launch on the node
    //taskName is a name chosen to identify the task in mesos and mesos-dns (if used)
    log.info(String.format("Launching node of type %s with tasks %s", nodeName,
        taskTypes.toString()));
    String taskIdName = String.format("%s.%s.%d", nodeName, executorName,
        System.currentTimeMillis());
    List<Resource> resources = getExecutorResources();
    ExecutorInfo executorInfo = createExecutor(taskIdName, nodeName, executorName, resources);
    List<TaskInfo> tasks = new ArrayList<>();
    for (String taskType : taskTypes) {
      List<Resource> taskResources = getTaskResources(taskType);
      String taskName = getNextTaskType(taskType);
      if (taskName.isEmpty()) return false;
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

      liveState.addStagingTask(task.getTaskId(), taskName);
      persistentState.addHdfsNode(taskId, offer.getHostname(), taskType);
    }
    driver.launchTasks(Arrays.asList(offer.getId()), tasks);
    return true;
  }

  private String getNextTaskType(String taskType) {
    if (taskType.equals(HDFSConstants.NAME_NODE_ID)) {
      for (int i = 1; i <= HDFSConstants.TOTAL_NAME_NODES; i++) {
        if (!liveState.getNameNodeNames().containsValue(HDFSConstants.NAME_NODE_ID + i)) {
          return HDFSConstants.NAME_NODE_ID + i;
        }
      }
      return ""; // we couldn't find a node name, we must have started enough.
    }
    if (taskType.equals(HDFSConstants.JOURNAL_NODE_ID)) {
      for (int i = 1; i <= conf.getJournalNodeCount(); i++) {
        if (!liveState.getJournalNodeNames().containsValue(HDFSConstants.JOURNAL_NODE_ID + i)) {
          return HDFSConstants.JOURNAL_NODE_ID + i;
        }
      }
      return ""; // we couldn't find a node name, we must have started enough.
    }
    return taskType;
  }

  private ExecutorInfo createExecutor(String taskIdName, String nodeName, String executorName,
      List<Resource> resources) {
    int confServerPort = conf.getConfigServerPort();
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
                                String.format("http://%s:%d/%s", conf.getFrameworkHostAddress(),
                                    confServerPort,
                                    HDFSConstants.HDFS_BINARY_FILE_NAME))
                            .build(),
                        CommandInfo.URI
                            .newBuilder()
                            .setValue(
                                String.format("http://%s:%d/%s", conf.getFrameworkHostAddress(),
                                    confServerPort,
                                    HDFSConstants.HDFS_CONFIG_FILE_NAME))
                            .build()))
                .setEnvironment(Environment.newBuilder()
                    .addAllVariables(Arrays.asList(
                        Environment.Variable.newBuilder()
                            .setName("HADOOP_OPTS")
                            .setValue(conf.getJvmOpts()).build(),
                        Environment.Variable.newBuilder()
                            .setName("HADOOP_HEAPSIZE")
                            .setValue(String.format("%d", conf.getHadoopHeapSize())).build(),
                        Environment.Variable.newBuilder()
                            .setName("HADOOP_NAMENODE_OPTS")
                            .setValue("-Xmx" + conf.getNameNodeHeapSize()
                                + "m -Xms" + conf.getNameNodeHeapSize() + "m").build(),
                        Environment.Variable.newBuilder()
                            .setName("HADOOP_DATANODE_OPTS")
                            .setValue("-Xmx" + conf.getDataNodeHeapSize()
                                + "m -Xms" + conf.getDataNodeHeapSize() + "m").build(),
                        Environment.Variable.newBuilder()
                            .setName("EXECUTOR_OPTS")
                            .setValue("-Xmx" + conf.getExecutorHeap()
                                + "m -Xms" + conf.getExecutorHeap() + "m").build())))
                .setValue(
                    "env ; cd hdfs-mesos-* && "
                        + "exec `if [ -z \"$JAVA_HOME\" ]; then echo java; "
                        + "else echo $JAVA_HOME/bin/java; fi` "
                        + "$HADOOP_OPTS "
                        + "$EXECUTOR_OPTS "
                        + "-cp lib/*.jar org.apache.mesos.hdfs.executor." + executorName).build())
        .build();
  }

  private List<Resource> getExecutorResources() {
    return Arrays.asList(
        Resource.newBuilder()
            .setName("cpus")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder()
                .setValue(conf.getExecutorCpus()).build())
            .setRole(conf.getHdfsRole())
            .build(),
        Resource.newBuilder()
            .setName("mem")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder()
                .setValue(conf.getExecutorHeap() * conf.getJvmOverhead()).build())
            .setRole(conf.getHdfsRole())
            .build());
  }

  private List<Resource> getTaskResources(String taskName) {
    return Arrays.asList(
        Resource.newBuilder()
            .setName("cpus")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder()
                .setValue(conf.getTaskCpus(taskName)).build())
            .setRole(conf.getHdfsRole())
            .build(),
        Resource.newBuilder()
            .setName("mem")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder()
                .setValue(conf.getTaskHeapSize(taskName)).build())
            .setRole(conf.getHdfsRole())
            .build());
  }

  private boolean tryToLaunchJournalNode(SchedulerDriver driver, Offer offer) {
    if (offerNotEnoughResources(offer, conf.getJournalNodeCpus(), conf.getJournalNodeHeapSize())) {
      log.info("Offer does not have enough resources");
      return false;
    }

    boolean launch = false;
    List<String> deadJournalNodes = persistentState.getDeadJournalNodes();

    log.info(deadJournalNodes);

    if (deadJournalNodes.isEmpty()) {
      if (liveState.getJournalNodeSize() == conf.getJournalNodeCount()) {
        log.info(String.format("Already running %s journalnodes", conf.getJournalNodeCount()));
      } else if (persistentState.journalNodeRunningOnSlave(offer.getHostname())) {
        log.info(String.format("Already running journalnode on %s", offer.getHostname()));
      } else if (persistentState.dataNodeRunningOnSlave(offer.getHostname())) {
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
        (conf.getNameNodeCpus() + conf.getZkfcCpus()),
        (conf.getNameNodeHeapSize() + conf.getZkfcHeapSize()))) {
      log.info("Offer does not have enough resources");
      return false;
    }

    boolean launch = false;
    List<String> deadNameNodes = persistentState.getDeadNameNodes();

    if (deadNameNodes.isEmpty()) {
      if (liveState.getNameNodeSize() == HDFSConstants.TOTAL_NAME_NODES) {
        log.info(String.format("Already running %s namenodes", HDFSConstants.TOTAL_NAME_NODES));
      } else if (persistentState.nameNodeRunningOnSlave(offer.getHostname())) {
        log.info(String.format("Already running namenode on %s", offer.getHostname()));
      } else if (persistentState.dataNodeRunningOnSlave(offer.getHostname())) {
        log.info(String.format("Cannot colocate namenode and datanode on %s", offer.getHostname()));
      } else if (!persistentState.journalNodeRunningOnSlave(offer.getHostname())) {
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
    if (offerNotEnoughResources(offer, conf.getDataNodeCpus(), conf.getDataNodeHeapSize())) {
      log.info("Offer does not have enough resources");
      return false;
    }

    boolean launch = false;
    List<String> deadDataNodes = persistentState.getDeadDataNodes();
    // TODO (elingg) Relax this constraint to only wait for DN's when the number of DN's is small
    // What number of DN's should we try to recover or should we remove this constraint
    // entirely?
    if (deadDataNodes.isEmpty()) {
      if (persistentState.dataNodeRunningOnSlave(offer.getHostname())
          || persistentState.nameNodeRunningOnSlave(offer.getHostname())
          || persistentState.journalNodeRunningOnSlave(offer.getHostname())) {
        log.info(String.format("Already running hdfs task on %s", offer.getHostname()));
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
    postfix = postfix.substring(postfix.indexOf(".") + 1, postfix.length());
    postfix = postfix.substring(postfix.indexOf(".") + 1, postfix.length());
    driver.sendFrameworkMessage(
        ExecutorID.newBuilder().setValue("executor." + postfix).build(),
        slaveID,
        message.getBytes());
  }

  private boolean isTerminalState(TaskStatus taskStatus) {
    return (taskStatus.getState().equals(TaskState.TASK_FAILED)
        || taskStatus.getState().equals(TaskState.TASK_FINISHED)
        || taskStatus.getState().equals(TaskState.TASK_KILLED)
        || taskStatus.getState().equals(TaskState.TASK_LOST)
        || taskStatus.getState().equals(TaskState.TASK_ERROR));
  }

  private boolean isRunningState(TaskStatus taskStatus) {
    return (taskStatus.getState().equals(TaskState.TASK_RUNNING));
  }

  private boolean isStagingState(TaskStatus taskStatus) {
    return (taskStatus.getState().equals(TaskState.TASK_STAGING));
  }

  private void reloadConfigsOnAllRunningTasks(SchedulerDriver driver) {
    for (Protos.TaskStatus taskStatus : liveState.getRunningTasks().values()) {
      sendMessageTo(driver, taskStatus.getTaskId(), taskStatus.getSlaveId(),
          HDFSConstants.RELOAD_CONFIG);
    }
  }

  private void correctCurrentPhase() {
    if (liveState.getJournalNodeSize() < conf.getJournalNodeCount()) {
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
          cpus > offerResource.getScalar().getValue()) {
        return true;
      }
      if (offerResource.getName().equals("mem") &&
          mem > offerResource.getScalar().getValue()) {
        return true;
      }
    }
    return false;
  }

  private void reconcileTasks(SchedulerDriver driver) {
    // TODO run this method repeatedly with exponential backoff in the case that it takes time for
    // different slaves to reregister upon master failover.
    reconciliationCompleted = false;
    driver.reconcileTasks(Collections.<Protos.TaskStatus> emptyList());
    Timer timer = new Timer();
    timer.schedule(new ReconcileStateTask(), conf.getReconciliationTimeout() * 1000);
  }

  private boolean reconciliationComplete() {
    return reconciliationCompleted;
  }

  private class ReconcileStateTask extends TimerTask {

    @Override
    public void run() {
      log.info("Current persistent state:");
      log.info(String.format("JournalNodes: %s", persistentState.getJournalNodes()));
      log.info(String.format("NameNodes: %s", persistentState.getNameNodes()));
      log.info(String.format("DataNodes: %s", persistentState.getDataNodes()));

      Collection<String> taskIds = persistentState.getAllTaskIds();
      HashMap<String, Protos.TaskStatus> runningTaskIds = liveState.getRunningTasks();
      for (String taskId : taskIds) {
        if (!runningTaskIds.containsKey(taskId))
          persistentState.removeTaskId(taskId);
      }
      reconciliationCompleted = true;
    }
  }
}
