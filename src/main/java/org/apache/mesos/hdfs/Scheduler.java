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

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Scheduler implements org.apache.mesos.Scheduler, Runnable {

  public static final Log log = LogFactory.getLog(Scheduler.class);
  private final SchedulerConf conf;
  private final LiveState liveState;
  private PersistentState persistentState;

  @Inject
  public Scheduler(SchedulerConf conf, LiveState liveState) {
    this(conf, liveState, new PersistentState(conf));
  }

  public Scheduler(SchedulerConf conf, LiveState liveState, PersistentState persistentState) {
    this.conf = conf;
    this.liveState = liveState;
    this.persistentState = persistentState;
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
      throw new RuntimeException(e);
    }
    log.info("Registered framework frameworkId=" + frameworkId.getValue());
  }
  
  @Override
  public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
    log.info("Reregistered framework.");
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
      liveState.removeTask(status.getTaskId());
    } else if (isRunningState(status)) {
      liveState.updateTaskForStatus(status); // TODO(rubbish) could terminal/running get pushed to
                                             // this

      log.info(String.format("Current Acquisition Phase: %s", liveState
          .getCurrentAcquisitionPhase().toString()));

      switch (liveState.getCurrentAcquisitionPhase()) {
        case JOURNAL_NODES :
          if (liveState.getJournalNodeSize() == conf.getJournalNodeCount()) {
            liveState.transitionTo(AcquisitionPhase.NAME_NODE_1);
          }
          break;
        case NAME_NODE_1 :
          if (liveState.getNameNodeSize() == 1 && liveState.getFirstNameNodeTaskId() != null) {
            liveState.transitionTo(AcquisitionPhase.NAME_NODE_2);
          } else {
            log.info("Cannot locate first namenode task id");
          }
          break;
        case NAME_NODE_2 :
          if (liveState.getNameNodeSize() == HDFSConstants.TOTAL_NAME_NODES
              && liveState.getSecondNameNodeTaskId() != null) {
            reloadConfigsOnAllRunningTasks(driver);
            sendMessageTo(
                driver,
                liveState.getFirstNameNodeTaskId(), liveState.getFirstNameNodeSlaveId(),
                HDFSConstants.NAME_NODE_INIT_MESSAGE);
            blockUntilNameNode1Healthy();
            sendMessageTo(
                driver,
                liveState.getSecondNameNodeTaskId(), liveState.getSecondNameNodeSlaveId(),
                HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE);
            liveState.transitionTo(AcquisitionPhase.DATA_NODES);
          } else {
            log.info("Cannot locate second namenode task id");
          }
          break;
        case DATA_NODES :
          break;
      }
    } else {
      log.warn(String.format("Don't know how to handle state=%s for taskId=%s",
          status.getState(), status.getTaskId().getValue()));
    }
  }

  @Override
  synchronized public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
    log.info(String.format("Received %d offers", offers.size()));

    if (liveState.getStagingTasksSize() != 0) {
      log.info("Declining offers because tasks are currently staging");
      for (Offer offer : offers) {
        driver.declineOffer(offer.getId());
      }
    } else {
      boolean acceptedOffer = false;
      for (Offer offer : offers) {
        if (acceptedOffer) {
          driver.declineOffer(offer.getId());
        } else {
          switch (liveState.getCurrentAcquisitionPhase()) {
            case JOURNAL_NODES :
              if (liveState.getJournalNodeSize() < conf.getJournalNodeCount()) {
                if (maybeLaunchJournalNode(driver, offer)) {
                  acceptedOffer = true;
                } else {
                  driver.declineOffer(offer.getId());
                }
              }
              break;
            case NAME_NODE_1 :
              if (maybeLaunchNameNode(driver, offer)) {
                acceptedOffer = true;
              } else {
                driver.declineOffer(offer.getId());
              }
              break;
            case NAME_NODE_2 :
              if (maybeLaunchNameNode(driver, offer)) {
                acceptedOffer = true;
              } else {
                driver.declineOffer(offer.getId());
              }
              break;
            case DATA_NODES :
              if (maybeLaunchDataNode(driver, offer)) {
                acceptedOffer = true;
              } else {
                driver.declineOffer(offer.getId());
              }
              break;
          }
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
        .setName("HDFS " + conf.getFrameworkName())
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
      throw new RuntimeException(e);
    }

    MesosSchedulerDriver driver = new MesosSchedulerDriver(this, frameworkInfo.build(),
        conf.getMesosMasterUri());
    driver.run().getValueDescriptor().getFullName();
  }
  private void launchNode(SchedulerDriver driver, Offer offer,
        String nodeName, List<String> taskNames, String executorName) {
    log.info(String.format("Launching node of type %s with tasks %s", nodeName,
        taskNames.toString()));
    String taskIdName = String.format("%s.%s.%d", nodeName, executorName,
        System.currentTimeMillis());
    List<Resource> resources = getExecutorResources();
    ExecutorInfo executorInfo = createExecutor(taskIdName, nodeName, executorName, resources);
    List<TaskInfo> tasks = new ArrayList<>();
    for (String taskName : taskNames) {
      List<Resource> taskResources = getTaskResources(taskName);
      TaskID taskId = TaskID.newBuilder()
          .setValue(String.format("task.%s.%s", taskName, taskIdName))
          .build();
      TaskInfo task = TaskInfo.newBuilder()
          .setExecutor(executorInfo)
          .setName(taskName)
          .setTaskId(taskId)
          .setSlaveId(offer.getSlaveId())
          .addAllResources(taskResources)
          .setData(ByteString.copyFromUtf8(
              String.format("bin/hdfs-mesos-%s", taskName)))
          .build();
      tasks.add(task);

      liveState.addStagingTask(task);
      persistentState.addNode(taskId, offer.getHostname(), taskName);
    }
    driver.launchTasks(Arrays.asList(offer.getId()), tasks);
  }
  private ExecutorInfo createExecutor(String taskIdName, String nodeName, String executorName,
        List<Resource> resources) {
    int confServerPort = conf.getConfigServerPort();
    return ExecutorInfo
        .newBuilder()
        .setName(nodeName + " executor")
        .setExecutorId(ExecutorID.newBuilder().setValue("executor." + taskIdName).build())
        .addAllResources(resources)
        .setCommand(CommandInfo.newBuilder()
        .addAllUris(Arrays.asList(
            CommandInfo.URI.newBuilder().setValue(
                String.format("http://%s:%d/%s", conf.getFrameworkHostAddress(), confServerPort,
                    HDFSConstants.HDFS_BINARY_FILE_NAME))
                .build(),
            CommandInfo.URI.newBuilder().setValue(
                String.format("http://%s:%d/%s", conf.getFrameworkHostAddress(), confServerPort,
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
                    .setValue("-Xmx" + conf.getNameNodeHeapSize() + "m -Xms" + conf.getNameNodeHeapSize() + "m").build(),
                Environment.Variable.newBuilder()
                    .setName("HADOOP_DATANODE_OPTS")
                    .setValue("-Xmx" + conf.getDataNodeHeapSize() + "m -Xms" + conf.getDataNodeHeapSize() + "m").build(),
                Environment.Variable.newBuilder()
                    .setName("EXECUTOR_OPTS")
                    .setValue("-Xmx" + conf.getExecutorHeap() + "m -Xms" + conf.getExecutorHeap() + "m").build())))
                    .setValue(
                        "env ; cd hdfs-mesos-* && " +
                          "exec `if [ -z \"$JAVA_HOME\" ]; then echo java; else echo $JAVA_HOME/bin/java; fi` " +
                            "$HADOOP_OPTS " +
                            "$EXECUTOR_OPTS " +
                            "-cp lib/*.jar org.apache.mesos.hdfs.executor." + executorName).build())
                    .build();
    }
    
  private List<Resource> getExecutorResources() {
    return Arrays.asList(
        Resource.newBuilder()
            .setName("cpus")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder()
                .setValue(conf.getExecutorCpus()).build())
            .setRole("*")
            .build(),
         Resource.newBuilder()
            .setName("mem")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder()
                .setValue(conf.getExecutorHeap() * conf.getJvmOverhead()).build())
            .setRole("*")
            .build());
  }
    
  private List<Resource> getTaskResources(String taskName) {
    return Arrays.asList(
        Resource.newBuilder()
            .setName("cpus")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder()
                .setValue(conf.getTaskCpus(taskName)).build())
            .setRole("*")
            .build(),
        Resource.newBuilder()
            .setName("mem")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder()
                .setValue(conf.getTaskHeapSize(taskName)).build())
            .setRole("*")
            .build());
  }

  private boolean maybeLaunchJournalNode(SchedulerDriver driver, Offer offer) {
    if (persistentState.journalNodeRunningOnSlave(offer.getHostname())) {
      log.info(String.format("Already running journalnode on %s", offer.getHostname()));
      return false;
    } else {
      launchNode(
          driver,
          offer,
          HDFSConstants.JOURNAL_NODE_ID,
          Arrays.asList(HDFSConstants.JOURNAL_NODE_ID),
          HDFSConstants.NODE_EXECUTOR_ID);
      return true;
    }
  }

  private boolean maybeLaunchNameNode(SchedulerDriver driver, Offer offer) {
    if (persistentState.nameNodeRunningOnSlave(offer.getHostname())) {
      log.info(String.format("Already running namenode on %s", offer.getHostname()));
      return false;
    } else {
      launchNode(
          driver,
          offer,
          HDFSConstants.NAME_NODE_ID,
          Arrays.asList(HDFSConstants.NAME_NODE_ID, HDFSConstants.ZKFC_NODE_ID),
          HDFSConstants.NAME_NODE_EXECUTOR_ID);
      return true;
    }
  }

  private boolean maybeLaunchDataNode(SchedulerDriver driver, Offer offer) {
    if (persistentState.dataNodeRunningOnSlave(offer.getHostname()) ||
        persistentState.nameNodeRunningOnSlave(offer.getHostname()) ||
        persistentState.journalNodeRunningOnSlave(offer.getHostname())) {
      log.info(String.format("Already running hdfs task on %s", offer.getHostname()));
      return false;
    } else {
      launchNode(
          driver,
          offer,
          HDFSConstants.DATA_NODE_ID,
          Arrays.asList(HDFSConstants.DATA_NODE_ID),
          HDFSConstants.NODE_EXECUTOR_ID);
      return true;
    }
  }

  private void sendMessageTo(SchedulerDriver driver, TaskID taskId, SlaveID slaveID, String message) {
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
        || taskStatus.getState().equals(TaskState.TASK_LOST));
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

  private void blockUntilNameNode1Healthy() {
    //TODO need to implement this method
    try {
      Thread.sleep(30000L);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
