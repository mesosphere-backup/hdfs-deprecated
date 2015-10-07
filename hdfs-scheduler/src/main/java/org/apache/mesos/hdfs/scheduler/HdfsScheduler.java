package org.apache.mesos.hdfs.scheduler;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.AcquisitionPhase;
import org.apache.mesos.hdfs.state.IPersistentStateStore;
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.state.PersistenceException;
import org.apache.mesos.hdfs.util.DnsResolver;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Observable;

/**
 * HDFS Mesos Framework Scheduler class implementation.
 */
public class HdfsScheduler extends Observable implements org.apache.mesos.Scheduler, Runnable {
  // TODO (elingg) remove as much logic as possible from Scheduler to clean up code
  private final Log log = LogFactory.getLog(HdfsScheduler.class);
  private final HdfsFrameworkConfig config;
  private final HdfsMesosConstraints hdfsMesosConstraints;
  private final LiveState liveState;
  private final IPersistentStateStore persistenceStore;
  private final DnsResolver dnsResolver;
  private final Reconciler reconciler;

  @Inject
  public HdfsScheduler(HdfsFrameworkConfig config,
    LiveState liveState, IPersistentStateStore persistenceStore) {

    this.config = config;
    this.hdfsMesosConstraints 
           = new HdfsMesosConstraints(this.config);
    this.liveState = liveState;
    this.persistenceStore = persistenceStore;
    this.dnsResolver = new DnsResolver(this, config);
    this.reconciler = new Reconciler(config, persistenceStore);
    addObserver(reconciler);
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
    reconcile(driver);
  }

  @Override
  public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
    log.info("Reregistered framework: starting task reconciliation");
    reconcile(driver);
  }

  
  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    log.info(String.format(
      "Received status update for taskId=%s state=%s message='%s' stagingTasks.size=%d",
      status.getTaskId().getValue(),
      status.getState().toString(),
      status.getMessage(),
      liveState.getStagingTasksSize()));

    log.info("Notifying observers");
    setChanged();
    notifyObservers(status);

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
          if (liveState.getJournalNodeSize() == config.getJournalNodeCount()) {
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
          if (!liveState.isNameNode1Initialized() && !liveState.isNameNode2Initialized()) {
            initActiveNN(driver);
          } else if (!liveState.isNameNode1Initialized()) {
            initStandbyNN(driver, liveState.getFirstNameNodeTaskId(), liveState.getFirstNameNodeSlaveId());
          } else if (!liveState.isNameNode2Initialized()) {
            initStandbyNN(driver, liveState.getSecondNameNodeTaskId(), liveState.getSecondNameNodeSlaveId());
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

  private void initActiveNN(SchedulerDriver driver) {
    TaskID taskId = liveState.getFirstNameNodeTaskId();
    SlaveID slaveId = liveState.getFirstNameNodeSlaveId();

    if (isNN2BackupMoreRecent()) {
      taskId = liveState.getSecondNameNodeTaskId();
      slaveId = liveState.getSecondNameNodeSlaveId();
    }

    String message = hasNNBackup() ? HDFSConstants.JOURNAL_NODE_INIT_MESSAGE : HDFSConstants.NAME_NODE_INIT_MESSAGE;
    dnsResolver.sendMessageAfterNNResolvable(driver, taskId, slaveId, message);
  }

  private void initStandbyNN(SchedulerDriver driver, TaskID taskId, SlaveID slaveId) {
    dnsResolver.sendMessageAfterNNResolvable(
        driver,
        taskId,
        slaveId,
        HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE);
  }

  private boolean hasNNBackup() {
    return config.getBackupDir() != null && new File(config.getBackupDir() + "/namenode1").exists();
  }

  @SuppressWarnings("SimplifiableIfStatement")
  private boolean isNN2BackupMoreRecent() {
    if (config.getBackupDir() == null) return false;

    File nn1TxFile = new File(config.getBackupDir() + "/namenode1/current/seen_txid");
    File nn2TxFile = new File(config.getBackupDir() + "/namenode2/current/seen_txid");
    if (!nn1TxFile.exists() || !nn2TxFile.exists()) return false;

    return readTxNum(nn2TxFile) > readTxNum(nn1TxFile);
  }

  private int readTxNum(File txFile) {
    int tx = 0;

    try (BufferedReader reader = new BufferedReader(new FileReader(txFile))) {
      tx = Integer.parseInt(reader.readLine());
    } catch (IOException ignore) {}

    return tx;
  }

  private void logOffers(List<Offer> offers) {
    log.info(String.format("Received %d offers", offers.size()));

    for (Offer offer : offers) {
      log.info(String.format("%s", offer.getId()));
    }
  }

  private void declineOffer(SchedulerDriver driver, Offer offer) {
    OfferID offerId = offer.getId();

    log.info(
      String.format(
        "Scheduler in phase: %s, declining offer: %s",
        liveState.getCurrentAcquisitionPhase(),
        offerId));

    driver.declineOffer(offerId);
  }

  @Override
  public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
    logOffers(offers);

    if (liveState.getCurrentAcquisitionPhase() == AcquisitionPhase.RECONCILING_TASKS && reconciler.complete()) {
      correctCurrentPhase();
    }

    // TODO (elingg) within each phase, accept offers based on the number of nodes you need
    boolean acceptedOffer = false;
    for (Offer offer : offers) {
      if (!hdfsMesosConstraints.constraintsAllow(offer)) {
        driver.declineOffer(offer.getId());
      } else if (acceptedOffer) {
        driver.declineOffer(offer.getId());
      } else {
        switch (liveState.getCurrentAcquisitionPhase()) {
          case RECONCILING_TASKS:
            declineOffer(driver, offer);
            break;
          case JOURNAL_NODES:
            JournalNode jn = new JournalNode(liveState, persistenceStore, config);
            acceptedOffer = jn.tryLaunch(driver, offer);
            break;
          case START_NAME_NODES:
            NameNode nn = new NameNode(liveState, persistenceStore, dnsResolver, config);
            acceptedOffer = nn.tryLaunch(driver, offer);
            break;
          case FORMAT_NAME_NODES:
            declineOffer(driver, offer);
            break;
          case DATA_NODES:
            DataNode dn = new DataNode(liveState, persistenceStore, config);
            acceptedOffer = dn.tryLaunch(driver, offer);
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
      .setName(config.getFrameworkName())
      .setFailoverTimeout(config.getFailoverTimeout())
      .setUser(config.getHdfsUser())
      .setRole(config.getHdfsRole())
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

    registerFramework(this, frameworkInfo.build(), config.getMesosMasterUri());
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
    if (config.cramCredentialsEnabled()) {
      try {
        Credential.Builder credentialBuilder = Credential.newBuilder()
          .setPrincipal(config.getPrincipal())
          .setSecret(ByteString.copyFrom(config.getSecret().getBytes("UTF-8")));

        return credentialBuilder.build();

      } catch (UnsupportedEncodingException ex) {
        log.error("Failed to encode secret when creating Credential.");
      }
    }

    return null;
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
    if (config.usingNativeHadoopBinaries()) {
      return;
    }
    for (Protos.TaskStatus taskStatus : liveState.getRunningTasks().values()) {
      sendMessageTo(driver, taskStatus.getTaskId(), taskStatus.getSlaveId(),
        HDFSConstants.RELOAD_CONFIG);
    }
  }

  private void correctCurrentPhase() {
    if (liveState.getJournalNodeSize() < config.getJournalNodeCount()) {
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

  private void reconcile(SchedulerDriver driver) {
    liveState.transitionTo(AcquisitionPhase.RECONCILING_TASKS);
    reconciler.reconcile(driver);
  }
}
