package org.apache.mesos.hdfs.scheduler;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.Credential;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.AcquisitionPhase;
import org.apache.mesos.hdfs.state.HdfsState;
import org.apache.mesos.hdfs.state.StateMachine;
import org.apache.mesos.hdfs.util.DnsResolver;
import org.apache.mesos.process.FailureUtils;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.protobuf.ExecutorInfoBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Observable;
import java.util.concurrent.ExecutionException;

/**
 * HDFS Mesos Framework Scheduler class implementation.
 */
public class HdfsScheduler extends Observable implements org.apache.mesos.Scheduler, Runnable {
  private final Log log = LogFactory.getLog(HdfsScheduler.class);
  private final HdfsFrameworkConfig config;
  private HdfsMesosConstraints hdfsMesosConstraints;
  private final HdfsState state;
  private final StateMachine stateMachine;
  private final DnsResolver dnsResolver;
  private NodeLauncher launcher;

  @Inject
  public HdfsScheduler(HdfsFrameworkConfig config, HdfsState state, StateMachine stateMachine) {
    this.config = config;
    this.hdfsMesosConstraints = new HdfsMesosConstraints(this.config);
    this.dnsResolver = new DnsResolver(this, config);
    this.state = state;
    this.stateMachine = stateMachine;
    launcher = new NodeLauncher();

    addObserver(stateMachine.getReconciler());
    addObserver(state);
  }

  @Override
  public void disconnected(SchedulerDriver driver) {
    log.info("Scheduler driver disconnected");
  }

  @Override
  public void error(SchedulerDriver driver, String message) {
    log.error("Scheduler driver error: " + message);
    // Currently, it's pretty hard to disambiguate this error from other causes of framework errors.
    // Watch MESOS-2522 which will add a reason field for framework errors to help with this.
    // For now the frameworkId is removed for all messages.
    boolean removeFrameworkId = message.contains("re-register");
    exitOnError(removeFrameworkId, message);
  }
  
  /**
    * Exits the JVM process, optionally deleting Hdfs FrameworkID
    * from the backing persistence store.
    *
    * If `removeFrameworkId` is set, the next Hdfs mesos process elected
    * leader will fail to find a stored FrameworkID and invoke `register`
    * instead of `reregister`.  This is important because on certain kinds
    * of framework errors (such as exceeding the framework failover timeout),
    * the scheduler may never re-register with the saved FrameworkID until
    * the leading Mesos master process is killed.
    */
  private void exitOnError(Boolean removeFrameworkId, String message) {
    if (removeFrameworkId) {
      try {
        state.removeFrameworkId();
      } catch (Exception ex) {
        log.error("Failed to remove FrameworkId with exception: " + ex);
      }

      throw new SchedulerException("Scheduler driver error: " + message);        
    }
  }
  
  @Override
  public void executorLost(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID, int status) {
    log.info("Executor lost: executorId=" + executorID.getValue()
      + " slaveId=" + slaveID.getValue() + " status=" + status);
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
      state.setFrameworkId(frameworkId);
    } catch (IOException | InterruptedException | ExecutionException e) {
      // these are zk exceptions... we are unable to maintain state.
      final String msg = "Error setting framework id in persistent state";
      log.error(msg, e);
      throw new SchedulerException(msg, e);
    }
    log.info("Registered framework frameworkId=" + frameworkId.getValue());
    stateMachine.reconcile(driver);
  }

  @Override
  public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
    log.info("Reregistered framework: starting task reconciliation");
    stateMachine.reconcile(driver);
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    log.info(String.format(
      "Received status update for taskId=%s state=%s message='%s'",
      status.getTaskId().getValue(),
      status.getState().toString(),
      status.getMessage()));

    log.info("Notifying observers of TaskStatus: " + status);
    setChanged();
    notifyObservers(status);

    reloadConfigsOnAllRunningTasks(driver);
    stateMachine.correctPhase();
  }

  private void logOffers(List<Offer> offers) {
    if (offers == null) {
      return;
    }
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
        stateMachine.getCurrentPhase(),
        offerId));

    driver.declineOffer(offerId);
  }

  @Override
  public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
    logOffers(offers);

    if (stateMachine.getCurrentPhase() == AcquisitionPhase.RECONCILING_TASKS) {
      stateMachine.correctPhase();
    }

    boolean acceptedOffer = false;
    for (Offer offer : offers) {
      if (acceptedOffer) {
        driver.declineOffer(offer.getId());
      } else if (!hdfsMesosConstraints.constraintsAllow(offer)) {
        driver.declineOffer(offer.getId());
      } else {
        try {
          HdfsNode node = null;

          switch (stateMachine.getCurrentPhase()) {
            case RECONCILING_TASKS:
              declineOffer(driver, offer);
              break;
            case JOURNAL_NODES:
              node = new JournalNode(state, config);
              break;
            case NAME_NODES:
              node = new NameNode(state, dnsResolver, config);
              break;
            case DATA_NODES:
              node = new DataNode(state, config);
              break;
          }

          if (node != null) {
            acceptedOffer = launcher.tryLaunch(driver, offer, node);
          }
        } catch (Exception ex) {
          log.error("Declining offer with exception: " + ex.getMessage()
            + " and stack: " + ExceptionUtils.getStackTrace(ex));
          declineOffer(driver, offer);
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
      .setPrincipal(config.getPrincipal())
      .setCheckpoint(true);

    try {
      FrameworkID frameworkID = state.getFrameworkId();
      if (frameworkID != null) {
        frameworkInfo.setId(frameworkID);
      }
    } catch (ClassNotFoundException | ExecutionException | InterruptedException | IOException e) {
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
      ExecutorInfoBuilder.createExecutorId("executor." + postfix),
      slaveID,
      message.getBytes(Charset.defaultCharset()));
  }

  private void reloadConfigsOnAllRunningTasks(SchedulerDriver driver) {
    if (config.usingNativeHadoopBinaries()) {
      return;
    }

    List<Task> tasks = null;
    try {
      tasks = state.getTasks();
    } catch (Exception ex) {
      FailureUtils.exit("Reloading configurations failed", HDFSConstants.RELOAD_EXIT_CODE);
    }

    for (Task task : tasks) {
      TaskStatus status = task.getStatus();
      if (status != null) {
        sendMessageTo(driver, status.getTaskId(), status.getSlaveId(),
          HDFSConstants.RELOAD_CONFIG);
      }
    }
  }
}
