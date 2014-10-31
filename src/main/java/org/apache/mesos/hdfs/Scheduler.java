package org.apache.mesos.hdfs;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.*;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.state.ClusterState;
import org.apache.mesos.hdfs.util.ResourceRoles;
import org.apache.mesos.hdfs.util.ResourceUtils;
import org.joda.time.DateTime;
import org.joda.time.Seconds;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class Scheduler implements org.apache.mesos.Scheduler, Runnable {
  public static final Log log = LogFactory.getLog(Scheduler.class);
  private final SchedulerConf conf;
  private final String localhost;
  private final ClusterState clusterState;
  private final ResourceUtils resourceUtils = new ResourceUtils(this);
  private Map<OfferID, Offer> pendingOffers;
  private DateTime reconciledAt;
  private boolean frameworkInitialized = false;
  private boolean initializingCluster = false;
  private Set<TaskID> stagingTasks = new HashSet<>();

  @Inject
  public Scheduler(SchedulerConf conf, ClusterState clusterState) {
    this.clusterState = clusterState;
    this.conf = conf;
    pendingOffers = new ConcurrentHashMap<>();

    try {
      localhost = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  public SchedulerConf getConf() {
    return conf;
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
      clusterState.getState().setFrameworkId(frameworkId);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    log.info("Registered framework frameworkId=" + frameworkId.getValue());
    reregistered(driver, masterInfo);
  }

  @Override
  public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
    List<TaskStatus> taskList = new ArrayList<TaskStatus>();
    for (TaskID taskId : clusterState.getTasks().keySet()) {
      taskList.add(clusterState.getTasks().get(taskId).taskStatus);
    }

    log.info("Reregistered framework, reconciling " + taskList.size() + " tasks.");

    driver.reconcileTasks(taskList);
    reconciledAt = new DateTime();
  }

  private void launchNode(SchedulerDriver driver, Offer offer, ResourceRoles roles,
      String nodeName, List<String> taskNames) {
    log.info(String.format("Launching node of type %s with tasks %s", nodeName,
        taskNames.toString()));
    int confServerPort = conf.getConfigServerPort();

    String taskIdName = String.format("%s.%d", nodeName, System.currentTimeMillis());
    ExecutorInfo executorInfo = ExecutorInfo
        .newBuilder()
        .setName(nodeName + " executor")
        .setExecutorId(ExecutorID.newBuilder().setValue("executor." + taskIdName).build())
        .addAllResources(
            Arrays.asList(
                Resource.newBuilder().setName("cpus").setType(Value.Type.SCALAR)
                    .setScalar(Value.Scalar.newBuilder().setValue(conf.getExecutorCpus()).build())
                    .setRole(roles.cpuRole).build(),
                Resource
                    .newBuilder()
                    .setName("mem")
                    .setType(Value.Type.SCALAR)
                    .setScalar(
                        Value.Scalar.newBuilder()
                            .setValue(conf.getExecutorHeap() * conf.getJvmOverhead()).build())
                    .setRole(roles.memRole).build(),
                Resource
                    .newBuilder()
                    .setType(Value.Type.RANGES)
                    .setName("ports")
                    .setRole(roles.portsRole)
                    .setRanges(
                        Value.Ranges.newBuilder().addRange(
                            Value.Range.newBuilder().setBegin(roles.portsBegin)
                                .setEnd(roles.portsEnd))).build()))
        .setCommand(
            CommandInfo
                .newBuilder()
                .addAllUris(
                    Arrays.asList(
                        CommandInfo.URI.newBuilder().setValue(conf.getExecUri()).build(),
                        CommandInfo.URI
                            .newBuilder()
                            .setValue(
                                String.format("http://%s:%d/hdfs-site.xml", localhost,
                                    confServerPort)).build()))
                .setEnvironment(
                    Environment.newBuilder().addAllVariables(
                        Arrays.asList(
                            Environment.Variable.newBuilder().setName("HADOOP_OPTS")
                                .setValue(conf.getJvmOpts()).build(),
                            Environment.Variable.newBuilder().setName("HADOOP_HEAPSIZE")
                                .setValue(String.format("%d", conf.getHadoopHeapSize())).build(),
                            Environment.Variable.newBuilder().setName("HADOOP_NAMENODE_OPTS")
                                .setValue("-Xmx" + conf.getNamenodeHeapSize() + "m").build(),
                            Environment.Variable.newBuilder().setName("HADOOP_DATANODE_OPTS")
                                .setValue("-Xmx" + conf.getDatanodeHeapSize() + "m").build(),
                            Environment.Variable.newBuilder().setName("EXECUTOR_OPTS")
                                .setValue("-Xmx" + conf.getExecutorHeap() + "m").build())))
                .setValue("env ; cd hadoop-2.* && bin/mesos-executor").build()).build();

    List<TaskInfo> tasks = new ArrayList<>();
    for (String taskName : taskNames) {
      List<Resource> resources = resourceUtils.buildResources(roles, conf.getTaskCpus(taskName),
          conf.getTaskHeapSize(taskName));

      TaskID taskId = TaskID.newBuilder()
          .setValue(String.format("task.%s.%s", taskName, taskIdName)).build();
      TaskInfo task = TaskInfo.newBuilder().setExecutor(executorInfo).setName(taskName)
          .setTaskId(taskId).setSlaveId(offer.getSlaveId()).addAllResources(resources)
          .setData(ByteString.copyFromUtf8(String.format("bin/hdfs-mesos-%s", taskName))).build();
      tasks.add(task);

      stagingTasks.add(taskId);
      clusterState.addTask(taskId,
          new DfsTask(taskName, offer.getSlaveId().getValue(), offer.getHostname()));
    }

    driver.launchTasks(Arrays.asList(offer.getId()), tasks);
  }

  private void launchNamenode(SchedulerDriver driver, Offer offer, ResourceRoles roles) {
    launchNode(driver, offer, roles, "namenode", Arrays.asList("namenode", "zkfc", "journalnode"));
  }

  private void launchJournalnode(SchedulerDriver driver, Offer offer, ResourceRoles roles) {
    launchNode(driver, offer, roles, "journalnode", Arrays.asList("journalnode"));
  }

  private void launchDatanode(SchedulerDriver driver, Offer offer, ResourceRoles roles) {
    launchNode(driver, offer, roles, "datanode", Arrays.asList("datanode"));
  }

  private void launchInitialJournalnodes(SchedulerDriver driver, Collection<Offer> offers) {
    int journalnodes = 0;
    for (Offer offer : offers) {
      pendingOffers.remove(offer.getId());
      launchJournalnode(driver, offer, resourceUtils.sufficientRolesForJournalnode(offer));
      journalnodes++;
      if (journalnodes >= conf.getJournalnodeCount()) {
        return;
      }
    }
  }

  private void launchInitialNamenodes(SchedulerDriver driver, Collection<Offer> offers) {
    int namenodes = 0;
    // Find offer that matches
    for (Offer offer : offers) {
      ResourceRoles roles = resourceUtils.sufficientRolesForNamenode(offer);
      if (roles != null) {
        pendingOffers.remove(offer.getId());
        launchNamenode(driver, offer, roles);
        namenodes++;
        if (namenodes >= 2) {
          return;
        }
      }
    }
  }

  @Override
  synchronized public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
    log.info(String.format("Received %d offers", offers.size()));
    int seconds = Seconds.secondsBetween(reconciledAt, DateTime.now()).getSeconds();
    if (frameworkInitialized && seconds < conf.getReconciliationStartupDelay()) {
      log.info(String.format("Declining offers pending reconciliation: %ds remaining",
          conf.getReconciliationStartupDelay() - seconds));
      for (Offer offer : offers) {
        driver.declineOffer(offer.getId());
      }
      return;
    }
    if (!stagingTasks.isEmpty()) {
      log.info("Declining offers because tasks are currently staging");
      for (Offer offer : offers) {
        driver.declineOffer(offer.getId());
      }
      return;
    }

    if (clusterState.getNamenodes().size() == 0 && clusterState.getJournalnodes().size() == 0) {
      // Cluster must be formatted! Looks like we're starting fresh?
      log.info("No namenodes or journalnodes found.  Collecting offers until we have sufficient capacity to launch.");

      int namenodes = 0;
      int journalnodes = 0;
      List<Offer> incomingOffers = new ArrayList<>();
      incomingOffers.addAll(offers);
      incomingOffers.addAll(pendingOffers.values());
      for (Offer offer : incomingOffers) {
        if (namenodes < 2 && resourceUtils.sufficientRolesForNamenode(offer) != null
            && clusterState.notInDfsHosts(offer.getSlaveId().getValue())) {
          namenodes++;
          pendingOffers.put(offer.getId(), offer);
        } else if (journalnodes < conf.getJournalnodeCount()
            && resourceUtils.sufficientRolesForJournalnode(offer) != null
            && clusterState.notInDfsHosts(offer.getSlaveId().getValue())) {
          journalnodes++;
          pendingOffers.put(offer.getId(), offer);
        } else {
          driver.declineOffer(offer.getId());
        }
      }
      log.info(String.format(
          "Currently have %d pending offers, with journalnodes=%d and namenodes=%d",
          pendingOffers.size(), journalnodes, namenodes));
      if (!initializingCluster && namenodes == 2 && journalnodes >= conf.getJournalnodeCount()) {
        log.info("Launching initial nodes with pending offers");
        initializingCluster = true;
        launchInitialNamenodes(driver, pendingOffers.values());
        launchInitialJournalnodes(driver, pendingOffers.values());
        // Decline any remaining offer
        for (OfferID offerID : pendingOffers.keySet()) {
          driver.declineOffer(offerID);
        }
        pendingOffers.clear();
      }
      return;
    }

    List<Offer> remainingOffers = new ArrayList<>();

    // We need to start another namenode. Do so now, and temporarily store the
    // remaining offers.
    if (clusterState.getNamenodes().size() < 2 && clusterState.getNamenodeHosts().size() < 2) {
      // Combine pending offers + current offers
      List<Offer> allOffers = new ArrayList<>();
      allOffers.addAll(offers);
      for (Offer offer : allOffers) {
        ResourceRoles roles = resourceUtils.sufficientRolesForNamenode(offer);
        if (roles != null && clusterState.notInDfsHosts(offer.getSlaveId().getValue())) {
          launchNamenode(driver, offer, roles);
          break; // Never start more than 1 at a time to prevent a race
                 // condition.
        } else {
          remainingOffers.add(offer);
        }
      }
    } else {
      remainingOffers.addAll(offers);
    }

    offers = remainingOffers;
    remainingOffers = new ArrayList<>();

    // If we need to launch some journals, do so now
    if (clusterState.getJournalnodes().size() - 2 < conf.getJournalnodeCount()) {
      for (Offer offer : offers) {
        ResourceRoles roles = resourceUtils.sufficientRolesForJournalnode(offer);
        if (roles != null && clusterState.notInDfsHosts(offer.getSlaveId().getValue())) {
          launchJournalnode(driver, offer, roles);
        } else {
          remainingOffers.add(offer);
        }
      }
    } else {
      remainingOffers.addAll(offers);
    }

    if (initializingCluster) {
      log.info(String.format("Declining remaining %d offers pending initialization",
          remainingOffers.size()));
      for (Offer offer : remainingOffers) {
        driver.declineOffer(offer.getId());
      }
      return;
    }

    offers = remainingOffers;
    remainingOffers = new ArrayList<>();

    // Check to see if we can launch some datanodes
    for (Offer offer : offers) {
      ResourceRoles roles = resourceUtils.sufficientRolesForDatanode(offer);
      if (roles != null && clusterState.notInDfsHosts(offer.getSlaveId().getValue())) {
        launchDatanode(driver, offer, roles);
      } else {
        remainingOffers.add(offer);
      }
    }

    // Decline remaining offers
    log.info(String.format("Declining %d offers", remainingOffers.size()));
    for (Offer offer : remainingOffers) {
      driver.declineOffer(offer.getId());
    }
  }

  @Override
  public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {
    log.info("Slave lost slaveId=" + slaveId.getValue());
  }

  private void sendMessageTo(SchedulerDriver driver, TaskID taskId, String message) {
    log.info(String.format("Sending message '%s' to taskId=%s", message, taskId.getValue()));
    DfsTask dfsTask = clusterState.getDfsTask(taskId);
    try {
      String postfix = taskId.getValue();
      postfix = postfix.substring(postfix.indexOf(".") + 1, postfix.length());
      postfix = postfix.substring(postfix.indexOf(".") + 1, postfix.length());
      driver.sendFrameworkMessage(ExecutorID.newBuilder().setValue("executor." + postfix).build(),
          SlaveID.newBuilder().setValue(dfsTask.slaveId).build(), message.getBytes("UTF-8"));
      return;
    } catch (UnsupportedEncodingException e) {
      log.error(e);
      return;
    }
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    // Nice!
    log.info(String.format(
        "Received status update for taskId=%s state=%s message='%s' stagingTasks.size=%d", status
            .getTaskId().getValue(), status.getState().toString(), status.getMessage(),
        stagingTasks.size()));

    DfsTask dfsTask = clusterState.getDfsTask(status.getTaskId());

    if (status.getState().equals(TaskState.TASK_FAILED)
        || status.getState().equals(TaskState.TASK_FINISHED)
        || status.getState().equals(TaskState.TASK_KILLED)
        || status.getState().equals(TaskState.TASK_LOST)) {
      stagingTasks.remove(status.getTaskId());
      clusterState.removeTask(status);
    } else if (status.getState().equals(TaskState.TASK_RUNNING)) {
      stagingTasks.remove(status.getTaskId());
      clusterState.updateTask(status);

      if (dfsTask.type == DfsTask.Type.DN) {
        sendMessageTo(driver, status.getTaskId(), "reload");
        sendMessageTo(driver, status.getTaskId(), "start");
        return;
      }

      if (initializingCluster) {
        log.info("Received status update during cluster initialization");
        if (dfsTask.type == DfsTask.Type.NN && status.getMessage().equals("initialized")) {
          // Time to bootstrap the other namenode.
          for (TaskID taskId : clusterState.getNamenodes()) {
            if (!taskId.equals(status.getTaskId())) {
              log.info("Reloading namenade taskId=" + status.getTaskId().getValue());
              sendMessageTo(driver, status.getTaskId(), "reload");
              sendMessageTo(driver, status.getTaskId(), "start");
              log.info("Bootstrapping namenade taskId=" + taskId.getValue());
              sendMessageTo(driver, taskId, "bootstrap");
              break;
            }
          }
          // Finished!
          initializingCluster = false;
        } else if ((dfsTask.type == DfsTask.Type.JN || dfsTask.type == DfsTask.Type.NN)
            && clusterState.getJournalnodes().size() - 2 == conf.getJournalnodeCount()
            && clusterState.getNamenodes().size() == 2) {
          log.info("All name/journalnodes now ready for initialization");
          // All instances are up. Tell journals to reload, and one namenode to
          // init.
          for (TaskID taskId : clusterState.getJournalnodes()) {
            log.info("Reloading config for taskId=" + taskId.getValue());
            sendMessageTo(driver, taskId, "reload");
            sendMessageTo(driver, taskId, "start_journalnode");
          }
          // Pick a namenode, initialize it.
          Iterator<TaskID> iter = clusterState.getNamenodes().iterator();
          TaskID namenode1 = iter.next();
          sendMessageTo(driver, namenode1, "initialize");
        }
      }
    } else if (dfsTask.type == DfsTask.Type.JN) {
      for (TaskID taskID : clusterState.getNamenodes()) {
        sendMessageTo(driver, taskID, "reload");
      }
      // Pick a namenode, ask it it initialized shared edits
      TaskID namenode = clusterState.getNamenodes().iterator().next();
      sendMessageTo(driver, namenode, "initializeSharedEdits");
    } else if (dfsTask.type == DfsTask.Type.NN) {
      if (status.getMessage().contains("bootstrapped")) {
        sendMessageTo(driver, status.getTaskId(), "start");
      } else {
        for (TaskID task : clusterState.getTasks().keySet()) {
          sendMessageTo(driver, task, "reload");
        }
        sendMessageTo(driver, status.getTaskId(), "start_journalnode");
        sendMessageTo(driver, status.getTaskId(), "bootstrap");
      }
    }
  }

  @Override
  public void run() {
    FrameworkInfo.Builder frameworkInfo = FrameworkInfo.newBuilder()
        .setName("HDFS " + conf.getClusterName()).setFailoverTimeout(conf.getFailoverTimeout())
        .setUser(conf.getHdfsUser()).setRole(conf.getHdfsRole()).setCheckpoint(true);

    try {
      FrameworkID frameworkID = clusterState.getState().getFrameworkID();
      if (frameworkID != null) {
        frameworkInfo.setId(frameworkID);
        frameworkInitialized = true;
      }
    } catch (InterruptedException | ExecutionException | InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }

    MesosSchedulerDriver driver = new MesosSchedulerDriver(this, frameworkInfo.build(),
        conf.getMesosMasterUri());
    driver.run().getValueDescriptor().getFullName();
  }

  public static class DfsTask {
    public final Type type;
    public TaskStatus taskStatus;
    public String slaveId;
    public String hostname;

    public DfsTask(String type, String slaveId, String hostname) {
      switch (type) {
        case "namenode" :
          this.type = Type.NN;
          break;
        case "journalnode" :
          this.type = Type.JN;
          break;
        case "datanode" :
          this.type = Type.DN;
          break;
        case "zkfc" :
          this.type = Type.ZKFC;
          break;
        default :
          throw new RuntimeException("Invalid type: " + type);
      }
      this.slaveId = slaveId;

      // Make sure this is the actual hostname.
      try {
        InetAddress addr = InetAddress.getByName(hostname);
        this.hostname = addr.getHostName();
      } catch (UnknownHostException e) {
        log.error(e);
        this.hostname = hostname;
      }
    }

    public enum Type {
      NN, DN, JN, ZKFC,
    }
  }

}
