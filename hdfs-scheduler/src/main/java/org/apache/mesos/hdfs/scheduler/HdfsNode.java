package org.apache.mesos.hdfs.scheduler;

import com.google.protobuf.ByteString;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.IPersistentStateStore;
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.Environment;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * HdfsNode base class. 
 */
public abstract class HdfsNode implements IOfferEvaluator, ILauncher {
  private final Log log = LogFactory.getLog(HdfsNode.class);
  private final LiveState liveState;
  private final ResourceFactory resourceFactory;

  protected final HdfsFrameworkConfig config;
  protected final IPersistentStateStore persistenceStore;
  protected final String name;


  public HdfsNode(LiveState liveState, IPersistentStateStore persistentStore, HdfsFrameworkConfig config, String name) {
    this.liveState = liveState;
    this.persistenceStore = persistentStore;
    this.config = config;
    this.name = name;
    this.resourceFactory = new ResourceFactory(config.getHdfsRole());
  }

  public String getName() {
    return name;
  }

  protected void launch(
      SchedulerDriver driver,
      Offer offer,
      String nodeName,
      List<String> taskTypes,
      String executorName) {

    List<TaskInfo> tasks = getTasks(driver, offer, nodeName, taskTypes, executorName);
    launchNode(driver, offer, tasks);
  }

  private void launchNode(SchedulerDriver driver, Offer offer, List<TaskInfo> tasks) {
    driver.launchTasks(Arrays.asList(offer.getId()), tasks);
  }

  private List<TaskInfo> getTasks(SchedulerDriver driver, Offer offer,
      String nodeName, List<String> taskTypes, String executorName) {
    // nodeName is the type of executor to launch
    // executorName is to distinguish different types of nodes
    // taskType is the type of task in mesos to launch on the node
    // taskName is a name chosen to identify the task in mesos and mesos-dns (if used)
    log.info(String.format("Launching node of type %s with tasks %s", nodeName, taskTypes.toString()));

    String taskIdName = String.format("%s.%s.%d", nodeName, executorName, System.currentTimeMillis());

    ExecutorInfo executorInfo = createExecutor(taskIdName, nodeName, executorName);

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

      liveState.addStagingTask(taskId);
      persistenceStore.addHdfsNode(taskId, offer.getHostname(), taskType, taskName);
    }

    return tasks;
  }

  private ExecutorInfo createExecutor(String taskIdName, String nodeName, String executorName) {
    int confServerPort = config.getConfigServerPort();

    String cmd = "export JAVA_HOME=$MESOS_DIRECTORY/" + config.getJreVersion()
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
      .addAllResources(getExecutorResources())
      .setCommand(
        CommandInfo
          .newBuilder()
          .addAllUris(
            Arrays.asList(
              CommandInfo.URI
                .newBuilder()
                .setValue(
                  String.format("http://%s:%d/%s", config.getFrameworkHostAddress(),
                    confServerPort,
                    HDFSConstants.HDFS_BINARY_FILE_NAME))
                .build(),
              CommandInfo.URI
                .newBuilder()
                .setValue(
                  String.format("http://%s:%d/%s", config.getFrameworkHostAddress(),
                    confServerPort,
                    HDFSConstants.HDFS_CONFIG_FILE_NAME))
                .build(),
              CommandInfo.URI
                .newBuilder()
                .setValue(config.getJreUrl())
                .build()))
          .setEnvironment(Environment.newBuilder()
            .addAllVariables(Arrays.asList(
              Environment.Variable.newBuilder()
                .setName("LD_LIBRARY_PATH")
                .setValue(config.getLdLibraryPath()).build(),
              Environment.Variable.newBuilder()
                .setName("HADOOP_OPTS")
                .setValue(config.getJvmOpts()).build(),
              Environment.Variable.newBuilder()
                .setName("HADOOP_HEAPSIZE")
                .setValue(String.format("%d", config.getHadoopHeapSize())).build(),
              Environment.Variable.newBuilder()
                .setName("HADOOP_NAMENODE_OPTS")
                .setValue("-Xmx" + config.getNameNodeHeapSize()
                  + "m -Xms" + config.getNameNodeHeapSize() + "m").build(),
              Environment.Variable.newBuilder()
                .setName("HADOOP_DATANODE_OPTS")
                .setValue("-Xmx" + config.getDataNodeHeapSize()
                  + "m -Xms" + config.getDataNodeHeapSize() + "m").build(),
              Environment.Variable.newBuilder()
                .setName("EXECUTOR_OPTS")
                .setValue("-Xmx" + config.getExecutorHeap()
                  + "m -Xms" + config.getExecutorHeap() + "m").build())))
          .setValue(cmd).build())
      .build();
  }

  private List<Resource> getTaskResources(String taskName) {
    double cpu = config.getTaskCpus(taskName);
    double mem = config.getTaskHeapSize(taskName) * config.getJvmOverhead();

    List<Resource> resources = new ArrayList<Resource>();
    resources.add(resourceFactory.createCpuResource(cpu));
    resources.add(resourceFactory.createMemResource(mem));

    return resources;
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
      for (int i = 1; i <= config.getJournalNodeCount(); i++) {
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

  private List<Resource> getExecutorResources() {
    double cpu = config.getExecutorCpus();
    double mem = config.getExecutorHeap() * config.getJvmOverhead();

    return Arrays.asList(
      resourceFactory.createCpuResource(cpu),
      resourceFactory.createMemResource(mem));
  }

  protected boolean offerNotEnoughResources(Offer offer, double cpus, int mem) {
    for (Resource offerResource : offer.getResourcesList()) {
      if (offerResource.getName().equals("cpus") &&
        cpus + config.getExecutorCpus() > offerResource.getScalar().getValue()) {
        return true;
      }

      if (offerResource.getName().equals("mem") &&
        (mem * config.getJvmOverhead())
          + (config.getExecutorHeap() * config.getJvmOverhead())
          > offerResource.getScalar().getValue()) {
        return true;
      }
    }

    return false;
  }
}
