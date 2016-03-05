package org.apache.mesos.hdfs.executor;

import com.google.inject.Inject;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.utils.URIBuilder;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.file.FileUtils;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.config.NodeConfig;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.process.FailureUtils;
import org.apache.mesos.process.ProcessUtil;
import org.apache.mesos.process.ProcessWatcher;
import org.apache.mesos.protobuf.TaskStatusBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * The base for several types of HDFS executors.  It also contains the main which is consistent for all executors.
 */
public abstract class AbstractNodeExecutor implements Executor {

  private final Log log = LogFactory.getLog(AbstractNodeExecutor.class);
  protected ExecutorInfo executorInfo;
  protected HdfsFrameworkConfig config;
  private ProcessWatcher procWatcher;

  // Timed Health Check for node health monitoring
  protected Timer healthCheckTimer;
  private NodeHealthChecker nodeHealthChecker;

  /**
   * Constructor which takes in configuration.
   */
  @Inject
  AbstractNodeExecutor(HdfsFrameworkConfig config) {
    this.config = config;
    this.procWatcher = new ProcessWatcher(new HdfsProcessExitHandler());
    healthCheckTimer = new Timer(true);
  }

  /**
   * Register the framework with the executor.
   */
  @Override
  public void registered(ExecutorDriver driver, ExecutorInfo executorInfo,
    FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
    // Set up data dir
    setUpDataDir();
    setUpDomainSocketDir();
    if (!config.usingNativeHadoopBinaries()) {
      createSymbolicLink(driver);
    }
    log.info("Executor registered with the slave");
  }

  /**
   * Delete and recreate the data directory.
   */
  private void setUpDataDir() {
    // Create primary data dir if it does not exist
    File dataDir = new File(config.getDataDir());
    FileUtils.createDir(dataDir);

    // Create secondary data dir if it does not exist
    if (config.getSecondaryDataDir() != null) {
      File secondaryDataDir = new File(config.getSecondaryDataDir());
      FileUtils.createDir(secondaryDataDir);
    }
  }

  /**
   * Delete and recreate domain socket directory.
   */
  private void setUpDomainSocketDir() {
    // Create domain socket dir if it does not exist
    File domainSocketDir = new File(config.getDomainSocketDir());
    FileUtils.createDir(domainSocketDir);
  }


  /**
   * Create Symbolic Link for the HDFS binary.
   */
  private void createSymbolicLink(ExecutorDriver driver) {
    // todo:  (kgs) https://mesosphere.atlassian.net/browse/HDFS-172
    log.info("Creating a symbolic link for HDFS binary");
    try {
      // Find Hdfs binary in sandbox
      File sandboxHdfsBinary = new File(System.getProperty("user.dir"));
      Path sandboxHdfsBinaryPath = Paths.get(sandboxHdfsBinary.getAbsolutePath());

      // Create mesosphere opt dir (parent dir of the symbolic link) if it does not exist
      File frameworkMountDir = new File(config.getFrameworkMountPath());
      FileUtils.createDir(frameworkMountDir);

      // Delete and recreate directory for symbolic link every time
      String hdfsBinaryPath = config.getFrameworkMountPath()
        + "/" + HDFSConstants.HDFS_BINARY_DIR;
      File hdfsBinaryDir = new File(hdfsBinaryPath);

      // Try to delete the symbolic link in case a dangling link is present
      try {
        Process process = ProcessUtil.startCmd("unlink", hdfsBinaryPath);
        int exitCode = process.waitFor();
        if (exitCode != 0) {
          log.info("Unable to unlink old sym link. Link may not exist. Exit code: " + exitCode);
        }
      } catch (IOException e) {
        log.fatal("Could not unlink " + hdfsBinaryPath, e);
        throw e;
      }

      // Delete the file if it exists
      if (hdfsBinaryDir.exists() && !FileUtils.deleteDirectory(hdfsBinaryDir)) {
        String msg = "Unable to delete file: " + hdfsBinaryDir;
        log.error(msg);
        throw new ExecutorException(msg);
      }

      // Create symbolic link
      Path hdfsLinkDirPath = Paths.get(hdfsBinaryPath);
      Files.createSymbolicLink(hdfsLinkDirPath, sandboxHdfsBinaryPath);
      log.info("The linked HDFS binary path is: " + sandboxHdfsBinaryPath);
      log.info("The symbolic link path is: " + hdfsLinkDirPath);
      // Adding binary to the PATH environment variable
      addBinaryToPath(driver, hdfsBinaryPath);
    } catch (IOException | InterruptedException e) {
      String msg = "Error creating the symbolic link to hdfs binary";
      shutdownExecutor(driver, 1, msg, e);
    }
  }

  /**
   * Add hdfs binary to the PATH environment variable by linking it to /usr/bin/hadoop. This
   * requires that /usr/bin/ is on the Mesos slave PATH, which is defined as part of the standard
   * Mesos slave packaging.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value = "DMI_HARDCODED_ABSOLUTE_FILENAME",
    justification = "hadoop is required to be in this location")
  private void addBinaryToPath(ExecutorDriver driver, String hdfsBinaryPath)
    throws IOException, InterruptedException {
    if (config.usingNativeHadoopBinaries()) {
      return;
    }
    String pathEnvVarLocation = "/usr/bin/hadoop";
    String scriptContent = "#!/bin/bash \n" + hdfsBinaryPath + "/bin/hadoop \"$@\"";

    File file = new File(pathEnvVarLocation);
    Writer fileWriter = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
    bufferedWriter.write(scriptContent);
    bufferedWriter.close();
    Process process = ProcessUtil.startCmd("chmod", "a+x", pathEnvVarLocation);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      String msg = "Error creating the symbolic link to hdfs binary."
        + "Failure running 'chmod a+x " + pathEnvVarLocation + "'";
      shutdownExecutor(driver, 1, msg);
    }
  }

  private void shutdownExecutor(ExecutorDriver driver, int statusCode, String message) {
    shutdownExecutor(driver, statusCode, message, null);
  }

  private void shutdownExecutor(ExecutorDriver driver, int statusCode, String message, Exception e) {
    if (StringUtils.isNotBlank(message)) {
      log.fatal(message, e);
    }
    FailureUtils.exit(message, statusCode);
  }

  /**
   * Starts a task's process so it goes into running state.
   */
  protected Process startProcess(ExecutorDriver driver, Task task) {
    log.info(String.format("Starting process: %s", task.getCmd()));
    Process proc = task.getProcess();
    reloadConfig(null);
    if (proc == null) {
      try {
        Map<String, String> envMap = createHdfsNodeEnvironment(task);

        Process process = ProcessUtil.startCmd(envMap, task.getCmd());
        procWatcher.watch(process);
        task.setProcess(process);
      } catch (IOException e) {
        log.error("Unable to start process:", e);
        task.getProcess().destroy();
        sendTaskFailed(driver, task);
      }
    } else {
      log.error("Tried to start process, but process already running");
    }

    return proc;
  }

  private Map<String, String> createHdfsNodeEnvironment(Task task) {
    Map<String, String> envMap = new HashMap<>();
    NodeConfig nodeConfig = config.getNodeConfig(task.getType());

    envMap.put("HADOOP_HEAPSIZE", String.format("%d", nodeConfig.getMaxHeap()));
    envMap.put("HADOOP_OPTS", config.getJvmOpts());
    envMap.put("HADOOP_NAMENODE_OPTS",
      "-Xmx" + config.getNodeConfig(HDFSConstants.NAME_NODE_ID).getMaxHeap() + "m -Xms" +
        config.getNodeConfig(HDFSConstants.NAME_NODE_ID).getMaxHeap() + "m");
    envMap.put("HADOOP_DATANODE_OPTS",
      "-Xmx" + config.getNodeConfig(HDFSConstants.DATA_NODE_ID).getMaxHeap() + "m -Xms" +
        config.getNodeConfig(HDFSConstants.DATA_NODE_ID).getMaxHeap() + "m");

    return envMap;
  }

  /**
   * Reloads the cluster configuration so the executor has the correct configuration info.
   */
  protected void reloadConfig(String message) {
    if (config.usingNativeHadoopBinaries()) {
      return;
    }
    // Find config URI
    String configUri = "";
    for (CommandInfo.URI uri : executorInfo.getCommand().getUrisList()) {
      if (uri.getValue().contains("hdfs-site.xml")) {
        configUri = uri.getValue();
      }
    }
    if (configUri.isEmpty()) {
      log.error("Couldn't find hdfs-site.xml URI");
      return;
    }
    try {
      log.info(String.format("Reloading hdfs-site.xml from %s", configUri));
      String updatedConfigUri = configUri;
      if (message != null && message.startsWith(HDFSConstants.RELOAD_CONFIG)){
        String[] hostPort = message.replaceAll(HDFSConstants.RELOAD_CONFIG,"").trim().split(HDFSConstants.HOST_PORT_SEPARATOR);
        URI uri = new URIBuilder(configUri).setHost(hostPort[0]).setPort(new Integer(hostPort[1])).build();
        updatedConfigUri = uri.toString();
      }
      Process process = ProcessUtil.startCmd(
        String.format("curl -o hdfs-site.xml %s && mv hdfs-site.xml etc/hadoop/", updatedConfigUri));
      //TODO(nicgrayson) check if the config has changed
      int exitCode = process.waitFor();
      if (exitCode == 0) {
        log.info("Finished reloading hdfs-site.xml, exited with status " + exitCode);
      } else {
        log.error("Error reloading hdfs-site.xml.");
      }
    } catch (InterruptedException | IOException | URISyntaxException e) {
      log.error("Caught exception", e);
    }
  }

  /**
   * Run a command and wait for it's successful completion.
   */
  protected void runCommand(ExecutorDriver driver, Task task, String command) {
    reloadConfig(null);
    try {
      log.info(String.format("About to run command: %s", command));
      Process init = ProcessUtil.startCmd(command);
      int exitCode = init.waitFor();
      if (exitCode == 0) {
        log.info("Finished running command, exited with status " + exitCode);
      } else {
        log.error("Unable to run command, exit code:" + exitCode);
        if (task.getProcess() != null) {
          task.getProcess().destroy();
        }
        sendTaskFailed(driver, task);
      }
    } catch (InterruptedException | IOException e) {
      log.error("Unable to run command:", e);
      if (task.getProcess() != null) {
        task.getProcess().destroy();
      }
      sendTaskFailed(driver, task);
    }
  }

  @Override
  public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {
    log.info("Executor reregistered with the slave");
  }

  @Override
  public void disconnected(ExecutorDriver driver) {
    log.info("Executor disconnected from the slave");
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] msg) {
    String messageStr = new String(msg, Charset.defaultCharset());
    log.info("Executor received framework message: " + messageStr);
    reloadConfig(messageStr);
  }

  @Override
  public void error(ExecutorDriver driver, String message) {
    log.error(this.getClass().getName() + ".error: " + message);
  }

  private void launchHealthCheck(ExecutorDriver driver, Task task) {
    String taskIdStr = task.getTaskInfo().getTaskId().getValue();
    log.info("Performing health check for task: " + taskIdStr);

    boolean taskHealthy = nodeHealthChecker.runHealthCheckForTask(task);
    if (!taskHealthy) {
      log.fatal("Node health check failed for task: " + taskIdStr);
      killTask(driver, task.getTaskInfo().getTaskId());
      // TODO (elingg) with better process supervision
      // (i.e. monitoring of ZKFC's), we do not need to exit the executors
      shutdownExecutor(driver, 2, "Failed health check");
    }
  }

  /**
   * Abstract method to launch a task.
   */
  public abstract void launchTask(final ExecutorDriver driver, final TaskInfo taskInfo);

  /**
   * Let the scheduler know that the task has failed.
   */
  private void sendTaskFailed(ExecutorDriver driver, Task task) {
    driver.sendStatusUpdate(
      new TaskStatusBuilder()
        .setTaskId(task.getTaskInfo().getTaskId())
        .setState(TaskState.TASK_FAILED)
        .build()
    );
  }

  /**
   * Implementation of a TimedHealthCheck through use of TimerTask.
   */
  protected class TimedHealthCheck extends TimerTask {
    Task task;
    ExecutorDriver driver;

    public TimedHealthCheck(ExecutorDriver driver, Task task) {
      this.driver = driver;
      this.task = task;
    }

    @Override
    public void run() {
      launchHealthCheck(driver, task);
    }
  }
}
