package org.apache.mesos.hdfs.executor;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.*;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.hdfs.util.StreamRedirect;

import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class AbstractNodeExecutor implements Executor {

  public static final Log log = LogFactory.getLog(AbstractNodeExecutor.class);
  protected ExecutorInfo executorInfo;
  protected SchedulerConf schedulerConf;

  /**
   * Constructor which takes in configuration.
   **/
  @Inject
  AbstractNodeExecutor(SchedulerConf schedulerConf) {
    this.schedulerConf = schedulerConf;
  }

  /**
   * Main method which injects the configuration and state and creates the driver.
   **/
  public static void main(String[] args) {
    Injector injector = Guice.createInjector();
    MesosExecutorDriver driver = new MesosExecutorDriver(
        injector.getInstance(AbstractNodeExecutor.class));
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  /**
   * Register the framework with the executor.
   **/
  @Override
  public void registered(ExecutorDriver driver, ExecutorInfo executorInfo,
      FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
    executorInfo = executorInfo;
    // Set up data dir
    setUpDataDir();
    createSymbolicLink();
    log.info("Executor registered with the slave");
  }

  /**
   * Delete and recreate the data directory.
   **/
  private void setUpDataDir() {
    // Create primary data dir if it does not exist
    File dataDir = new File(schedulerConf.getDataDir());
    // TODO(elingg) Need to actually recover the data instead of getting rid of it.
    if (dataDir.exists()) {
      deleteFile(dataDir);
    }
    dataDir.mkdirs();

    // Create secondary data dir if it does not exist
    File secondaryDataDir = new File(schedulerConf.getSecondaryDataDir());
    if (!secondaryDataDir.exists()) {
      secondaryDataDir.mkdirs();
    }
  }

  /**
   * Delete a file or directory.
   **/
  private void deleteFile(File fileToDelete) {
    if (fileToDelete.isDirectory()) {
      String[] entries = fileToDelete.list();
      for (String entry : entries) {
        File childFile = new File(fileToDelete.getPath(), entry);
        deleteFile(childFile);
      }
    }
    fileToDelete.delete();
  }

  /**
   * Create Symbolic Link for the HDFS binary.
   **/
  private void createSymbolicLink() {
    log.info("Creating a symbolic link for HDFS binary");
    try {
      // Find Hdfs binary in sandbox
      File sandboxHdfsBinary = new File(System.getProperty("user.dir"));
      Path sandboxHdfsBinaryPath = Paths.get(sandboxHdfsBinary.getAbsolutePath());

      // Create mesosphere opt dir (parent dir of the symbolic link) if it does not exist
      File frameworkMountDir = new File(schedulerConf.getFrameworkMountPath());
      if (!frameworkMountDir.exists()) {
        frameworkMountDir.mkdirs();
      }

      // Delete and recreate directory for symbolic link every time
      String hdfsBinaryPath = schedulerConf.getFrameworkMountPath()
          + "/" + HDFSConstants.HDFS_BINARY_DIR;
      File hdfsBinaryDir = new File(hdfsBinaryPath);
      if (hdfsBinaryDir.exists()) {
        deleteFile(hdfsBinaryDir);
      }

      // Create symbolic link
      Path hdfsLinkDirPath = Paths.get(hdfsBinaryPath);
      Files.createSymbolicLink(hdfsLinkDirPath, sandboxHdfsBinaryPath);
      log.info("The linked HDFS binary path is: " + sandboxHdfsBinaryPath);
      log.info("The symbolic link path is: " + hdfsLinkDirPath);
      // Adding binary to the PATH environment variable
      addBinaryToPath(hdfsBinaryPath);
    } catch (Exception e) {
      log.fatal("Error creating the symbolic link to hdfs binary: " + e);
      System.exit(1);
    }
  }

  /**
   * Add hdfs binary to the PATH environment variable by linking it to /usr/bin/hadoop. This
   * requires that /usr/bin/ is on the Mesos slave PATH, which is defined as part of the standard
   * Mesos slave packaging.
   **/
  private void addBinaryToPath(String hdfsBinaryPath) throws IOException {
    String pathEnvVarLocation = "/usr/bin/hadoop";
    String scriptContent = "#!/bin/bash \n" + hdfsBinaryPath + "/bin/hadoop \"$@\"";
    FileWriter fileWriter = new FileWriter(pathEnvVarLocation);
    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
    bufferedWriter.write(scriptContent);
    bufferedWriter.close();
    Runtime.getRuntime().exec("chmod a+x " + pathEnvVarLocation);
  }
  /**
   * Starts a task's process so it goes into running state.
   **/
  protected void startProcess(ExecutorDriver driver, Task task) {
    reloadConfig();
    Process process = task.process;
    if (process == null) {
      try {
        process = Runtime.getRuntime().exec(new String[]{
            "sh", "-c", task.cmd});
        redirectProcess(process);
      } catch (IOException e) {
        log.fatal(e);
        sendTaskFailed(driver, task);
        System.exit(2);
      }
    } else {
      log.error("Tried to start process, but process already running");
    }
  }

  /**
   * Reloads the cluster configuration so the executor has the correct configuration info.
   **/
  protected void reloadConfig() {
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
      String cfgCmd[] = new String[]{"sh", "-c",
          String.format("curl -o hdfs-site.xml %s ; cp hdfs-site.xml etc/hadoop/", configUri)};
      Process process = Runtime.getRuntime().exec(cfgCmd);
      //TODO(nicgrayson) check if the config has changed
      redirectProcess(process);
      int exitCode = process.waitFor();
      log.info("Finished reloading hdfs-site.xml, exited with status " + exitCode);
    } catch (InterruptedException | IOException e) {
      log.error("Caught exception", e);
    }
  }

  /**
   * Redirects a process to STDERR and STDOUT for logging and debugging purposes.
   **/
  protected void redirectProcess(Process process) {
    StreamRedirect stdoutRedirect = new StreamRedirect(process.getInputStream(), System.out);
    stdoutRedirect.start();
    StreamRedirect stderrRedirect = new StreamRedirect(process.getErrorStream(), System.err);
    stderrRedirect.start();
  }

  /**
   * Run a command and wait for it's successful completion.
   **/
  protected void runCommand(ExecutorDriver driver, Task task, String command) {
    reloadConfig();
    try {
      log.info(String.format("About to run command: %s", command));
      Process init = Runtime.getRuntime().exec(new String[]{"sh", "-c", command});
      redirectProcess(init);
      int exitCode = init.waitFor();
      log.info("Finished running command, exited with status " + exitCode);
      if (exitCode != 0) {
        log.fatal("Unable to run command: " + command);
        sendTaskFailed(driver, task);
        System.exit(1);
      }
    } catch (InterruptedException | IOException e) {
      log.fatal(e);
      System.exit(1);
    }
  }
  /**
   * Abstract method to launch a task.
   **/
  public abstract void launchTask(final ExecutorDriver driver, final TaskInfo taskInfo);

  /**
   * Let the scheduler know that the task has failed.
   **/
  private void sendTaskFailed(ExecutorDriver driver, Task task) {
    driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(task.taskInfo.getTaskId())
        .setState(TaskState.TASK_FAILED)
        .build());
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
    log.info("Executor received framework message of length: " + msg.length + " bytes");
  }

  @Override
  public void error(ExecutorDriver driver, String message) {
    log.error(this.getClass().getName() + ".error: " + message);
  }

  @Override
  public void shutdown(ExecutorDriver d) {
    // TODO(elingg) let's shut down the driver more gracefully
    log.info("Executor asked to shutdown");
  }

  /**
   * The task class for use within the executor
   **/
  public class Task {
    public TaskInfo taskInfo;
    public String cmd;
    public Process process;

    Task(TaskInfo taskInfo) {
      this.taskInfo = taskInfo;
      this.cmd = taskInfo.getData().toStringUtf8();
      log.info(String.format("Launching task, taskId=%s cmd='%s'", taskInfo.getTaskId().getValue(),
          cmd));
    }
  }

}
