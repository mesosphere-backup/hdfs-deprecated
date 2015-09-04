package org.apache.mesos.hdfs.executor;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.file.FileUtils;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.hdfs.util.StreamRedirect;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * The base for several types of HDFS executors.  It also contains the main which is consistent for all executors.
 */
public abstract class AbstractNodeExecutor implements Executor {

  private final Log log = LogFactory.getLog(AbstractNodeExecutor.class);
  protected ExecutorInfo executorInfo;
  protected HdfsFrameworkConfig hdfsFrameworkConfig;

  /**
   * Constructor which takes in configuration.
   */
  @Inject
  AbstractNodeExecutor(HdfsFrameworkConfig hdfsFrameworkConfig) {
    this.hdfsFrameworkConfig = hdfsFrameworkConfig;
  }

  /**
   * Main method which injects the configuration and state and creates the driver.
   */
  public static void main(String[] args) {
    Injector injector = Guice.createInjector();
    MesosExecutorDriver driver = new MesosExecutorDriver(
      injector.getInstance(AbstractNodeExecutor.class));
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  /**
   * Register the framework with the executor.
   */
  @Override
  public void registered(ExecutorDriver driver, ExecutorInfo executorInfo,
    FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
    // Set up data dir
    setUpDataDir();
    if (!hdfsFrameworkConfig.usingNativeHadoopBinaries()) {
      createSymbolicLink();
    }
    log.info("Executor registered with the slave");
  }

  /**
   * Delete and recreate the data directory.
   */
  private void setUpDataDir() {
    // Create primary data dir if it does not exist
    File dataDir = new File(hdfsFrameworkConfig.getDataDir());
    FileUtils.createDir(dataDir);

    // Create secondary data dir if it does not exist
    File secondaryDataDir = new File(hdfsFrameworkConfig.getSecondaryDataDir());
    FileUtils.createDir(secondaryDataDir);
  }


  /**
   * Create Symbolic Link for the HDFS binary.
   */
  private void createSymbolicLink() {
    log.info("Creating a symbolic link for HDFS binary");
    try {
      // Find Hdfs binary in sandbox
      File sandboxHdfsBinary = new File(System.getProperty("user.dir"));
      Path sandboxHdfsBinaryPath = Paths.get(sandboxHdfsBinary.getAbsolutePath());

      // Create mesosphere opt dir (parent dir of the symbolic link) if it does not exist
      File frameworkMountDir = new File(hdfsFrameworkConfig.getFrameworkMountPath());
      FileUtils.createDir(frameworkMountDir);

      // Delete and recreate directory for symbolic link every time
      String hdfsBinaryPath = hdfsFrameworkConfig.getFrameworkMountPath()
        + "/" + HDFSConstants.HDFS_BINARY_DIR;
      File hdfsBinaryDir = new File(hdfsBinaryPath);

      // Try to delete the symbolic link in case a dangling link is present
      try {
        ProcessBuilder processBuilder = new ProcessBuilder("unlink", hdfsBinaryPath);
        Process process = processBuilder.start();
        redirectProcess(process);
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
      addBinaryToPath(hdfsBinaryPath);
    } catch (IOException | InterruptedException e) {
      String msg = "Error creating the symbolic link to hdfs binary";
      shutdownExecutor(1, msg, e);
    }
  }

  /**
   * Add hdfs binary to the PATH environment variable by linking it to /usr/bin/hadoop. This
   * requires that /usr/bin/ is on the Mesos slave PATH, which is defined as part of the standard
   * Mesos slave packaging.
   */
  private void addBinaryToPath(String hdfsBinaryPath) throws IOException, InterruptedException {
    if (hdfsFrameworkConfig.usingNativeHadoopBinaries()) {
      return;
    }
    String pathEnvVarLocation = "/usr/bin/hadoop";
    String scriptContent = "#!/bin/bash \n" + hdfsBinaryPath + "/bin/hadoop \"$@\"";

    File file = new File(pathEnvVarLocation);
    Writer fileWriter = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
    bufferedWriter.write(scriptContent);
    bufferedWriter.close();
    ProcessBuilder processBuilder = new ProcessBuilder("chmod", "a+x", pathEnvVarLocation);
    Process process = processBuilder.start();
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      String msg = "Error creating the symbolic link to hdfs binary."
        + "Failure running 'chmod a+x " + pathEnvVarLocation + "'";
      shutdownExecutor(1, msg);
    }
  }

  private void shutdownExecutor(int statusCode, String message) {
    shutdownExecutor(statusCode, message, null);
  }

  private void shutdownExecutor(int statusCode, String message, Exception e) {
    if (StringUtils.isNotBlank(message)) {
      log.fatal(message, e);
    }
    System.exit(statusCode);
  }

  /**
   * Starts a task's process so it goes into running state.
   */
  protected void startProcess(ExecutorDriver driver, Task task) {
    log.info(String.format("Starting process: %s", task.getCmd()));
    reloadConfig();
    if (task.getProcess() == null) {
      try {
        ProcessBuilder processBuilder = new ProcessBuilder("sh", "-c", task.getCmd());
        task.setProcess(processBuilder.start());
        redirectProcess(task.getProcess());
      } catch (IOException e) {
        log.error("Unable to start process:", e);
        task.getProcess().destroy();
        sendTaskFailed(driver, task);
      }
    } else {
      log.error("Tried to start process, but process already running");
    }
  }

  /**
   * Reloads the cluster configuration so the executor has the correct configuration info.
   */
  protected void reloadConfig() {
    if (hdfsFrameworkConfig.usingNativeHadoopBinaries()) {
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
      ProcessBuilder processBuilder = new ProcessBuilder("sh", "-c",
        String.format("curl -o hdfs-site.xml %s && mv hdfs-site.xml etc/hadoop/", configUri));
      Process process = processBuilder.start();
      //TODO(nicgrayson) check if the config has changed
      redirectProcess(process);
      int exitCode = process.waitFor();
      if (exitCode == 0) {
        log.info("Finished reloading hdfs-site.xml, exited with status " + exitCode);
      } else {
        log.error("Error reloading hdfs-site.xml.");
      }
    } catch (InterruptedException | IOException e) {
      log.error("Caught exception", e);
    }
  }

  /**
   * Redirects a process to STDERR and STDOUT for logging and debugging purposes.
   */
  protected void redirectProcess(Process process) {
    StreamRedirect stdoutRedirect = new StreamRedirect(process.getInputStream(), System.out);
    stdoutRedirect.start();
    StreamRedirect stderrRedirect = new StreamRedirect(process.getErrorStream(), System.err);
    stderrRedirect.start();
  }

  /**
   * Run a command and wait for it's successful completion.
   */
  protected void runCommand(ExecutorDriver driver, Task task, String command) {
    reloadConfig();
    try {
      log.info(String.format("About to run command: %s", command));
      ProcessBuilder processBuilder = new ProcessBuilder("sh", "-c", command);
      Process init = processBuilder.start();
      redirectProcess(init);
      int exitCode = init.waitFor();
      if (exitCode == 0) {
        log.info("Finished running command, exited with status " + exitCode);
      } else {
        log.error("Unable to run command: " + command);
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

  /**
   * Abstract method to launch a task.
   */
  public abstract void launchTask(final ExecutorDriver driver, final TaskInfo taskInfo);

  /**
   * Let the scheduler know that the task has failed.
   */
  private void sendTaskFailed(ExecutorDriver driver, Task task) {
    driver.sendStatusUpdate(TaskStatus.newBuilder()
      .setTaskId(task.getTaskInfo().getTaskId())
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
    reloadConfig();
    String messageStr = new String(msg, Charset.defaultCharset());
    log.info("Executor received framework message: " + messageStr);
  }

  @Override
  public void error(ExecutorDriver driver, String message) {
    log.error(this.getClass().getName() + ".error: " + message);
  }

}
