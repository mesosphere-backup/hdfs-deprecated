package org.apache.mesos.hdfs.executor;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
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
import org.apache.mesos.hdfs.config.HdfsConfig;
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
 * Abstract executor for HDFS with common services for NameNode and NodeExecutors It also provides
 * the main() for executors
 */
public abstract class AbstractNodeExecutor implements Executor {

  public static final Log LOG = LogFactory.getLog(AbstractNodeExecutor.class);
  private static final String DEFAULT_HADOOP_PATH = "/usr/bin/hadoop";

  protected ExecutorInfo executorInfo;
  protected HdfsConfig hdfsConfig;

  /**
   * Constructor which takes in configuration.
   */
  @Inject
  AbstractNodeExecutor(HdfsConfig hdfsConfig) {
    this.hdfsConfig = hdfsConfig;
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
    createSymbolicLink();
    LOG.info("Executor registered with the slave");
  }

  /**
   * Delete and recreate the data directory.
   */
  private void setUpDataDir() {
    // Create primary data dir if it does not exist
    File dataDir = new File(hdfsConfig.getDataDir());
    createDir(dataDir);

    // Create secondary data dir if it does not exist
    File secondaryDataDir = new File(hdfsConfig.getSecondaryDataDir());
    createDir(secondaryDataDir);
  }

  private void createDir(File dataDir) {
    if (dataDir.exists()) {
      LOG.warn("data dir exits:" + dataDir);
    } else if (!dataDir.mkdirs()) {
      LOG.error("unable to create dir: " + dataDir);
    }
  }

  /**
   * Delete a file or directory.
   **/
  protected void deleteFile(File fileToDelete) {
    if (fileToDelete.isDirectory()) {
      String[] entries = fileToDelete.list();
      for (String entry : entries) {
        File childFile = new File(fileToDelete.getPath(), entry);
        deleteFile(childFile);
      }
    }
    if (!fileToDelete.delete()) {
      LOG.error("unable to delete file: " + fileToDelete);
    }
  }

  /**
   * Create Symbolic Link for the HDFS binary.
   */
  private void createSymbolicLink() {
    LOG.info("Creating a symbolic link for HDFS binary");
    try {
      // Find Hdfs binary in sandbox
      File sandboxHdfsBinary = new File(System.getProperty("user.dir"));
      Path sandboxHdfsBinaryPath = Paths.get(sandboxHdfsBinary.getAbsolutePath());

      // Create mesosphere opt dir (parent dir of the symbolic link) if it does not exist
      File frameworkMountDir = new File(hdfsConfig.getFrameworkMountPath());
      createDir(frameworkMountDir);

      // Delete and recreate directory for symbolic link every time
      String hdfsBinaryPath = hdfsConfig.getFrameworkMountPath()
          + "/" + HDFSConstants.HDFS_BINARY_DIR;
      File hdfsBinaryDir = new File(hdfsBinaryPath);

      // Try to delete the symbolic link in case a dangling link is present
      try {
        Process process = Runtime.getRuntime().exec("unlink " + hdfsBinaryPath);
        redirectProcess(process);
        int exitCode = process.waitFor();
        if (exitCode != 0) {
          log.info("Unable to unlink old sym link. Link may not exist. Exit code: " + exitCode);
        }
      } catch (IOException e) {
        log.fatal("Could not unlink" + hdfsBinaryPath + ": " + e);
        System.exit(1);
      }

      // Delete the file if it exists
      if (hdfsBinaryDir.exists()) {
        deleteFile(hdfsBinaryDir);
      }

      // Create symbolic link
      Path hdfsLinkDirPath = Paths.get(hdfsBinaryPath);
      Files.createSymbolicLink(hdfsLinkDirPath, sandboxHdfsBinaryPath);
      LOG.info("The linked HDFS binary path is: " + sandboxHdfsBinaryPath);
      LOG.info("The symbolic link path is: " + hdfsLinkDirPath);
      // Adding binary to the PATH environment variable
      addBinaryToPath(hdfsBinaryPath);
    } catch (IOException e) {
      LOG.fatal("Error creating the symbolic link to hdfs binary: " + e);
      System.exit(1);
    }
  }

  /**
   * Add hdfs binary to the PATH environment variable by linking it to /usr/bin/hadoop. This
   * requires that /usr/bin/ is on the Mesos slave PATH, which is defined as part of the standard
   * Mesos slave packaging.
   */
  private void addBinaryToPath(String hdfsBinaryPath) throws IOException {
    String pathEnvVarLocation = DEFAULT_HADOOP_PATH;
    String scriptContent = "#!/bin/bash \n" + hdfsBinaryPath + "/bin/hadoop \"$@\"";
    File file = new File(pathEnvVarLocation);
    Writer fileWriter = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
    bufferedWriter.write(scriptContent);
    bufferedWriter.close();
    Runtime.getRuntime().exec("chmod a+x " + pathEnvVarLocation);
  }

  /**
   * Starts a task's process so it goes into running state.
   */
  protected void startProcess(ExecutorDriver driver, Task task) {
    reloadConfig();
    Process process = task.getProcess();
    if (process == null) {
      try {
        process = Runtime.getRuntime().exec(new String[]{
            "sh", "-c", task.getCmd()});
        redirectProcess(process);
      } catch (IOException e) {
        LOG.fatal(e);
        sendTaskFailed(driver, task);
        System.exit(2);
      }
    } else {
      LOG.error("Tried to start process, but process already running");
    }
  }

  /**
   * Reloads the cluster configuration so the executor has the correct configuration info.
   */
  protected void reloadConfig() {
    // Find config URI
    String configUri = "";
    for (CommandInfo.URI uri : executorInfo.getCommand().getUrisList()) {
      if (uri.getValue().contains("hdfs-site.xml")) {
        configUri = uri.getValue();
      }
    }
    if (configUri.isEmpty()) {
      LOG.error("Couldn't find hdfs-site.xml URI");
      return;
    }
    try {
      LOG.info(String.format("Reloading hdfs-site.xml from %s", configUri));
      String cfgCmd[] = new String[]{"sh", "-c",
          String.format("curl -o hdfs-site.xml %s ; cp hdfs-site.xml etc/hadoop/", configUri)};
      Process process = Runtime.getRuntime().exec(cfgCmd);
      //TODO(nicgrayson) check if the config has changed
      redirectProcess(process);
      int exitCode = process.waitFor();
      LOG.info("Finished reloading hdfs-site.xml, exited with status " + exitCode);
    } catch (InterruptedException | IOException e) {
      LOG.error("Caught exception", e);
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
      LOG.info(String.format("About to run command: %s", command));
      Process init = Runtime.getRuntime().exec(new String[]{"sh", "-c", command});
      redirectProcess(init);
      int exitCode = init.waitFor();
      LOG.info("Finished running command, exited with status " + exitCode);
      if (exitCode != 0) {
        LOG.fatal("Unable to run command: " + command);
        sendTaskFailed(driver, task);
        System.exit(1);
      }
    } catch (InterruptedException | IOException e) {
      LOG.fatal(e);
      System.exit(1);
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
    LOG.info("Executor reregistered with the slave");
  }

  @Override
  public void disconnected(ExecutorDriver driver) {
    LOG.info("Executor disconnected from the slave");
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] msg) {
    reloadConfig();
    String messageStr = new String(msg, Charset.defaultCharset());
    LOG.info("Executor received framework message: " + messageStr);
  }

  @Override
  public void error(ExecutorDriver driver, String message) {
    LOG.error(this.getClass().getName() + ".error: " + message);
  }

  @Override
  public void shutdown(ExecutorDriver d) {
    // TODO(elingg) let's shut down the driver more gracefully
    LOG.info("Executor asked to shutdown");
  }
}
