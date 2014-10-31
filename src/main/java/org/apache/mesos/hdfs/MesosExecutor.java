package org.apache.mesos.hdfs;

import com.google.common.util.concurrent.RateLimiter;
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
import org.apache.mesos.hdfs.util.StreamRedirect;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MesosExecutor implements Executor {
  public static final Log log = LogFactory.getLog(MesosExecutor.class);
  private Map<TaskID, Task> tasks = new ConcurrentHashMap<>();
  private ExecutorInfo executorInfo;
  private RateLimiter reloadLimiter = RateLimiter.create(1 / 60.); // no more
                                                                   // than once
                                                                   // every 60s
  private BackupService backupService;
  private SchedulerConf schedulerConf;

  @Inject
  MesosExecutor(SchedulerConf schedulerConf, BackupService backupService) {
    this.backupService = backupService;
    this.schedulerConf = schedulerConf;
  }

  public static void main(String[] args) {
    Injector injector = Guice.createInjector(new ProdConfigModule());

    MesosExecutorDriver driver = new MesosExecutorDriver(injector.getInstance(MesosExecutor.class));
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  @Override
  public void registered(ExecutorDriver driver, ExecutorInfo executorInfo,
      FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
    log.info("Executor registered with the slave");

    // Make sure data dir exists
    File dataDir = new File(schedulerConf.getDataDir());
    if (!dataDir.exists()) {
      dataDir.mkdirs();
    }

  }

  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
    executorInfo = task.getExecutor();
    tasks.put(task.getTaskId(), new Task(task));

    driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId())
        .setState(TaskState.TASK_RUNNING).build());

    // Check if we have a namenode.
    // If so, report back to scheduler whether or not there's a backup archive
    // available.
    if (task.getTaskId().getValue().contains("namenode.namenode")) {
      if (backupService.checkForBackup()) {
        driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.getTaskId())
            .setState(TaskState.TASK_RUNNING).setMessage("backup-archive-available").build());
      }
    }
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {
    log.info("Killing task : " + taskId.getValue());
    if (tasks.get(taskId).process != null) {
      tasks.get(taskId).process.destroy();
      tasks.get(taskId).process = null;
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

  private void sendTaskFailed(ExecutorDriver driver, Task task) {
    driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.taskInfo.getTaskId())
        .setState(TaskState.TASK_FAILED).build());
  }

  private void startProcesses(ExecutorDriver driver) {
    for (Task task : tasks.values()) {
      if (task.process == null) {
        startProcess(driver, task);
      }
    }
  }

  private void startProcess(ExecutorDriver driver, Task task) {
    Process process = task.process;
    if (process == null) {
      try {
        process = Runtime.getRuntime().exec(new String[]{"sh", "-c", task.cmd});
        redirectProcess(process);
      } catch (IOException e) {
        log.fatal(e);
        sendTaskFailed(driver, task);
        System.exit(2);
      }
    } else {
      log.error("Received 'start' command, but process already running");
    }
  }

  private void reloadConfig() {
    if (!reloadLimiter.tryAcquire()) {
      log.info("Limiting reload rate");
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
      String cfgCmd[] = new String[]{"sh", "-c",
          String.format("curl -o hdfs-site.xml %s ; cp hdfs-site.xml etc/hadoop/", configUri)};
      Process process = Runtime.getRuntime().exec(cfgCmd);
      redirectProcess(process);
      int exitCode = process.waitFor();
      log.info("Finished reloading hdfs-site.xml, exited with status " + exitCode);
    } catch (InterruptedException | IOException e) {
      log.error("Caught exception", e);
    }
  }

  private void redirectProcess(Process process) {
    StreamRedirect stdoutRedirect = new StreamRedirect(process.getInputStream(), System.out);
    stdoutRedirect.start();
    StreamRedirect stderrRedirect = new StreamRedirect(process.getErrorStream(), System.err);
    stderrRedirect.start();
  }

  private void namenodeInit(ExecutorDriver driver, String opt, String message) {
    // Find the namenode task
    Task task = null;
    for (TaskID taskId : tasks.keySet()) {
      if (taskId.getValue().contains("namenode.namenode")) {
        task = tasks.get(taskId);
      }
    }
    try {
      reloadConfig();
      String fullCmd = task.cmd + " " + opt;
      log.info(String.format("About to run command: %s", fullCmd));
      Process init = Runtime.getRuntime().exec(new String[]{"sh", "-c", fullCmd});
      redirectProcess(init);
      int exitCode = init.waitFor();
      log.info("Finished running command, exited with status " + exitCode);
      if (exitCode != 0) {
        log.fatal("Unable to initialize");
        sendTaskFailed(driver, task);
        System.exit(1);
      } else {
        startProcess(driver, task);
        driver.sendStatusUpdate(TaskStatus.newBuilder().setTaskId(task.taskInfo.getTaskId())
            .setState(TaskState.TASK_RUNNING).setMessage(message).build());
      }
    } catch (InterruptedException | IOException e) {
      log.fatal(e);
      sendTaskFailed(driver, task);
      System.exit(1);
    }
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] msg) {
    log.info("Executor received framework message of length: " + msg.length + " bytes");
    String command;
    try {
      command = new String(msg, "UTF-8");

      switch (command) {
        case "start" :
          log.info("Starting all processes");
          startProcesses(driver);
          break;
        case "start_journalnode" :
          log.info("Starting journalnode");
          for (TaskID taskId : tasks.keySet()) {
            if (taskId.getValue().contains(".journalnode.")) {
              startProcess(driver, tasks.get(taskId));
              break;
            }
          }
          break;
        case "reload" :
          log.info("Asked to reload config");
          reloadConfig();
          for (Task task : tasks.values()) {
            if (task.process != null) {
              task.process.destroy();
              task.process = null;
              startProcess(driver, task);
            }
          }
          break;
        case "initialize" :
          log.info("Asked to initialize");
          namenodeInit(driver, "-i", "initialized");
          break;
        case "bootstrap" :
          log.info("Asked to bootstrap");
          namenodeInit(driver, "-b", "bootstrapped");
          break;
        case "initializeSharedEdits" :
          log.info("Asked to initializeSharedEdits");
          namenodeInit(driver, "-s", "initialized");
          break;
        case "backup" :
          log.info("Asked to perform backup");
          backupService.createAndStoreArchive();
        default :
          throw new RuntimeException("Unknown command: " + command);
      }
    } catch (UnsupportedEncodingException e) {
      log.error("Caught exception", e);
    }
  }

  @Override
  public void error(ExecutorDriver driver, String message) {
    log.error("MesosExecutor.error: " + message);
  }

  @Override
  public void shutdown(ExecutorDriver d) {
    log.info("Executor asked to shutdown");
  }

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
