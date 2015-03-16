package org.apache.mesos.hdfs.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.Scheduler;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.state.PersistentState;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

public class DnsResolver {
  public static final Log log = LogFactory.getLog(Scheduler.class);

  private final Scheduler scheduler;
  private final SchedulerConf conf;
  private final PersistentState persistentState;

  public DnsResolver(Scheduler scheduler, SchedulerConf conf, PersistentState persistentState) {
    this.scheduler = scheduler;
    this.conf = conf;
    this.persistentState = persistentState;
  }

  public boolean journalNodesResolvable() {
    if (!conf.usingMesosDns()) return true; //short circuit since Mesos handles this otherwise
    Set<String> hosts = new HashSet<>();
    for (int i = conf.getJournalNodeCount(); i > 0; i--) {
      hosts.add(HDFSConstants.JOURNAL_NODE_ID + i + "." + conf.getFrameworkName() + "." + conf.getMesosDnsDomain());
    }
    boolean success = true;
    for (String host : hosts) {
      log.info("Resolving DNS for " + host);
      try {
        InetAddress.getByName(host);
        log.info("Successfully found " + host);
      } catch (SecurityException | IOException e) {
        log.info("Couldn't resolve host " + host);
        log.info("Sleeping before retrying.");
        success = false;
        break;
      }
    }
    return success;
  }

  public boolean nameNodesResolvable() {
    if (!conf.usingMesosDns()) return true; //short circuit since Mesos handles this otherwise
    Set<String> hosts = new HashSet<>();
    for (int i = HDFSConstants.TOTAL_NAME_NODES; i > 0; i--) {
      hosts.add(HDFSConstants.NAME_NODE_ID + i + "." + conf.getFrameworkName() + "." + conf.getMesosDnsDomain());
    }
    boolean success = true;
    for (String host : hosts) {
      log.info("Resolving DNS for " + host);
      try {
        InetAddress.getByName(host);
        log.info("Successfully found " + host);
      } catch (SecurityException | IOException e) {
        log.info("Couldn't resolve host " + host);
        log.info("Sleeping before retrying.");
        success = false;
        break;
      }
    }
    return success;
  }

  public void sendMessageAfterNNResolvable(final SchedulerDriver driver,
      final Protos.TaskID taskId, final Protos.SlaveID slaveID, final String message) {
    if (!conf.usingMesosDns()) {
      // short circuit since Mesos handles this otherwise
      scheduler.sendMessageTo(driver, taskId, slaveID, message);
      return;
    }
    class PreNNInitTask extends TimerTask {
      @Override
      public void run() {
        if (nameNodesResolvable()) {
          this.cancel();
          scheduler.sendMessageTo(driver, taskId, slaveID, message);
        }
      }
    }
    Timer timer = new Timer();
    PreNNInitTask task = new PreNNInitTask();
    timer.scheduleAtFixedRate(task, 0, 10000);
  }
}
