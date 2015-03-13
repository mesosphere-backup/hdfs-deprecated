package org.apache.mesos.hdfs.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hdfs.Scheduler;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.state.PersistentState;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Abhay Agarwal on 3/12/15.
 */
public class MesosDns {
  public static final Log log = LogFactory.getLog(Scheduler.class);

  public static boolean journalNodesResolvable(SchedulerConf conf, PersistentState persistentState) {
    Set<String> hosts = new HashSet<>();
    if (conf.usingMesosDns()) {
      for (int i = conf.getJournalNodeCount(); i > 0; i--)
        hosts.add(HDFSConstants.JOURNAL_NODE_ID + i + "." + conf.getFrameworkName() + "." + conf.getMesosDnsDomain());
    } else hosts.addAll(persistentState.getJournalNodes().keySet());

    boolean success = true;
    for (String host : hosts) {
      log.info("Resolving DNS for " + host);
      try {
        InetAddress.getByName(host);
        log.info("Successfully found " + host);
      } catch (SecurityException | IOException e) {
        log.info("Couldn't resolve host " + host);
        success = false;
        break;
      }
    }
    return success;
  }

  public static boolean nameNodesResolvable(SchedulerConf conf, PersistentState persistentState) {
    Set<String> hosts = new HashSet<>();
    if (conf.usingMesosDns()) {
      for (int i = HDFSConstants.TOTAL_NAME_NODES; i > 0; i--)
        hosts.add(HDFSConstants.NAME_NODE_ID + i + "." + conf.getFrameworkName() + "." + conf.getMesosDnsDomain());
    } else hosts.addAll(persistentState.getNameNodes().keySet());

    boolean success = true;
    for (String host : hosts) {
      log.info("Resolving DNS for " + host);
      try {
        InetAddress.getByName(host);
        log.info("Successfully found " + host);
      } catch (SecurityException | IOException e) {
        log.info("Couldn't resolve host " + host);
        success = false;
        break;
      }
    }
    return success;
  }
}
