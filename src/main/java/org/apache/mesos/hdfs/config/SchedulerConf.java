package org.apache.mesos.hdfs.config;

import com.google.inject.Singleton;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

@Singleton
public class SchedulerConf extends Configured {

  public SchedulerConf(Configuration conf) {
    setConf(conf);
  }

  public SchedulerConf() {
    // The path is configurable via the mesos.conf.path system property
    // so it can be changed when starting up the scheduler via bash
    Properties props = System.getProperties();
    Path configPath = new Path(props.getProperty("mesos.conf.path", "etc/hadoop/mesos-site.xml"));
    Configuration configuration = new Configuration();
    configuration.addResource(configPath);
    setConf(configuration);
  }

  public boolean usingMesosDns() {
    return Boolean.valueOf(getConf().get("mesos.hdfs.mesosdns", "false"));
  }

  public String getMesosDnsDomain() {
    return getConf().get("mesos.hdfs.mesosdns.domain", "mesos");
  }

  public boolean usingPresharedConfig() {
    return Boolean.valueOf(getConf().get("mesos.hdfs.preshared-configuration", "false"));
  }

  public String getExecutorPath() {
    return getConf().get("mesos.hdfs.executor.path", "..");
  }

  public String getConfigPath() {
    return getConf().get("mesos.hdfs.config.path", "etc/hadoop/hdfs-site.xml");
  }

  public int getHadoopHeapSize() {
    return getConf().getInt("mesos.hdfs.hadoop.heap.size", 256);
  }

  public int getDataNodeHeapSize() {
    return getConf().getInt("mesos.hdfs.datanode.heap.size", 1024);
  }

  public int getJournalNodeHeapSize() {
    return getHadoopHeapSize();
  }

  public int getNameNodeHeapSize() {
    return getConf().getInt("mesos.hdfs.namenode.heap.size", 1024);
  }

  public int getExecutorHeap() {
    return getConf().getInt("mesos.hdfs.executor.heap.size", 256);
  }

  public int getZkfcHeapSize() {
    return getHadoopHeapSize();
  }

  public int getTaskHeapSize(String taskName) {
    switch (taskName) {
      case "zkfc" :
        return getZkfcHeapSize();
      case "namenode" :
        return getNameNodeHeapSize();
      case "datanode" :
        return getDataNodeHeapSize();
      case "journalnode" :
        return getJournalNodeHeapSize();
      default :
        throw new RuntimeException("Invalid taskName=" + taskName);
    }
  }

  public double getJvmOverhead() {
    return getConf().getDouble("mesos.hdfs.jvm.overhead", 1.25);
  }

  public String getJvmOpts() {
    return getConf().get(
        "mesos.hdfs.jvm.opts", ""
            + "-XX:+UseConcMarkSweepGC "
            + "-XX:+CMSClassUnloadingEnabled "
            + "-XX:+UseTLAB "
            + "-XX:+AggressiveOpts "
            + "-XX:+UseCompressedOops "
            + "-XX:+UseFastEmptyMethods "
            + "-XX:+UseFastAccessorMethods "
            + "-Xss256k "
            + "-XX:+AlwaysPreTouch "
            + "-XX:+UseParNewGC "
            + "-Djava.library.path=/usr/lib:/usr/local/lib:lib/native");
  }

  public double getExecutorCpus() {
    return getConf().getDouble("mesos.hdfs.executor.cpus", 0.5);
  }

  public double getZkfcCpus() {
    return getExecutorCpus();
  }

  public double getNameNodeCpus() {
    return getConf().getDouble("mesos.hdfs.namenode.cpus", 1);
  }

  public double getJournalNodeCpus() {
    return getConf().getDouble("mesos.hdfs.journalnode.cpus", 1);
  }

  public double getDataNodeCpus() {
    return getConf().getDouble("mesos.hdfs.datanode.cpus", 1);
  }

  public double getTaskCpus(String taskName) {
    switch (taskName) {
      case "zkfc" :
        return getZkfcCpus();
      case "namenode" :
        return getNameNodeCpus();
      case "datanode" :
        return getDataNodeCpus();
      case "journalnode" :
        return getJournalNodeCpus();
      default :
        throw new RuntimeException("Invalid taskName=" + taskName);
    }
  }

  public int getJournalNodeCount() {
    return getConf().getInt("mesos.hdfs.journalnode.count", 3);
  }

  public String getFrameworkName() {
    return getConf().get("mesos.hdfs.framework.name", "hdfs");
  }

  public long getFailoverTimeout() {
    return getConf().getLong("mesos.failover.timeout.sec", 31449600);
  }

  // TODO(elingg) Most likely this user name will change to HDFS
  public String getHdfsUser() {
    return getConf().get("mesos.hdfs.user", "root");
  }

  // TODO(elingg) This role needs to be updated.
  public String getHdfsRole() {
    return getConf().get("mesos.hdfs.role", "*");
  }

  public String getMesosMasterUri() {
    return getConf().get("mesos.master.uri", "zk://localhost:2181/mesos");
  }

  public String getDataDir() {
    return getConf().get("mesos.hdfs.data.dir", "/tmp/hdfs/data");
  }

  public String getSecondaryDataDir() {
    return getConf().get("mesos.hdfs.secondary.data.dir", "/var/run/hadoop-hdfs");
  }

  public String getHaZookeeperQuorum() {
    return getConf().get("mesos.hdfs.zkfc.ha.zookeeper.quorum", "localhost:2181");
  }

  public String getStateZkServers() {
    return getConf().get("mesos.hdfs.state.zk", "localhost:2181");
  }

  public int getStateZkTimeout() {
    return getConf().getInt("mesos.hdfs.state.zk.timeout.ms", 20000);
  }

  public String getNativeLibrary() {
    return getConf().get("mesos.native.library", "/usr/local/lib/libmesos.so");
  }

  public String getFrameworkMountPath() {
    return getConf().get("mesos.hdfs.framework.mnt.path", "/opt/mesosphere");
  }

  public String getFrameworkHostAddress() {
    String hostAddress = getConf().get("mesos.hdfs.framework.hostaddress");
    if (hostAddress == null) {
      try {
        hostAddress = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
    }
    return hostAddress;
  }

  // The port can be changed by setting the PORT0 environment variable
  // See /bin/hdfs-mesos for more details
  public int getConfigServerPort() {
    String configServerPortString = System.getProperty("mesos.hdfs.config.server.port");
    if (configServerPortString == null) {
      configServerPortString = getConf().get("mesos.hdfs.config.server.port", "8765");
    }
    return Integer.valueOf(configServerPortString);
  }

  public int getReconciliationTimeout() {
    return getConf().getInt("mesos.reconciliation.timeout.seconds", 30);
  }

  public int getDeadNodeTimeout() {
    return getConf().getInt("mesos.hdfs.deadnode.timeout.seconds", 90);
  }
}
