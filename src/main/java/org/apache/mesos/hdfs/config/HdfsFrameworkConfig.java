package org.apache.mesos.hdfs.config;

import com.google.inject.Singleton;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * Provides executor configurations for launching processes at the slave leveraging hadoop
 * configurations.
 */
@Singleton
public class HdfsFrameworkConfig {

  private Configuration hadoopConfig;

  private static final int DEFAULT_HEAP_SIZE = 256;
  private static final int DEFAULT_HADOOP_HEAP_SIZE = DEFAULT_HEAP_SIZE;
  private static final int DEFAULT_EXECUTOR_HEAP_SIZE = DEFAULT_HEAP_SIZE;
  private static final int DEFAULT_DATANODE_HEAP_SIZE = 1024;
  private static final int DEFAULT_NAMENODE_HEAP_SIZE = 1024;

  private static final double DEFAULT_CPUS = 0.5;
  private static final double DEFAULT_EXECUTOR_CPUS = DEFAULT_CPUS;
  private static final double DEFAULT_NAMENODE_CPUS = 1;
  private static final double DEFAULT_JOURNAL_CPUS = 1;
  private static final double DEFAULT_DATANODE_CPUS = 1;

  private static final double DEFAULT_JVM_OVERHEAD = 1.25;
  private static final int DEFAULT_JOURNAL_NODE_COUNT = 3;
  private static final int DEFAULT_FAILOVER_TIMEOUT = 31449600;
  private static final int DEFAULT_ZK_TIME_MS = 20000;
  private static final int DEFAULT_RECONCILIATION_TIMEOUT = 30;

  public final Log log = LogFactory.getLog(HdfsFrameworkConfig.class);

  public HdfsFrameworkConfig(Configuration conf) {
    setConf(conf);
  }

  private void setConf(Configuration conf) {
    this.hadoopConfig = conf;
  }

  private Configuration getConf() {
    return hadoopConfig;
  }

  public HdfsFrameworkConfig() {
    // The path is configurable via the mesos.conf.path system property
    // so it can be changed when starting up the scheduler via bash
    Properties props = System.getProperties();
    Path configPath = new Path(props.getProperty("mesos.conf.path", "etc/hadoop/mesos-site.xml"));
    Configuration configuration = new Configuration();
    configuration.addResource(configPath);
    setConf(configuration);
  }

  public String getExecutorPath() {
    return getConf().get("mesos.hdfs.executor.path", "..");
  }

  public String getConfigPath() {
    return getConf().get("mesos.hdfs.config.path", "etc/hadoop/hdfs-site.xml");
  }

  public int getHadoopHeapSize() {
    return getConf().getInt("mesos.hdfs.hadoop.heap.size", DEFAULT_HADOOP_HEAP_SIZE);
  }

  public int getDataNodeHeapSize() {
    return getConf().getInt("mesos.hdfs.datanode.heap.size", DEFAULT_DATANODE_HEAP_SIZE);
  }

  public int getJournalNodeHeapSize() {
    return getHadoopHeapSize();
  }

  public int getNameNodeHeapSize() {
    return getConf().getInt("mesos.hdfs.namenode.heap.size", DEFAULT_NAMENODE_HEAP_SIZE);
  }

  public int getExecutorHeap() {
    return getConf().getInt("mesos.hdfs.executor.heap.size", DEFAULT_EXECUTOR_HEAP_SIZE);
  }

  public int getZkfcHeapSize() {
    return getHadoopHeapSize();
  }

  public int getTaskHeapSize(String taskName) {
    int size = DEFAULT_HEAP_SIZE;
    switch (taskName) {
      case HDFSConstants.ZKFC_NODE_ID:
        size = getZkfcHeapSize();
        break;
      case HDFSConstants.NAME_NODE_ID:
        size = getNameNodeHeapSize();
        break;
      case HDFSConstants.DATA_NODE_ID:
        size = getDataNodeHeapSize();
        break;
      case HDFSConstants.JOURNAL_NODE_ID:
        size = getJournalNodeHeapSize();
        break;
      default:
        final String msg = "Invalid request for heapsize for taskName = " + taskName;
        log.error(msg);
        throw new ConfigurationException(msg);
    }
    return size;
  }

  public double getJvmOverhead() {
    return getConf().getDouble("mesos.hdfs.jvm.overhead", DEFAULT_JVM_OVERHEAD);
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
    return getConf().getDouble("mesos.hdfs.executor.cpus", DEFAULT_EXECUTOR_CPUS);
  }

  public double getZkfcCpus() {
    return getExecutorCpus();
  }

  public double getNameNodeCpus() {
    return getConf().getDouble("mesos.hdfs.namenode.cpus", DEFAULT_NAMENODE_CPUS);
  }

  public double getJournalNodeCpus() {
    return getConf().getDouble("mesos.hdfs.journalnode.cpus", DEFAULT_JOURNAL_CPUS);
  }

  public double getDataNodeCpus() {
    return getConf().getDouble("mesos.hdfs.datanode.cpus", DEFAULT_DATANODE_CPUS);
  }

  public double getTaskCpus(String taskName) {
    double cpus = DEFAULT_CPUS;
    switch (taskName) {
      case HDFSConstants.ZKFC_NODE_ID:
        cpus = getZkfcCpus();
        break;
      case HDFSConstants.NAME_NODE_ID:
        cpus = getNameNodeCpus();
        break;
      case HDFSConstants.DATA_NODE_ID:
        cpus = getDataNodeCpus();
        break;
      case HDFSConstants.JOURNAL_NODE_ID:
        cpus = getJournalNodeCpus();
        break;
      default:
        final String msg = "Invalid request for CPUs for taskName= " + taskName;
        log.error(msg);
        throw new ConfigurationException(msg);
    }
    return cpus;
  }

  public int getJournalNodeCount() {
    return getConf().getInt("mesos.hdfs.journalnode.count", DEFAULT_JOURNAL_NODE_COUNT);
  }

  public String getFrameworkName() {
    return getConf().get("mesos.hdfs.framework.name", "hdfs");
  }

  public long getFailoverTimeout() {
    return getConf().getLong("mesos.failover.timeout.sec", DEFAULT_FAILOVER_TIMEOUT);
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
    return getConf().getInt("mesos.hdfs.state.zk.timeout.ms", DEFAULT_ZK_TIME_MS);
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
        final String msg = "issue with host address";
        log.error(msg, e);
        throw new ConfigurationException(msg);
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
    return Integer.parseInt(configServerPortString);
  }

  public int getReconciliationTimeout() {
    return getConf().getInt("mesos.reconciliation.timeout.seconds", DEFAULT_RECONCILIATION_TIMEOUT);
  }

  public int getDeadNodeTimeout() {
    return getConf().getInt("mesos.hdfs.deadnode.timeout.seconds", 90);
  }
}
