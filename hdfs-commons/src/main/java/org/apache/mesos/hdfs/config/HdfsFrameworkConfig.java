package org.apache.mesos.hdfs.config;

import com.google.inject.Singleton;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

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

  private static final int DEFAULT_HADOOP_HEAP_SIZE = 512;
  private static final int DEFAULT_EXECUTOR_HEAP_SIZE = 256;
  private static final int DEFAULT_DATANODE_HEAP_SIZE = 1024;
  private static final int DEFAULT_NAMENODE_HEAP_SIZE = 4096;

  private static final double DEFAULT_CPUS = 0.5;
  private static final double DEFAULT_EXECUTOR_CPUS = DEFAULT_CPUS;
  private static final double DEFAULT_NAMENODE_CPUS = 1;
  private static final double DEFAULT_JOURNAL_CPUS = 1;
  private static final double DEFAULT_DATANODE_CPUS = 1;

  private static final double DEFAULT_JVM_OVERHEAD = 1.35;
  private static final int DEFAULT_JOURNAL_NODE_COUNT = 3;
  private static final int DEFAULT_FAILOVER_TIMEOUT_SEC = 31449600;
  private static final int DEFAULT_ZK_TIME_MS = 20000;
  private static final int DEFAULT_RECONCILIATION_TIMEOUT_SEC = 4;
  private static final int DEFAULT_MAX_RECONCILIATION_TIMEOUT_SEC = 30;
  private static final int DEFAULT_DEADNODE_TIMEOUT_SEC = 90;

  private final Log log = LogFactory.getLog(HdfsFrameworkConfig.class);

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

  public String getPrincipal() {
    return getConf().get("mesos.hdfs.principal", "");
  }

  public String getSecret() {
    return getConf().get("mesos.hdfs.secret", "");
  }

  public boolean cramCredentialsEnabled() {
    String principal = getPrincipal();
    String secret = getSecret();
    boolean principalExists = !principal.isEmpty();
    boolean secretExists = !secret.isEmpty();

    return principalExists && secretExists;
  }

  public boolean usingMesosDns() {
    return Boolean.valueOf(getConf().get("mesos.hdfs.mesosdns", "false"));
  }

  public String getMesosDnsDomain() {
    return getConf().get("mesos.hdfs.mesosdns.domain", "mesos");
  }

  public boolean usingNativeHadoopBinaries() {
    return Boolean.valueOf(getConf().get("mesos.hdfs.native-hadoop-binaries", "false"));
  }

  public String getExecutorPath() {
    return getConf().get("mesos.hdfs.executor.path", ".");
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
      case "zkfc":
        cpus = getZkfcCpus();
        break;
      case "namenode":
        cpus = getNameNodeCpus();
        break;
      case "datanode":
        cpus = getDataNodeCpus();
        break;
      case "journalnode":
        cpus = getJournalNodeCpus();
        break;
      default:
        final String msg = "Invalid request for CPUs for taskName= " + taskName;
        log.error(msg);
        throw new ConfigurationException(msg);
    }
    return cpus;
  }

  public int getTaskHeapSize(String taskName) {
    int size;
    switch (taskName) {
      case "zkfc":
        size = getZkfcHeapSize();
        break;
      case "namenode":
        size = getNameNodeHeapSize();
        break;
      case "datanode":
        size = getDataNodeHeapSize();
        break;
      case "journalnode":
        size = getJournalNodeHeapSize();
        break;
      default:
        final String msg = "Invalid request for heapsize for taskName = " + taskName;
        log.error(msg);
        throw new ConfigurationException(msg);
    }
    return size;
  }

  public int getJournalNodeCount() {
    return getConf().getInt("mesos.hdfs.journalnode.count", DEFAULT_JOURNAL_NODE_COUNT);
  }

  public String getFrameworkName() {
    return getConf().get("mesos.hdfs.framework.name", "hdfs");
  }

  public long getFailoverTimeout() {
    return getConf().getLong("mesos.failover.timeout.sec", DEFAULT_FAILOVER_TIMEOUT_SEC);
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
    return getConf().get("mesos.hdfs.data.dir", "/var/lib/hdfs/data");
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
        throw new ConfigurationException(e);
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
    return getConf().getInt("mesos.reconciliation.timeout.sec", DEFAULT_RECONCILIATION_TIMEOUT_SEC);
  }

  public int getMaxReconciliationTimeout() {
    return getConf().getInt("mesos.max-reconciliation.timeout.sec", DEFAULT_MAX_RECONCILIATION_TIMEOUT_SEC);
  }

  public int getDeadNodeTimeout() {
    return getConf().getInt("mesos.hdfs.deadnode.timeout.seconds", DEFAULT_DEADNODE_TIMEOUT_SEC);
  }

  public String getJreUrl() {
    return getConf().get("mesos.hdfs.jre-url", "https://downloads.mesosphere.io/java/jre-7u76-linux-x64.tar.gz");
  }

  public String getLdLibraryPath() {
    return getConf().get("mesos.hdfs.ld-library-path", "/usr/local/lib");
  }

  public String getJreVersion() {
    return getConf().get("mesos.hdfs.jre-version", "jre1.7.0_76");
  }
}
