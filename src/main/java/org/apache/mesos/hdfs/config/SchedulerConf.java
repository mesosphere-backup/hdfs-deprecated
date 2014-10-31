package org.apache.mesos.hdfs.config;

import org.apache.hadoop.conf.Configuration;

public class SchedulerConf extends MainConf {
  public SchedulerConf(Configuration conf, int configServerPort) {
    super(conf, configServerPort);
  }

  public int getHadoopHeapSize() {
    return getConf().getInt("mesos.hdfs.hadoop.heap.size", 512);
  }

  public int getDatanodeHeapSize() {
    return getConf().getInt("mesos.hdfs.datanode.heap.size", 1024);
  }

  public int getJournalnodeHeapSize() {
    return getHadoopHeapSize();
  }

  public int getNamenodeHeapSize() {
    return getConf().getInt("mesos.hdfs.namenode.heap.size", 4096);
  }

  public int getZkfcHeapSize() {
    return getHadoopHeapSize();
  }

  public double getJvmOverhead() {
    return getConf().getDouble("mesos.hdfs.jvm.overhead", 1.25);
  }

  public String getJvmOpts() {
    return getConf().get(
        "mesos.hdfs.jvm.opts",
        "" + "-XX:+UseConcMarkSweepGC " + "-XX:+CMSClassUnloadingEnabled " + "-XX:+UseTLAB "
            + "-XX:+AggressiveOpts " + "-XX:+UseCompressedOops " + "-XX:+UseFastEmptyMethods "
            + "-XX:+UseFastAccessorMethods " + "-Xss256k " + "-XX:+AlwaysPreTouch "
            + "-XX:+UseParNewGC " + "-Djava.library.path=/usr/lib:/usr/local/lib:lib/native");
  }

  public double getExecutorCpus() {
    return getConf().getDouble("mesos.hdfs.executor.cpus", 0.5);
  }

  public double getZkfcCpus() {
    return getExecutorCpus();
  }

  public int getExecutorHeap() {
    return getConf().getInt("mesos.hdfs.executor.heap.size", 256);
  }

  public double getNamenodeCpus() {
    return getConf().getDouble("mesos.hdfs.namenode.cpus", 8);
  }

  public double getJournalnodeCpus() {
    return getConf().getDouble("mesos.hdfs.journalnode.cpus", 1);
  }

  public double getDatanodeCpus() {
    return getConf().getDouble("mesos.hdfs.datanode.cpus", 2);
  }

  public int getTaskHeapSize(String taskName) {
    switch (taskName) {
      case "zkfc" :
        return getZkfcHeapSize();
      case "namenode" :
        return getNamenodeHeapSize();
      case "datanode" :
        return getDatanodeHeapSize();
      case "journalnode" :
        return getJournalnodeHeapSize();
      default :
        throw new RuntimeException("Invalid taskName=" + taskName);
    }
  }

  public double getTaskCpus(String taskName) {
    switch (taskName) {
      case "zkfc" :
        return getZkfcCpus();
      case "namenode" :
        return getNamenodeCpus();
      case "datanode" :
        return getDatanodeCpus();
      case "journalnode" :
        return getJournalnodeCpus();
      default :
        throw new RuntimeException("Invalid taskName=" + taskName);
    }
  }

  public int getJournalnodeCount() {
    return getConf().getInt("mesos.hdfs.journalnode.count", 3);
  }

  public String getExecUri() {
    return getConf().get("mesos.hdfs.executor.uri");
  }

  public String getClusterName() {
    return getConf().get("mesos.hdfs.cluster.name", "mesos-ha");
  }

  public long getFailoverTimeout() {
    return getConf().getLong("mesos.failover.timeout.sec", 7 * 24 * 3600);
  }

  public String getHdfsUser() {
    return getConf().get("mesos.hdfs.user", "hdfs");
  }

  public String getHdfsRole() {
    return getConf().get("mesos.hdfs.role", "hdfs");
  }

  public String getMesosMasterUri() {
    return getConf().get("mesos.master.uri");
  }

  public int getReconciliationStartupDelay() {
    return getConf().getInt("mesos.hdfs.reconciliation.startup.delay", 120);
  }

  public String getDataDir() {
    return getConf().get("mesos.hdfs.data.dir");
  }

  public String getHaZookeeperQuorum() {
    return getConf().get("mesos.hdfs.zkfc.ha.zookeeper.quorum");
  }

  public String getStorageProvider() {
    return getConf().get("mesos.hdfs.backup.storage.provider");
  }

  public String getStorageBucket() {
    return getConf().get("mesos.hdfs.backup.storage.bucket");
  }

  public String getStoragePrefix() {
    return getConf().get("mesos.hdfs.backup.storage.prefix");
  }

  public String getStorageCredentialsPath() {
    return getConf().get("mesos.hdfs.backup.storage.credentials.path");
  }
}
