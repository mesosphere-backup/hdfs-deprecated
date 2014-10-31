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
    return getConf().getInt("mesos.hdfs.namenode.heap.size", 2048);
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
    return getConf().getDouble("mesos.hdfs.namenode.cpus", 0.5);
  }

  public double getJournalnodeCpus() {
    return getConf().getDouble("mesos.hdfs.journalnode.cpus", 0.5);
  }

  public double getDatanodeCpus() {
    return getConf().getDouble("mesos.hdfs.datanode.cpus", 1.5);
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
    return getConf().getInt("mesos.hdfs.journalnode.count", 1);
  }

  // TODO use different path for executor
  public String getExecUri() {
    return getConf().get("mesos.hdfs.executor.uri",
        "https://s3-us-west-1.amazonaws.com/mesosphere-executors-public/hadoop-mesos-cdh5.tar.gz");
  }

  public String getClusterName() {
    return getConf().get("mesos.hdfs.cluster.name", "mesos-ha");
  }

  public long getFailoverTimeout() {
    return getConf().getLong("mesos.failover.timeout.sec", 0);
  }

  public String getHdfsUser() {
    return getConf().get("mesos.hdfs.user", "hdfs");
  }

  public String getHdfsRole() {
    return getConf().get("mesos.hdfs.role", "*");
  }

  public String getMesosMasterUri() {
    return getConf().get("mesos.master.uri", "zk://localhost:2181/mesos");
  }

  public int getReconciliationStartupDelay() {
    return getConf().getInt("mesos.hdfs.reconciliation.startup.delay", 120);
  }

  public String getDataDir() {
    return getConf().get("mesos.hdfs.data.dir", "/tmp/hdfs/data");
  }

  public String getHaZookeeperQuorum() {
    return getConf().get("mesos.hdfs.zkfc.ha.zookeeper.quorum", "localhost:2181");
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
