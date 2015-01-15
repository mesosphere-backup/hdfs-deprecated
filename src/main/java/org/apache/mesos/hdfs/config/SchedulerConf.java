package org.apache.mesos.hdfs.config;

import org.apache.hadoop.conf.Configuration;

public class SchedulerConf extends MainConf {
  public SchedulerConf(Configuration conf, int configServerPort) {
    super(conf, configServerPort);
  }

  public int getHadoopHeapSize() {
    return getConf().getInt("mesos.hdfs.hadoop.heap.size", 512);
  }

  public int getDataNodeHeapSize() {
    return getConf().getInt("mesos.hdfs.datanode.heap.size", 1024);
  }

  public int getJournalNodeHeapSize() {
    return getHadoopHeapSize();
  }

  public int getNameNodeHeapSize() {
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

  public int getExecutorHeap() {
    return getConf().getInt("mesos.hdfs.executor.heap.size", 256);
  }

  public double getNameNodeCpus() {
    return getConf().getDouble("mesos.hdfs.namenode.cpus", 0.5);
  }
  public double getJournalNodeCpus() {
    return getConf().getDouble("mesos.hdfs.journalnode.cpus", 0.5);
  }

  public double getDataNodeCpus() {
    return getConf().getDouble("mesos.hdfs.datanode.cpus", 1.5);
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
    return getConf().getInt("mesos.hdfs.journalnode.count", 1);
  }

  public String getExecUri() {
    return getConf().get("mesos.hdfs.executor.uri",
        "hdfs-mesos-0.0.2.tgz");
  }

  public String getResourceBase() {
    return getConf().get("mesos.hdfs.resource.base", "/tmp/resources");
  }

  public String getClusterName() {
    return getConf().get("mesos.hdfs.cluster.name", "mesos-ha");
  }

  public long getFailoverTimeout() {
    return getConf().getLong("mesos.failover.timeout.sec", 0);
  }

  public String getHdfsUser() {
    return getConf().get("mesos.hdfs.user", "root");
  }

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

  public String getConfigPath() {
    return getConf().get("mesos.hdfs.config.path", "etc/hadoop/hdfs-site.xml");
  }

  public String getHaZookeeperQuorum() {
    return getConf().get("mesos.hdfs.zkfc.ha.zookeeper.quorum", "localhost:2181");
  }

}
