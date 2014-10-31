package org.apache.mesos.hdfs.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class MainConf extends Configured {
  final private int configServerPort;

  MainConf(Configuration conf, int configServerPort) {
    setConf(conf);
    this.configServerPort = configServerPort;
  }

  public String getClusterName() {
    return getConf().get("mesos.hdfs.cluster.name", "hdfs-super-available");
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

  public int getConfigServerPort() {
    return configServerPort;
  }
}
