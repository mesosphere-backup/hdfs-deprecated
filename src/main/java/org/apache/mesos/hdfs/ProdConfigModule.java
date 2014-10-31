package org.apache.mesos.hdfs;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mesos.MesosNativeLibrary;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.state.ClusterState;
import org.apache.mesos.hdfs.state.State;
import org.apache.mesos.state.ZooKeeperState;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

class ProdConfigModule extends AbstractModule {
  @Provides
  Properties providesProperties() {
    return System.getProperties();
  }

  @Provides
  @Named("ConfigPath")
  String providesConfigPath(Properties props) {
    String sitePath = props.getProperty("mesos.site.path", "etc/hadoop");
    return new Path(props.getProperty("mesos.conf.path", sitePath + "/mesos-site.xml")).toString();
  }

  @Provides
  SchedulerConf providesSchedulerConfig(Properties props, @Named("ConfigPath") String configPath) {
    Configuration conf = new Configuration();
    conf.addResource(configPath);
    int configServerPort = Integer.valueOf(props.getProperty("mesos.hdfs.config.server.port",
        "8765"));
    return new SchedulerConf(conf, configServerPort);
  }

  @Provides
  ClusterState providesClusterState(SchedulerConf schedulerConf) {
    MesosNativeLibrary.load(schedulerConf.getNativeLibrary());
    ZooKeeperState zkState = new ZooKeeperState(schedulerConf.getStateZkServers(),
        schedulerConf.getStateZkTimeout(), TimeUnit.MILLISECONDS, "/hdfs-mesos/"
            + schedulerConf.getClusterName());
    State state = new State(zkState);
    return new ClusterState(state);
  }

  @Provides
  BackupService providesBackupService(SchedulerConf schedulerConf) throws Exception {
    return (BackupService) Class.forName(schedulerConf.getStorageProvider())
        .getDeclaredConstructor(SchedulerConf.class).newInstance(schedulerConf);
  }

  @Override
  protected void configure() {
  }
}
