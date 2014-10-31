package org.apache.mesos.hdfs;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.state.ClusterState;
import org.apache.mesos.hdfs.state.State;
import org.apache.mesos.state.ZooKeeperState;

import java.util.Properties;

import static org.mockito.Mockito.mock;

public class TestConfigModule extends AbstractModule {
  private String dataDir;
  private String credentialsPath = "";
  private String backupBucket = "test";
  private String backupPrefix = "testprefix";

  public void setDataDir(String dataDir) {
    this.dataDir = dataDir;
  }

  public void setCredentialsPath(String credentialsPath) {
    this.credentialsPath = credentialsPath;
  }

  @Provides
  Properties providesProperties() {
    return new Properties();
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
    int configServerPort = Integer.valueOf(props.getProperty("mesos.hdfs.config.server.port",
        "8765"));

    conf.set("mesos.hdfs.data.dir", dataDir);
    return new SchedulerConf(conf, configServerPort);
  }

  @Provides
  ClusterState providesClusterState(SchedulerConf schedulerConf) {
    ZooKeeperState zkState = mock(ZooKeeperState.class);
    State state = mock(State.class);
    ClusterState clusterState = ClusterState.getInstance();
    clusterState.init(state);
    return clusterState;
  }

  @Override
  protected void configure() {
  }
}
