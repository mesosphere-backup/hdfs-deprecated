package org.apache.mesos.hdfs.webapp;

import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.nio.SelectChannelConnector;

/**
 * Provides the jetty http connector.
 */
public class HttpConnectorProvider implements Provider<Connector> {

  private HdfsFrameworkConfig config;

  @Inject
  public HttpConnectorProvider(HdfsFrameworkConfig config) {
    this.config = config;
  }

  @Override
  public Connector get() {
    SelectChannelConnector scc = new SelectChannelConnector();
    scc.setHost(config.getFrameworkHostAddress());
    scc.setPort(config.getConfigServerPort());
    return scc;
  }

}
