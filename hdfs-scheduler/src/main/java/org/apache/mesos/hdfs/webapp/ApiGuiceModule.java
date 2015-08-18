package org.apache.mesos.hdfs.webapp;

import com.google.inject.AbstractModule;
import org.mortbay.jetty.Connector;

/**
 * Guice web application configuration.
 */
public class ApiGuiceModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(Connector.class).toProvider(HttpConnectorProvider.class);
    install(new ApiServletModule());
  }

}
