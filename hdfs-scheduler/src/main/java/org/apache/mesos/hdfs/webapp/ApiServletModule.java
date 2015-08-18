package org.apache.mesos.hdfs.webapp;

import org.apache.mesos.hdfs.api.ClusterResource;
import org.apache.mesos.hdfs.api.ConfigResource;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

/**
 * Guice module for configuring myriad web api.
 */
public class ApiServletModule extends ServletModule {

    @Override
    protected void configureServlets() {
        bind(ClusterResource.class);
        bind(ConfigResource.class);
        bind(GuiceContainer.class);
        serve("/api/*").with(GuiceContainer.class);
    }

}
