package org.apache.mesos.hdfs.webapp;

import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import com.google.inject.Inject;
import com.google.inject.servlet.GuiceFilter;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.FilterMapping;
import org.mortbay.jetty.servlet.ServletHandler;

/**
 * Jetty web server configuration.
 */
public class ApiServer {

  private Server jetty;
  private Connector connector;
  private GuiceFilter filter;
  private HdfsFrameworkConfig config;

  @Inject
  public ApiServer(Server jetty, Connector connector, GuiceFilter filter,
      HdfsFrameworkConfig config) {
    this.jetty = jetty;
    this.connector = connector;
    this.filter = filter;
    this.config = config;
  }

  public void start() throws Exception {
    jetty.addConnector(connector);

    String filterName = "HdfsGuiceFilter";

    FilterMapping mapping = new FilterMapping();
    mapping.setPathSpec("/*");
    mapping.setDispatches(Handler.ALL);
    mapping.setFilterName(filterName);

    FilterHolder holder = new FilterHolder(filter);
    holder.setName(filterName);

    ServletHandler handler = new ServletHandler();
    handler.addFilter(holder, mapping);

    Context context = new Context();
    context.setServletHandler(handler);
    context.addServlet(DefaultServlet.class, "/");
    context.setResourceBase(config.getExecutorPath());

    jetty.addHandler(context);
    jetty.start();
  }

}
