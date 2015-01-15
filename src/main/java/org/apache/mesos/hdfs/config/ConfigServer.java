package org.apache.mesos.hdfs.config;

import com.floreysoft.jmte.Engine;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.mesos.hdfs.state.ClusterState;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class ConfigServer {

  private Server server;
  private Engine engine;
  private SchedulerConf schedulerConf;

  @Inject
  public ConfigServer(SchedulerConf schedulerConf) throws Exception {
    this.schedulerConf = schedulerConf;
    engine = new Engine();
    server = new Server(schedulerConf.getConfigServerPort());

    ResourceHandler resource_handler = new ResourceHandler();
    resource_handler.setResourceBase(schedulerConf.getResourceBase());

    HandlerList handlers = new HandlerList();
    handlers.setHandlers(new Handler[]{
        resource_handler, new ServeHdfsConfigHandler()});
    server.setHandler(handlers);

    server.start();
  }

  public void stop() throws Exception {
    server.stop();
  }

  private class ServeHdfsConfigHandler extends AbstractHandler {
    public synchronized void handle(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) throws IOException {

      ClusterState clusterState = ClusterState.getInstance();
      File confFile = new File(schedulerConf.getConfigPath());

      if (!confFile.exists()) {
        throw new FileNotFoundException("Couldn't file config file: " + confFile.getPath()
            + ". Please make sure it exists.");
      }

      String content = new String(Files.readAllBytes(Paths.get(confFile.getPath())));

      Set<String> nameNodes = new TreeSet<>();
      nameNodes.addAll(clusterState.getNameNodeHosts());

      Set<String> journalNodes = new TreeSet<>();
      journalNodes.addAll(nameNodes);
      journalNodes.addAll(clusterState.getJournalNodeHosts());

      Map<String, Object> model = new HashMap<>();
      Iterator<String> iter = nameNodes.iterator();
      if (iter.hasNext()) {
        model.put("nn1Hostname", iter.next());
      }
      if (iter.hasNext()) {
        model.put("nn2Hostname", iter.next());
      }

      String journalNodeString = "";
      for (String jn : journalNodes) {
        journalNodeString += jn + ":8485;";
      }
      if (!journalNodeString.isEmpty()) {
        // Chop the trailing ,
        journalNodeString = journalNodeString.substring(0, journalNodeString.length() - 1);
      }

      model.put("journalnodes", journalNodeString);

      model.put("clusterName", schedulerConf.getClusterName());
      model.put("dataDir", schedulerConf.getDataDir());
      model.put("haZookeeperQuorum", schedulerConf.getHaZookeeperQuorum());

      content = engine.transform(content, model);

      response.setContentType("application/octet-stream;charset=utf-8");
      response
          .setHeader("Content-Disposition", "attachment; filename=\"" + "hdfs-site.xml" + "\" ");
      response.setHeader("Content-Transfer-Encoding", "binary");
      response.setHeader("Content-Length", Integer.toString(content.length()));

      response.setStatus(HttpServletResponse.SC_OK);
      baseRequest.setHandled(true);
      response.getWriter().println(content);
    }
  }

}
