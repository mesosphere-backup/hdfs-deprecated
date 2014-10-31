package org.apache.mesos.hdfs.config;

import com.floreysoft.jmte.Engine;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.mesos.hdfs.state.ClusterState;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

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
<<<<<<< HEAD
  public ConfigServer(SchedulerConf schedulerConf) throws Exception {
=======
  public ConfigServer(SchedulerConf schedulerConf, @Named("ConfigPath") String sitePath,
      ClusterState clusterState) throws Exception {
>>>>>>> master
    this.schedulerConf = schedulerConf;
    engine = new Engine();
    server = new Server(schedulerConf.getConfigServerPort());
    server.setHandler(new ServeHdfsConfigHandler());
    server.start();
  }

  public void stop() throws Exception {
    server.stop();
  }

  private class ServeHdfsConfigHandler extends AbstractHandler {
<<<<<<< HEAD
    public synchronized void handle(String target, Request baseRequest, HttpServletRequest request,
=======
    public void handle(String target, Request baseRequest, HttpServletRequest request,
>>>>>>> master
        HttpServletResponse response) throws IOException {

      ClusterState clusterState = ClusterState.getInstance();
      File confFile = new File("etc/hadoop/hdfs-site.xml");

      if (!confFile.exists()) {
        throw new FileNotFoundException("Couldn't file config file: " + confFile.getPath()
            + ". Please make sure it exists.");
      }

      String content = new String(Files.readAllBytes(Paths.get(confFile.getPath())));

      Set<String> namenodes = new TreeSet<>();
      namenodes.addAll(clusterState.getNamenodeHosts());

      Set<String> journalnodes = new TreeSet<>();
      journalnodes.addAll(namenodes);
      journalnodes.addAll(clusterState.getJournalnodeHosts());

      Map<String, Object> model = new HashMap<>();
      Iterator<String> iter = namenodes.iterator();
      if (iter.hasNext()) {
        model.put("nn1Hostname", iter.next());
      }
      if (iter.hasNext()) {
        model.put("nn2Hostname", iter.next());
      }

      String journalnodeString = "";
      for (String jn : journalnodes) {
        journalnodeString += jn + ":8485;";
      }
      if (!journalnodeString.isEmpty()) {
        journalnodeString = journalnodeString.substring(0, journalnodeString.length() - 1); // Chop
                                                                                            // trailing
                                                                                            // ','
      }

      model.put("journalnodes", journalnodeString);

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
