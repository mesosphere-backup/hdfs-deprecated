package org.apache.mesos.hdfs.config;

import com.floreysoft.jmte.Engine;
import com.google.inject.Inject;
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.util.HDFSConstants;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class ConfigServer {

  private Server server;
  private Engine engine;
  private final SchedulerConf schedulerConf;
  private final LiveState liveState;

  @Inject
  public ConfigServer(SchedulerConf schedulerConf, LiveState liveState) throws Exception {
    this.schedulerConf = schedulerConf;
    this.liveState = liveState;
    engine = new Engine();
    server = new Server(schedulerConf.getConfigServerPort());
    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setResourceBase(schedulerConf.getExecutorPath());
    HandlerList handlers = new HandlerList();
    handlers.setHandlers(new Handler[]{
        resourceHandler, new ServeHdfsConfigHandler()});
    server.setHandler(handlers);
    server.start();
  }

  public void stop() throws Exception {
    server.stop();
  }

  private class ServeHdfsConfigHandler extends AbstractHandler {
    public synchronized void handle(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) throws IOException {

      File confFile = new File(schedulerConf.getConfigPath());

      if (!confFile.exists()) {
        throw new FileNotFoundException("Couldn't file config file: " + confFile.getPath()
            + ". Please make sure it exists.");
      }

      String content = new String(Files.readAllBytes(Paths.get(confFile.getPath())));
      Map<String, Object> model = new HashMap<>();

      Set<String> nameNodes = new TreeSet<>();
      Set<String> journalNodes = new TreeSet<>();

      if (schedulerConf.usingMesosDns()) { //we can just use the default name scheme
        for (int i = HDFSConstants.TOTAL_NAME_NODES; i > 0; i--)
          nameNodes.add(HDFSConstants.NAME_NODE_ID + i + 
              "." + schedulerConf.getFrameworkName() + 
              "." + schedulerConf.getMesosDnsDomain());
        for (int i = schedulerConf.getJournalNodeCount() + HDFSConstants.TOTAL_NAME_NODES; i > 0; i--)
          journalNodes.add(HDFSConstants.JOURNAL_NODE_ID + i + 
              "." + schedulerConf.getFrameworkName() + 
              "." + schedulerConf.getMesosDnsDomain());
      } else {
        nameNodes.addAll(liveState.getNameNodeHosts());
        journalNodes.addAll(liveState.getNameNodeHosts());
        journalNodes.addAll(liveState.getJournalNodeHosts());
      }

      //add namenodes to hdfs schedulerConf
      Iterator<String> iter = nameNodes.iterator();
      for (int i = nameNodes.size(); i > 0; i--) {
        model.put("nn" + i + "Hostname", iter.next());
      }

      //add journal nodes to hdfs schedulerConf
      Iterator<String> jiter = journalNodes.iterator();
      String jNodeString = "";
      while (jiter.hasNext()) {
        jNodeString += jiter.next() + ":8485";
        if (jiter.hasNext()) jNodeString += ";";
      }
      model.put("journalnodes", jNodeString);

      model.put("frameworkName", schedulerConf.getFrameworkName());
      model.put("dataDir", schedulerConf.getDataDir());
      model.put("haZookeeperQuorum", schedulerConf.getHaZookeeperQuorum());

      content = engine.transform(content, model);

      response.setContentType("application/octet-stream;charset=utf-8");
      response.setHeader("Content-Disposition", "attachment; filename=\"" +
          HDFSConstants.HDFS_CONFIG_FILE_NAME + "\" ");
      response.setHeader("Content-Transfer-Encoding", "binary");
      response.setHeader("Content-Length", Integer.toString(content.length()));

      response.setStatus(HttpServletResponse.SC_OK);
      baseRequest.setHandled(true);
      response.getWriter().println(content);
    }
  }

}
