package org.apache.mesos.hdfs.config;

import com.floreysoft.jmte.Engine;
import com.google.inject.Inject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hdfs.state.IPersistentStateStore;
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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * This is the HTTP service which allows executors to fetch the configuration for hdfs-site.xml.
 */
public class ConfigServer {
  private final Log log = LogFactory.getLog(ConfigServer.class);

  private Server server;
  private Engine engine;
  private HdfsFrameworkConfig hdfsFrameworkConfig;
  private IPersistentStateStore persistenceStore;

  @Inject
  public ConfigServer(HdfsFrameworkConfig hdfsFrameworkConfig, IPersistentStateStore persistenceStore) {
    this.hdfsFrameworkConfig = hdfsFrameworkConfig;
    this.persistenceStore = persistenceStore;
    engine = new Engine();
    server = new Server(hdfsFrameworkConfig.getConfigServerPort());
    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setResourceBase(hdfsFrameworkConfig.getExecutorPath());
    HandlerList handlers = new HandlerList();
    handlers.setHandlers(new Handler[]{
      resourceHandler, new ServeHdfsConfigHandler()});
    server.setHandler(handlers);

    try {
      server.start();

    } catch (Exception e) {
      final String msg = "Unable to start jetty server";
      log.error(msg, e);
      throw new ConfigServerException(msg, e);
    }
  }

  public void stop() throws ConfigServerException {
    try {
      server.stop();
    } catch (Exception e) {
      final String msg = "Unable to stop the jetty service";
      log.error(msg, e);
      throw new ConfigServerException(msg, e);
    }
  }

  private class ServeHdfsConfigHandler extends AbstractHandler {
    public synchronized void handle(String target, Request baseRequest, HttpServletRequest request,
      HttpServletResponse response) throws IOException {

      File confFile = new File(hdfsFrameworkConfig.getConfigPath());

      if (!confFile.exists()) {
        throw new FileNotFoundException("Couldn't file config file: " + confFile.getPath()
          + ". Please make sure it exists.");
      }

      String content = new String(Files.readAllBytes(Paths.get(confFile.getPath())), Charset.defaultCharset());

      Set<String> nameNodes = new TreeSet<>();
      nameNodes.addAll(persistenceStore.getNameNodes().keySet());

      Set<String> journalNodes = new TreeSet<>();
      journalNodes.addAll(persistenceStore.getJournalNodes().keySet());

      Map<String, Object> model = new HashMap<>();
      Iterator<String> iter = nameNodes.iterator();
      if (iter.hasNext()) {
        model.put("nn1Hostname", iter.next());
      }
      if (iter.hasNext()) {
        model.put("nn2Hostname", iter.next());
      }

      String journalNodeString = getJournalNodes(journalNodes);

      model.put("journalnodes", journalNodeString);
      model.put("frameworkName", hdfsFrameworkConfig.getFrameworkName());
      model.put("dataDir", hdfsFrameworkConfig.getDataDir());
      model.put("haZookeeperQuorum", hdfsFrameworkConfig.getHaZookeeperQuorum());

      String node = request.getParameter("node");
      if (hdfsFrameworkConfig.getBackupDir() != null && node != null && node.startsWith(HDFSConstants.NAME_NODE_ID)) {
        model.put("backupDir", hdfsFrameworkConfig.getBackupDir());
        model.put("node", node);
      }

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

    private String getJournalNodes(Set<String> journalNodes) {
      StringBuilder journalNodeStringBuilder = new StringBuilder("");
      for (String jn : journalNodes) {
        journalNodeStringBuilder.append(jn).append(":8485;");
      }
      String journalNodeString = journalNodeStringBuilder.toString();

      if (!journalNodeString.isEmpty()) {
        // Chop the trailing ,
        journalNodeString = journalNodeString.substring(0, journalNodeString.length() - 1);
      }
      return journalNodeString;
    }
  }
}
