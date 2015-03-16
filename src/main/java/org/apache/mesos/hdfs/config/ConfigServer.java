package org.apache.mesos.hdfs.config;

import com.floreysoft.jmte.Engine;
import com.google.inject.Inject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hdfs.state.PersistentState;
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
 * Provides configuration information through http API service.
 */
public class ConfigServer {

  public final Log log = LogFactory.getLog(ConfigServer.class);

  private Server server;
  private Engine engine;
  private HdfsFrameworkConfig hdfsConf;
  private PersistentState persistentState;

  @Inject
  public ConfigServer(HdfsFrameworkConfig hdfsConf) {
    this(hdfsConf, new PersistentState(hdfsConf));
  }

  public ConfigServer(HdfsFrameworkConfig hdfsConf, PersistentState persistentState) {
    this.hdfsConf = hdfsConf;
    this.persistentState = persistentState;
    engine = new Engine();
    server = new Server(hdfsConf.getConfigServerPort());
    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setResourceBase(hdfsConf.getExecutorPath());
    HandlerList handlers = new HandlerList();
    handlers.setHandlers(new Handler[]{
        resourceHandler, new ServeHdfsConfigHandler()});
    server.setHandler(handlers);

    try {
      server.start();

      // NOPMD jetty throws a generic exception, we have to catch it!
    } catch (Exception e) {
      final String msg = "unable to start jetty server";
      log.error(msg, e);
      throw new ConfigServerException(msg);
    }
  }

  public void stop() {
    try {
      server.stop();
    } catch (Exception e) {
      log.debug("unable to stop the jetty service", e);
    }
  }

  private class ServeHdfsConfigHandler extends AbstractHandler {

    public synchronized void handle(String target,
                                    Request baseRequest,
                                    HttpServletRequest request,
                                    HttpServletResponse response) throws IOException {

      File confFile = new File(hdfsConf.getConfigPath());

      if (!confFile.exists()) {
        throw new FileNotFoundException("Couldn't file config file: " + confFile.getPath()
            + ". Please make sure it exists.");
      }

      String content = new String(Files.readAllBytes(Paths.get(confFile.getPath())), Charset.defaultCharset());

      Set<String> nameNodes = new TreeSet<>();
      nameNodes.addAll(persistentState.getNameNodes().keySet());

      Set<String> journalNodes = new TreeSet<>();
      journalNodes.addAll(persistentState.getJournalNodes().keySet());

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
      model.put("frameworkName", hdfsConf.getFrameworkName());
      model.put("dataDir", hdfsConf.getDataDir());
      model.put("haZookeeperQuorum", hdfsConf.getHaZookeeperQuorum());

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
