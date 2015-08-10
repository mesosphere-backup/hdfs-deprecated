package org.apache.mesos.hdfs.api;

import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.IPersistentStateStore;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.floreysoft.jmte.Engine;
import com.google.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import java.io.File;
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
 * Config generation API.
 */
@Path("config")
public class ConfigResource {
  private final Log log = LogFactory.getLog(ConfigResource.class);

  private HdfsFrameworkConfig config;
  private IPersistentStateStore store;
  private Engine engine;

  @Inject
  public ConfigResource(HdfsFrameworkConfig config, IPersistentStateStore store) {
    this.config = config;
    this.store = store;
    this.engine = new Engine();
  }

  @GET
  @Path(HDFSConstants.HDFS_CONFIG_FILE_NAME)
  @Produces(MediaType.APPLICATION_XML)
  public Response getHdfsConfig() {
    File confFile = new File(config.getConfigPath());

    if (!confFile.exists()) {
      log.warn("Couldn't find config file: " + confFile.getPath() + ". Please make sure it exists.");
      return Response.status(Status.INTERNAL_SERVER_ERROR).build();
    }

    String content;
    try {
      content = new String(Files.readAllBytes(Paths.get(confFile.getPath())), Charset.defaultCharset());
    } catch (IOException e) {
      log.warn("IOException: " + e.getMessage());
      return Response.status(Status.INTERNAL_SERVER_ERROR).build();
    }

    Set<String> nameNodes = new TreeSet<>();
    nameNodes.addAll(store.getNameNodes().keySet());

    Set<String> journalNodes = new TreeSet<>();
    journalNodes.addAll(store.getJournalNodes().keySet());

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
    model.put("frameworkName", config.getFrameworkName());
    model.put("dataDir", config.getDataDir());
    model.put("haZookeeperQuorum", config.getHaZookeeperQuorum());

    content = engine.transform(content, model);

    ResponseBuilder response = Response.ok(content);
    response.header("Content-Length", content.length());
    return response.build();
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
