package org.apache.mesos.hdfs.api;

import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.state.AcquisitionPhase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.google.inject.Inject;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * Cluster management API.
 */
@Path("cluster")
public class ClusterResource {
  private final Log log = LogFactory.getLog(ClusterResource.class);

  private HdfsFrameworkConfig config;
  private LiveState liveState;

  @Inject
  public ClusterResource(HdfsFrameworkConfig config, LiveState liveState) {
    this.config = config;
    this.liveState = liveState;
  }

  @POST
  @Path("scaleup")
  public Response scaleUp(@QueryParam("instances") int instances) {
    log.info("Received ScaleUp Request");
    log.info(String.format(" Instances: %d", instances));
    if (instances <= liveState.getDataNodeSize() || 
        instances < config.getDFSReplication()) {
      return Response.status(Status.BAD_REQUEST).build();
    } 
    return scale(instances);
  }

  private Response scale(int instances) {
    config.setDataNodeCount(instances);
    liveState.transitionTo(AcquisitionPhase.DATA_NODES);
    return Response.ok().build();
  }

}
