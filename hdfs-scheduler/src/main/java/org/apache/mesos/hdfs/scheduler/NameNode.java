package org.apache.mesos.hdfs.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.IPersistentStateStore;
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.util.DnsResolver;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.SchedulerDriver;

import java.util.Arrays;
import java.util.List;

/**
 * HDFS Mesos Framework Scheduler class implementation.
 */
public class NameNode extends HdfsNode {
  private final Log log = LogFactory.getLog(NameNode.class);
  private List<String> taskTypes = Arrays.asList(HDFSConstants.NAME_NODE_ID, HDFSConstants.ZKFC_NODE_ID);
  private String executorName = HDFSConstants.NAME_NODE_EXECUTOR_ID;
  private DnsResolver dnsResolver;

  public NameNode(
      LiveState liveState,
      IPersistentStateStore persistentStore,
      DnsResolver dnsResolver,
      HdfsFrameworkConfig config) {
    super(liveState, persistentStore, config, HDFSConstants.NAME_NODE_ID);
    this.dnsResolver = dnsResolver;
  }

  public boolean evaluate(Offer offer) {
    boolean accept = false;

    if (dnsResolver.journalNodesResolvable()) {
      if (offerNotEnoughResources(offer,
            (config.getNameNodeCpus() + config.getZkfcCpus()),
            (config.getNameNodeHeapSize() + config.getZkfcHeapSize()))) {
        log.info("Offer does not have enough resources");
      } else {
        List<String> deadNameNodes = persistenceStore.getDeadNameNodes();

        if (deadNameNodes.isEmpty()) {
          if (persistenceStore.getNameNodes().size() == HDFSConstants.TOTAL_NAME_NODES) {
            log.info(String.format("Already running %s namenodes", HDFSConstants.TOTAL_NAME_NODES));
          } else if (persistenceStore.nameNodeRunningOnSlave(offer.getHostname())) {
            log.info(String.format("Already running namenode on %s", offer.getHostname()));
          } else if (persistenceStore.dataNodeRunningOnSlave(offer.getHostname())) {
            log.info(String.format("Cannot colocate namenode and datanode on %s", offer.getHostname()));
          } else if (!persistenceStore.journalNodeRunningOnSlave(offer.getHostname())) {
            log.info(String.format("We need to coloate the namenode with a journalnode and there is"
                  + "no journalnode running on this host. %s", offer.getHostname()));
          } else {
            accept = true;
          }
        } else if (deadNameNodes.contains(offer.getHostname())) {
          accept = true;
        }
      }
    }

    return accept;
  }

  public void launch(SchedulerDriver driver, Offer offer) {
    launch(driver, offer, name, taskTypes, executorName);
  }
}
