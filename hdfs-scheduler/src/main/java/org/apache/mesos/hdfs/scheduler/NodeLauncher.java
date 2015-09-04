package org.apache.mesos.hdfs.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.SchedulerDriver;

/**
 * NodeLauncher. 
 */
public class NodeLauncher {
  private final Log log = LogFactory.getLog(NodeLauncher.class);

  public boolean launch(SchedulerDriver driver, HdfsNode node, Offer offer) {
    String nodeName = node.getName();
    OfferID offerId = offer.getId();

    log.info(String.format("Node: %s, evaluating offer: %s", nodeName, offerId));
    boolean acceptOffer = node.evaluate(offer);

    if (acceptOffer) {
      log.info(String.format("Node: %s, accepting offer: %s", nodeName, offerId));
      node.launch(driver, offer);
    } else {
      log.info(String.format("Node: %s, declining offer: %s", nodeName, offerId));
      driver.declineOffer(offerId);
    }

    return acceptOffer;
  }
}
