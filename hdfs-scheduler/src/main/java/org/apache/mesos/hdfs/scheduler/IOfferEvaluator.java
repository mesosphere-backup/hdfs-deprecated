package org.apache.mesos.hdfs.scheduler;

import org.apache.mesos.Protos.Offer;

/**
 * IOfferEvaluator interface. 
 */
public interface IOfferEvaluator {
  public boolean evaluate(Offer offer);
}

