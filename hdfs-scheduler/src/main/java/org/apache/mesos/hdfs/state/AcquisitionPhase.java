package org.apache.mesos.hdfs.state;

/**
 * Defines node types.
 */
public enum AcquisitionPhase {

  /**
   * Waits here for the timeout on (re)registration.
   */
  RECONCILING_TASKS,

  /**
   * Launches and waits for all journalnodes to start.
   */
  JOURNAL_NODES,

  /**
   * Launches the both namenodes.
   */
  START_NAME_NODES,

  /**
   * Formats both namenodes (first with initialize, second with bootstrap.
   */
  FORMAT_NAME_NODES,

  /**
   * Launches a configurable amount of datanodes.
   */
  DATA_NODES,

  /**
   * If everything is healthy, the scheduler stays here and awaits further 
   * instruction.
   */
  ACTIVE
}
