package org.apache.mesos.hdfs.util;

/**
 * Constants for HDFS.
 */
public final class HDFSConstants {

  private HDFSConstants() {
  }

  // Total number of NameNodes
  // Note: We do not currently support more or less than 2 NameNodes
  public static final Integer TOTAL_NAME_NODES = 2;
  public static final Integer MILLIS_FROM_SECONDS = 1000;

  // Messages
  public static final String NAME_NODE_INIT_MESSAGE = "-i";
  public static final String NAME_NODE_BOOTSTRAP_MESSAGE = "-b";
  public static final String JOURNAL_NODE_INIT_MESSAGE = "-s";
  public static final String RELOAD_CONFIG = "reload config";

  // NodeIds
  public static final String NAME_NODE_ID = "namenode";
  public static final String JOURNAL_NODE_ID = "journalnode";
  public static final String DATA_NODE_ID = "datanode";
  public static final String ZKFC_NODE_ID = "zkfc";

  // NameNode TaskId
  public static final String NAME_NODE_TASKID = ".namenode.namenode.";

  // ExecutorsIds
  public static final String NODE_EXECUTOR_ID = "NodeExecutor";
  public static final String NAME_NODE_EXECUTOR_ID = "NameNodeExecutor";

  // Path to Store HDFS Binary
  public static final String HDFS_BINARY_DIR = "hdfs";

  // Current HDFS Binary File Name
  public static final String HDFS_BINARY_FILE_NAME = "hdfs-mesos-executor-0.1.5.tgz";

  // HDFS Config File Name
  public static final String HDFS_CONFIG_FILE_NAME = "hdfs-site.xml";

  // Listening Ports
  public static final Integer DATA_NODE_PORT = 50075;
  public static final Integer JOURNAL_NODE_PORT = 8480;
  public static final Integer ZKFC_NODE_PORT = 8019;
  public static final Integer NAME_NODE_PORT = 50070;

  // Exit codes
  public static final Integer PROC_EXIT_CODE = 1;
  public static final Integer RELOAD_EXIT_CODE = 2;
  public static final Integer NAMENODE_EXIT_CODE = 3;
  public static final Integer RECONCILE_EXIT_CODE = 4;

  // NameNode initialization constants 
  public static final String ZK_FRAMEWORK_ID_KEY = "FrameworkId";
  public static final Integer ZK_MUTEX_ACQUIRE_TIMEOUT_SEC = 30;
  public static final Integer CURATOR_MAX_RETRIES = 3;

  public static final String NAMENODE_NUM_PARAM = "nn";

  public static final String NN_STATUS_KEY = "status";
  public static final String NN_STATUS_INIT_VAL = "initialized";
  public static final String NN_STATUS_UNINIT_VAL = "uninitialized";
  public static final String NN_STATUS_FORMATTED_VAL = "formatted";
  public static final String NN_STATUS_BOOTSTRAPPED_VAL = "bootstrapped";

  public static final String PROPERTY_VAR_PREFIX = "MESOS_HDFS_";

  public static final Integer POLL_DELAY_MS = 1000;
  public static final String HOST_PORT_SEPARATOR = ":";
}
