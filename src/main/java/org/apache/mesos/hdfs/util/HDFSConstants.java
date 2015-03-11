package org.apache.mesos.hdfs.util;

/**
 * defines the constants shared between the scheduler and the executors
 */
public interface HDFSConstants {

  // Total number of NameNodes
  // Note: We do not currently support more or less than 2 NameNodes
  Integer TOTAL_NAME_NODES = 2;

  // Messages
  String NAME_NODE_INIT_MESSAGE = "-i";
  String NAME_NODE_BOOTSTRAP_MESSAGE = "-b";
  String RELOAD_CONFIG = "reload config";

  // NodeIds
  String NAME_NODE_ID = "namenode";
  String JOURNAL_NODE_ID = "journalnode";
  String DATA_NODE_ID = "datanode";
  String ZKFC_NODE_ID = "zkfc";

  // NameNode TaskId
  String NAME_NODE_TASKID = ".namenode.namenode.";

  // ExecutorsIds
  String NODE_EXECUTOR_ID = "NodeExecutor";
  String NAME_NODE_EXECUTOR_ID = "NameNodeExecutor";

  // Path to Store HDFS Binary
  String HDFS_BINARY_DIR = "hdfs";

  // Current HDFS Binary File Name
  String HDFS_BINARY_FILE_NAME = "hdfs-mesos-0.0.2.tgz";

  // HDFS Config File Name
  String HDFS_CONFIG_FILE_NAME = "hdfs-site.xml";

}
