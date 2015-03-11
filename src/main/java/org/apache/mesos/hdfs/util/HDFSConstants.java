package org.apache.mesos.hdfs.util;

/**
 * defines the constants shared between the scheduler and the executors
 */
public interface HDFSConstants {

  // Total number of NameNodes
  // Note: We do not currently support more or less than 2 NameNodes
  final Integer TOTAL_NAME_NODES = 2;

  // Messages
  final String NAME_NODE_INIT_MESSAGE = "-i";
  final String NAME_NODE_BOOTSTRAP_MESSAGE = "-b";
  final String RELOAD_CONFIG = "reload config";

  // NodeIds
  final String NAME_NODE_ID = "namenode";
  final String JOURNAL_NODE_ID = "journalnode";
  final String DATA_NODE_ID = "datanode";
  final String ZKFC_NODE_ID = "zkfc";

  // NameNode TaskId
  final String NAME_NODE_TASKID = ".namenode.namenode.";

  // ExecutorsIds
  final String NODE_EXECUTOR_ID = "NodeExecutor";
  final String NAME_NODE_EXECUTOR_ID = "NameNodeExecutor";

  // Path to Store HDFS Binary
  final String HDFS_BINARY_DIR = "hdfs";

  // Current HDFS Binary File Name
  final String HDFS_BINARY_FILE_NAME = "hdfs-mesos-0.0.2.tgz";

  // HDFS Config File Name
  final String HDFS_CONFIG_FILE_NAME = "hdfs-site.xml";

}
