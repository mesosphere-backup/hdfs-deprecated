package org.apache.mesos.hdfs.util;

public interface HDFSConstants {

  // Total number of NameNodes
  // Note: We do not currently support more or less than 2 NameNodes
  static final Integer TOTAL_NAME_NODES = 2;

  // Messages
  static final String NAME_NODE_INIT_MESSAGE = "-i";
  static final String NAME_NODE_BOOTSTRAP_MESSAGE = "-b";
  static final String RELOAD_CONFIG = "reload config";

  // NodeIds
  static final String NAME_NODE_ID = "namenode";
  static final String JOURNAL_NODE_ID = "journalnode";
  static final String DATA_NODE_ID = "datanode";
  static final String ZKFC_NODE_ID = "zkfc";

  // NameNode TaskId
  static final String NAME_NODE_TASKID = ".namenode.namenode.";

  // ExecutorsIds
  static final String NODE_EXECUTOR_ID = "NodeExecutor";
  static final String NAME_NODE_EXECUTOR_ID = "NameNodeExecutor";

  // Path to Store HDFS Binary
  static final String HDFS_BINARY_DIR = "hdfs";

  // Current HDFS Binary File Name
  static final String HDFS_BINARY_FILE_NAME = "hdfs-mesos-0.0.2.tgz";

  // HDFS Config File Name
  static final String HDFS_CONFIG_FILE_NAME = "hdfs-site.xml";

}
