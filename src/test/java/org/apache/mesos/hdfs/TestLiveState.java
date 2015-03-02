package org.apache.mesos.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestLiveState {

  private final SchedulerConf schedulerConf = new SchedulerConf(new Configuration());

  @Test
  public void testNameNodeTask() {

    LiveState clusterState = new LiveState(schedulerConf);

    TaskID taskId = TaskID.newBuilder()
        .setValue(".namenode.namenode.123")
        .build();

    SlaveID slaveId = SlaveID.newBuilder()
        .setValue("worker10.19.15.1")
        .build();

    // Add task
    clusterState.addTask(taskId, HDFSConstants.NAME_NODE_ID, "10.19.15.1", "worker10.19.15.1");

    TaskStatus taskStatus = TaskStatus.newBuilder()
        .setTaskId(taskId)
        .setSlaveId(slaveId)
        .setState(TaskState.TASK_RUNNING)
        .build();

    // Update task
    clusterState.updateTask(taskStatus);

    assertTrue(clusterState.getNameNodes().contains(taskId));
    assertFalse(clusterState.notInDfsHosts(slaveId.getValue()));
    assertTrue(clusterState.getNameNodeHosts().contains("10.19.15.1"));
  }

  @Test
  public void testJournalNodeTask() {
    LiveState clusterState = new LiveState(schedulerConf);

    TaskID taskId = TaskID.newBuilder()
        .setValue(".namenode.journalnode.123")
        .build();

    SlaveID slaveId = SlaveID.newBuilder()
        .setValue("worker10.80.16.2")
        .build();

    // Add task
    clusterState.addTask(taskId, HDFSConstants.JOURNAL_NODE_ID, "10.80.16.2", "worker10.80.16.2");

    TaskStatus taskStatus = TaskStatus.newBuilder()
        .setTaskId(taskId)
        .setSlaveId(slaveId)
        .setState(TaskState.TASK_RUNNING)
        .build();

    // Update task
    clusterState.updateTask(taskStatus);

    assertTrue(clusterState.getJournalNodes().contains(taskId));
    assertFalse(clusterState.notInDfsHosts(slaveId.getValue()));
    assertTrue(clusterState.getJournalNodeHosts().contains("10.80.16.2"));

    // Remove task
    clusterState.removeTask(taskStatus);

    assertFalse(clusterState.getJournalNodes().contains(taskId));
    assertTrue(clusterState.notInDfsHosts(slaveId.getValue()));
    assertFalse(clusterState.getJournalNodeHosts().contains("10.80.16.2"));
  }
}
