package org.apache.mesos.hdfs;

import com.google.protobuf.ByteString;
import org.apache.mesos.Protos;
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestLiveState {

  private LiveState liveState;

  @Test
  public void getsJournalNodeSize() {
    liveState.updateTaskForStatus(createTaskStatus("journalnode", 2));
    liveState.updateTaskForStatus(createTaskStatus("journalnode", 1));
    liveState.updateTaskForStatus(createTaskStatus("datanode", 1));

    assertEquals(2, liveState.getJournalNodeSize());
  }

  @Test
  public void getsNameNodeSize() {
    liveState.updateTaskForStatus(createTaskStatus("journalnode", 1));
    liveState.updateTaskForStatus(createTaskStatus(HDFSConstants.NAME_NODE_TASKID, 2));
    liveState.updateTaskForStatus(createTaskStatus(HDFSConstants.NAME_NODE_TASKID, 1));
    liveState.updateTaskForStatus(createTaskStatus("datanode", 1));

    assertEquals(2, liveState.getNameNodeSize());
  }

  @Test
  public void getsFirstNamenodeTaskId() {
    assertEquals(null, liveState.getFirstNameNodeTaskId());
    liveState.updateTaskForStatus(createTaskStatus("journalnode", 1));
    liveState.updateTaskForStatus(createTaskStatus(HDFSConstants.NAME_NODE_TASKID, 1));
    liveState.updateTaskForStatus(createTaskStatus(HDFSConstants.NAME_NODE_TASKID, 2));
    assertEquals(
        Protos.TaskID.newBuilder().setValue(HDFSConstants.NAME_NODE_TASKID + ".1").build(),
        liveState.getFirstNameNodeTaskId());
  }

  @Test
  public void getsSecondNamenodeTaskId() {
    assertEquals(null, liveState.getSecondNameNodeTaskId());
    liveState.updateTaskForStatus(createTaskStatus("journalnode", 1));
    liveState.updateTaskForStatus(createTaskStatus(HDFSConstants.NAME_NODE_TASKID, 1));
    assertEquals(null, liveState.getSecondNameNodeTaskId());
    liveState.updateTaskForStatus(createTaskStatus(HDFSConstants.NAME_NODE_TASKID, 2));
    assertEquals(
        Protos.TaskID.newBuilder().setValue(HDFSConstants.NAME_NODE_TASKID + ".2").build(),
        liveState.getSecondNameNodeTaskId());
  }

  @Test
  public void getsFirstNamenodeSlaveId() {
    assertEquals(null, liveState.getFirstNameNodeSlaveId());
    liveState.updateTaskForStatus(createTaskStatus("journalnode", 1));
    liveState.updateTaskForStatus(createTaskStatus(HDFSConstants.NAME_NODE_TASKID, 1));
    liveState.updateTaskForStatus(createTaskStatus(HDFSConstants.NAME_NODE_TASKID, 2));
    assertEquals("slave.1", liveState.getFirstNameNodeSlaveId().getValue());
  }

  @Test
  public void getsSecondNamenodeSlaveId() {
    assertEquals(null, liveState.getSecondNameNodeSlaveId());
    liveState.updateTaskForStatus(createTaskStatus("journalnode", 2));
    liveState.updateTaskForStatus(createTaskStatus(HDFSConstants.NAME_NODE_TASKID, 1));
    assertEquals(null, liveState.getSecondNameNodeSlaveId());
    liveState.updateTaskForStatus(createTaskStatus(HDFSConstants.NAME_NODE_TASKID, 2));
    assertEquals("slave.2", liveState.getSecondNameNodeSlaveId().getValue());
  }

  @Test
  public void addsAndRemovesStagingTasks() {
    liveState.addStagingTask(createTaskInfo("journalnode"));
    assertEquals(1, liveState.getStagingTasksSize());
    liveState.removeStagingTask(createTaskInfo("journalnode").getTaskId());
    assertEquals(0, liveState.getStagingTasksSize());
  }

  @Before
  public void setup() {
    liveState = new LiveState();
  }

  private Protos.TaskStatus createTaskStatus(String taskId, Integer taskNumber) {
    return Protos.TaskStatus.newBuilder()
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave." + taskNumber.toString()))
        .setTaskId(Protos.TaskID.newBuilder().setValue(taskId + "." + taskNumber.toString()))
        .setState(Protos.TaskState.TASK_RUNNING)
        .build();
  }

  private Protos.TaskInfo createTaskInfo(String taskName) {
    return Protos.TaskInfo.newBuilder()
        .setName(taskName)
        .setTaskId(Protos.TaskID.newBuilder().setValue(taskName + "." + "1"))
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave.1"))
        .setData(ByteString.copyFromUtf8(
            String.format("bin/hdfs-mesos-%s", taskName)))
        .build();
  }
}
