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
    liveState.updateTaskForStatus(createRunningTaskStatus("journalnode", 2, ""));
    liveState.updateTaskForStatus(createRunningTaskStatus("journalnode", 1, ""));
    liveState.updateTaskForStatus(createRunningTaskStatus("datanode", 1, ""));

    assertEquals(2, liveState.getJournalNodeSize());
  }

  @Test
  public void getsDataNodeSize() {
    liveState.updateTaskForStatus(createRunningTaskStatus("datanode", 1, ""));
    assertEquals(1, liveState.getDataNodeSize());

    liveState.updateTaskForStatus(createRunningTaskStatus("datanode", 2, ""));
    Protos.TaskStatus taskStatus = createRunningTaskStatus("datanode", 3, "");
    liveState.updateTaskForStatus(taskStatus);
    liveState.updateTaskForStatus(createRunningTaskStatus("journalnode", 1, ""));
    assertEquals(3, liveState.getDataNodeSize());
    
    liveState.removeRunningTask(taskStatus.getTaskId());
    assertEquals(2, liveState.getDataNodeSize());
  }

  @Test
  public void getsNameNodeSize() {
    liveState.updateTaskForStatus(createRunningTaskStatus("journalnode", 1, ""));
    liveState.updateTaskForStatus(createRunningTaskStatus(HDFSConstants.NAME_NODE_TASKID, 1,
        HDFSConstants.NAME_NODE_INIT_MESSAGE));
    liveState.updateTaskForStatus(createRunningTaskStatus(HDFSConstants.NAME_NODE_TASKID, 2,
        HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE));
    liveState.updateTaskForStatus(createRunningTaskStatus("datanode", 1, ""));

    assertEquals(2, liveState.getNameNodeSize());
  }

  @Test
  public void getsFirstNamenodeTaskId() {
    assertEquals(null, liveState.getFirstNameNodeTaskId());
    liveState.updateTaskForStatus(createRunningTaskStatus("journalnode", 1, ""));
    liveState.updateTaskForStatus(createRunningTaskStatus(HDFSConstants.NAME_NODE_TASKID, 1,
        HDFSConstants.NAME_NODE_INIT_MESSAGE));
    liveState.updateTaskForStatus(createRunningTaskStatus(HDFSConstants.NAME_NODE_TASKID, 2,
        HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE));
    assertEquals(
        Protos.TaskID.newBuilder().setValue(HDFSConstants.NAME_NODE_TASKID + ".1").build(),
        liveState.getFirstNameNodeTaskId());
  }

  @Test
  public void getsSecondNamenodeTaskId() {
    assertEquals(null, liveState.getSecondNameNodeTaskId());
    liveState.updateTaskForStatus(createRunningTaskStatus("journalnode", 1, ""));
    liveState.updateTaskForStatus(createRunningTaskStatus(HDFSConstants.NAME_NODE_TASKID, 1,
        HDFSConstants.NAME_NODE_INIT_MESSAGE));
    assertEquals(null, liveState.getSecondNameNodeTaskId());
    liveState.updateTaskForStatus(createRunningTaskStatus(HDFSConstants.NAME_NODE_TASKID, 2,
        HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE));
    assertEquals(
        Protos.TaskID.newBuilder().setValue(HDFSConstants.NAME_NODE_TASKID + ".2").build(),
        liveState.getSecondNameNodeTaskId());
  }

  @Test
  public void getsFirstNamenodeSlaveId() {
    assertEquals(null, liveState.getFirstNameNodeSlaveId());
    liveState.updateTaskForStatus(createRunningTaskStatus("journalnode", 1, ""));
    liveState.updateTaskForStatus(createRunningTaskStatus(HDFSConstants.NAME_NODE_TASKID, 1,
        HDFSConstants.NAME_NODE_INIT_MESSAGE));
    liveState.updateTaskForStatus(createRunningTaskStatus(HDFSConstants.NAME_NODE_TASKID, 2,
        HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE));
    assertEquals("slave.1", liveState.getFirstNameNodeSlaveId().getValue());
  }

  @Test
  public void getsSecondNamenodeSlaveId() {
    assertEquals(null, liveState.getSecondNameNodeSlaveId());
    liveState.updateTaskForStatus(createRunningTaskStatus("journalnode", 2, ""));
    liveState.updateTaskForStatus(createRunningTaskStatus(HDFSConstants.NAME_NODE_TASKID, 1,
        HDFSConstants.NAME_NODE_INIT_MESSAGE));
    assertEquals(null, liveState.getSecondNameNodeSlaveId());
    liveState.updateTaskForStatus(createRunningTaskStatus(HDFSConstants.NAME_NODE_TASKID, 2,
        HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE));
    assertEquals("slave.2", liveState.getSecondNameNodeSlaveId().getValue());
  }

  @Test
  public void addsAndRemovesStagingTasks() {
    liveState.addStagingTask(createTaskInfo("journalnode").getTaskId());
    assertEquals(1, liveState.getStagingTasksSize());
    liveState.removeStagingTask(createTaskInfo("journalnode").getTaskId());
    assertEquals(0, liveState.getStagingTasksSize());
  }

  @Before
  public void setup() {
    liveState = new LiveState();
  }

  private Protos.TaskStatus createRunningTaskStatus(String taskId, Integer taskNumber, String message) {
    return Protos.TaskStatus.newBuilder()
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave." + taskNumber.toString()))
        .setTaskId(Protos.TaskID.newBuilder().setValue(taskId + "." + taskNumber.toString()))
        .setState(Protos.TaskState.TASK_RUNNING)
        .setMessage(message)
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
