package org.apache.mesos.hdfs;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.state.AcquisitionPhase;
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.state.PersistentState;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class TestScheduler {

  private final SchedulerConf schedulerConf = new SchedulerConf(new Configuration());

  @Mock
  SchedulerDriver driver;

  @Mock
  PersistentState persistentState;

  @Mock
  LiveState liveState;

  @Captor
  ArgumentCaptor<Collection<Protos.TaskInfo>> taskInfosCapture;

  Scheduler scheduler;

  @Test
  public void statusUpdateWasStagingNowRunning() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.JOURNAL_NODES);

    Protos.TaskID taskId = createTaskId("1");

    scheduler.statusUpdate(driver, createTaskStatus(taskId, Protos.TaskState.TASK_RUNNING));

    verify(liveState).removeStagingTask(taskId);
  }

  @Test
  public void statusUpdateTransitionFromAcquiringJournalNodesToNameNode1() {
    Protos.TaskID taskId = createTaskId("1");

    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.JOURNAL_NODES);
    when(liveState.getJournalNodeSize()).thenReturn(1);

    scheduler.statusUpdate(driver,
        createTaskStatus(taskId, Protos.TaskState.TASK_RUNNING));

    verify(liveState).transitionTo(AcquisitionPhase.NAME_NODE_1);
  }

  @Test
  public void statusUpdateAcquiringJournalNodesNotEnoughYet() {
    Protos.TaskID taskId = createTaskId("1");

    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.JOURNAL_NODES);
    when(liveState.getJournalNodeSize()).thenReturn(2);

    scheduler.statusUpdate(driver,
        createTaskStatus(taskId, Protos.TaskState.TASK_RUNNING));

    verify(liveState, never()).transitionTo(AcquisitionPhase.NAME_NODE_1);
  }

  @Test
  public void statusUpdateTransitionFromAcquiringNameNode1ToNameNode2() {
    Protos.TaskID taskId = createTaskId(HDFSConstants.NAME_NODE_TASKID + "1");
    Protos.SlaveID slaveId = createSlaveId("1");
    Protos.ExecutorID executorId = createExecutorId("executor.namenode.1");

    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.NAME_NODE_1);
    when(liveState.getNameNodeSize()).thenReturn(1);
    when(liveState.getFirstNameNodeTaskId()).thenReturn(taskId);
    when(liveState.getFirstNameNodeSlaveId()).thenReturn(slaveId);

    scheduler.statusUpdate(driver,
        createTaskStatus(taskId, Protos.TaskState.TASK_RUNNING));

    verify(liveState).transitionTo(AcquisitionPhase.NAME_NODE_2);
  }

  @Test
  public void statusUpdateTransitionFromAcquiringNameNode2ToDataNode() {
    Protos.TaskID taskId1 = createTaskId(HDFSConstants.NAME_NODE_TASKID + "1");
    Protos.SlaveID slaveId1 = createSlaveId("1");
    Protos.ExecutorID executorId1 = createExecutorId("executor.namenode.1");
    Protos.TaskID taskId2 = createTaskId(HDFSConstants.NAME_NODE_TASKID + "1");
    Protos.SlaveID slaveId2 = createSlaveId("1");
    Protos.ExecutorID executorId2 = createExecutorId("executor.namenode.1");

    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.NAME_NODE_2);
    when(liveState.getNameNodeSize()).thenReturn(2);
    when(liveState.getFirstNameNodeTaskId()).thenReturn(taskId1);
    when(liveState.getFirstNameNodeSlaveId()).thenReturn(slaveId2);
    when(liveState.getSecondNameNodeTaskId()).thenReturn(taskId2);
    when(liveState.getSecondNameNodeSlaveId()).thenReturn(slaveId2);

    scheduler.statusUpdate(driver,
        createTaskStatus(taskId1, Protos.TaskState.TASK_RUNNING));

    verify(driver).sendFrameworkMessage(executorId1, slaveId1,
        HDFSConstants.NAME_NODE_INIT_MESSAGE.getBytes());

    verify(driver).sendFrameworkMessage(executorId2, slaveId2,
        HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE.getBytes());

    verify(liveState).transitionTo(AcquisitionPhase.DATA_NODES);
  }

  @Test
  public void statusUpdateAquiringDataNodesJustStays() {
    Protos.TaskID taskId = createTaskId("1");

    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.DATA_NODES);

    scheduler.statusUpdate(driver,
        createTaskStatus(taskId, Protos.TaskState.TASK_RUNNING));

    verify(liveState, never()).transitionTo(any(AcquisitionPhase.class));

  }

  @Test
  public void startsAJournalNodeWhenGivenAnOffer() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.JOURNAL_NODES);

    scheduler.resourceOffers(driver,
        Lists.newArrayList(
            createTestOffer(0),
            createTestOffer(1),
            createTestOffer(2)
            ));

    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
    assertEquals(1, taskInfosCapture.getValue().size());
  }

  @Test
  public void launchesOnlyNeededNumberOfJournalNodes() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.JOURNAL_NODES);
    when(liveState.getJournalNodeSize()).thenReturn(1);

    scheduler.resourceOffers(driver,
        Lists.newArrayList(
            createTestOffer(0)
            ));

    verify(driver, never()).launchTasks(anyList(), anyList());
  }

  @Test
  public void launchesNamenodeWhenInNamenode1Phase() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.NAME_NODE_1);

    scheduler.resourceOffers(driver, Lists.newArrayList(createTestOffer(0)));

    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
    Protos.TaskInfo taskInfo = taskInfosCapture.getValue().iterator().next();
    assertTrue(taskInfo.getName().contains(HDFSConstants.NAME_NODE_ID));
  }

  @Test
  public void launchesNamenodeWhenInNamenode2Phase() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.NAME_NODE_2);

    scheduler.resourceOffers(driver, Lists.newArrayList(createTestOffer(0)));

    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
    Protos.TaskInfo taskInfo = taskInfosCapture.getValue().iterator().next();
    assertTrue(taskInfo.getName().contains(HDFSConstants.NAME_NODE_ID));
  }

  @Test
  public void declinesAnyOffersPastWhatItNeeds() {
    Scheduler scheduler = new Scheduler(schedulerConf, new LiveState(), persistentState);

    scheduler.resourceOffers(driver,
        Lists.newArrayList(
            createTestOffer(0),
            createTestOffer(1),
            createTestOffer(2),
            createTestOffer(3)
            ));

    verify(driver, times(3)).declineOffer(any(Protos.OfferID.class));
  }

  @Test
  public void launchesDataNodesWhenInDatanodesPhase() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.DATA_NODES);

    scheduler.resourceOffers(driver,
        Lists.newArrayList(
            createTestOffer(0)
            )
        );

    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
    Protos.TaskInfo taskInfo = taskInfosCapture.getValue().iterator().next();
    assertTrue(taskInfo.getName().contains(HDFSConstants.DATA_NODE_ID));
  }

  @Test
  public void removesTerminalTasksFromLiveState() {
    scheduler.statusUpdate(driver, createTaskStatus(createTaskId("0"),
        Protos.TaskState.TASK_FAILED));
    scheduler.statusUpdate(driver, createTaskStatus(createTaskId("1"),
        Protos.TaskState.TASK_FINISHED));
    scheduler.statusUpdate(driver, createTaskStatus(createTaskId("2"),
        Protos.TaskState.TASK_KILLED));
    scheduler.statusUpdate(driver, createTaskStatus(createTaskId("3"),
        Protos.TaskState.TASK_LOST));

    verify(liveState, times(4)).removeStagingTask(any(Protos.TaskID.class));
    verify(liveState, times(4)).removeTask(any(Protos.TaskID.class));
  }

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    this.scheduler = new Scheduler(schedulerConf, liveState, persistentState);
  }

  private Protos.TaskID createTaskId(String id) {
    return Protos.TaskID.newBuilder().setValue(id).build();
  }

  private Protos.OfferID createTestOfferId(int instanceNumber) {
    return Protos.OfferID.newBuilder().setValue("offer" + instanceNumber).build();
  }

  private Protos.SlaveID createSlaveId(String slaveId) {
    return Protos.SlaveID.newBuilder().setValue(slaveId).build();
  }

  private Protos.ExecutorID createExecutorId(String executorId) {
    return Protos.ExecutorID.newBuilder().setValue(executorId).build();
  }

  private Protos.Offer createTestOffer(int instanceNumber) {
    return Protos.Offer.newBuilder()
        .setId(createTestOfferId(instanceNumber))
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework1").build())
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave" + instanceNumber).build())
        .setHostname("host" + instanceNumber)
        .build();
  }

  private Protos.TaskStatus createTaskStatus(Protos.TaskID taskID, Protos.TaskState state) {
    return Protos.TaskStatus.newBuilder()
        .setTaskId(taskID)
        .setState(state)
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave").build())
        .setMessage("From Test")
        .build();
  }
}
