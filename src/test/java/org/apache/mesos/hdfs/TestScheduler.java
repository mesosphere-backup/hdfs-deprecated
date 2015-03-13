package org.apache.mesos.hdfs;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.state.AcquisitionPhase;
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.state.PersistentState;
import org.apache.mesos.hdfs.util.DnsResolver;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collection;

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

  @Mock
  DnsResolver dnsResolver;

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
  public void statusUpdateTransitionFromAcquiringJournalNodesToStartingNameNodes() {
    Protos.TaskID taskId = createTaskId("1");

    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.JOURNAL_NODES);
    when(liveState.getJournalNodeSize()).thenReturn(3);

    scheduler.statusUpdate(driver,
        createTaskStatus(taskId, Protos.TaskState.TASK_RUNNING));

    verify(liveState).transitionTo(AcquisitionPhase.START_NAME_NODES);
  }

  @Test
  public void statusUpdateAcquiringJournalNodesNotEnoughYet() {
    Protos.TaskID taskId = createTaskId("1");

    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.JOURNAL_NODES);
    when(liveState.getJournalNodeSize()).thenReturn(2);

    scheduler.statusUpdate(driver,
        createTaskStatus(taskId, Protos.TaskState.TASK_RUNNING));

    verify(liveState, never()).transitionTo(AcquisitionPhase.START_NAME_NODES);
  }

  @Test
  public void statusUpdateTransitionFromStartingNameNodesToFormateNameNodes() {
    Protos.TaskID taskId = createTaskId(HDFSConstants.NAME_NODE_TASKID + "1");
    Protos.SlaveID slaveId = createSlaveId("1");

    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.START_NAME_NODES);
    when(liveState.getNameNodeSize()).thenReturn(2);
    when(liveState.getJournalNodeSize()).thenReturn(schedulerConf.getJournalNodeCount());
    when(liveState.getFirstNameNodeTaskId()).thenReturn(taskId);
    when(liveState.getFirstNameNodeSlaveId()).thenReturn(slaveId);

    scheduler.statusUpdate(driver,
        createTaskStatus(taskId, Protos.TaskState.TASK_RUNNING));

    verify(liveState).transitionTo(AcquisitionPhase.FORMAT_NAME_NODES);
  }

  @Test
  public void statusUpdateTransitionFromFormatNameNodesToDataNodes() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.FORMAT_NAME_NODES);
    when(liveState.getJournalNodeSize()).thenReturn(schedulerConf.getJournalNodeCount());
    when(liveState.getNameNodeSize()).thenReturn(HDFSConstants.TOTAL_NAME_NODES);
    when(liveState.isNameNode1Initialized()).thenReturn(true);
    when(liveState.isNameNode2Initialized()).thenReturn(true);

    scheduler.statusUpdate(
        driver,
        createTaskStatus(createTaskId(HDFSConstants.NAME_NODE_TASKID),
            Protos.TaskState.TASK_RUNNING));

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
        Lists.newArrayList(createTestOfferWithResources(0, 2, 1024)));

    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
    assertEquals(1, taskInfosCapture.getValue().size());
  }

  @Test
  public void launchesOnlyNeededNumberOfJournalNodes() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.JOURNAL_NODES);
    when(liveState.getJournalNodeSize()).thenReturn(3);

    scheduler.resourceOffers(driver, Lists.newArrayList(createTestOffer(0)));

    verify(driver, never()).launchTasks(anyList(), anyList());
  }

  @Test
  public void launchesNamenodeWhenInNamenode1Phase() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.START_NAME_NODES);
    when(persistentState.journalNodeRunningOnSlave("host0")).thenReturn(true);
    when(dnsResolver.journalNodesResolvable()).thenReturn(true);

    scheduler.resourceOffers(driver, Lists.newArrayList(createTestOffer(0)));

    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
    Protos.TaskInfo taskInfo = taskInfosCapture.getValue().iterator().next();
    assertTrue(taskInfo.getName().contains(HDFSConstants.NAME_NODE_ID));
  }

  @Test
  public void declinesAnyOffersPastWhatItNeeds() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.DATA_NODES);

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
    verify(liveState, times(4)).removeRunningTask(any(Protos.TaskID.class));
  }

  @Test
  public void declinesOffersWithNotEnoughResources() {
    when(liveState.getCurrentAcquisitionPhase()).thenReturn(AcquisitionPhase.DATA_NODES);
    Protos.Offer offer = createTestOfferWithResources(0, 0.1, 64);

    scheduler.resourceOffers(driver, Lists.newArrayList(offer));

    verify(driver, times(1)).declineOffer(offer.getId());
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

  private Protos.Offer createTestOfferWithResources(int instanceNumber, double cpus, int mem) {
    return Protos.Offer.newBuilder()
        .setId(createTestOfferId(instanceNumber))
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework1").build())
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave" + instanceNumber).build())
        .setHostname("host" + instanceNumber)
        .addAllResources(Arrays.asList(
            Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder()
                    .setValue(cpus).build())
                .setRole("*")
                .build(),
            Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder()
                    .setValue(mem).build())
                .setRole("*")
                .build()))
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
