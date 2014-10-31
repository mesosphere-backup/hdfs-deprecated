package org.apache.mesos.hdfs.state;

import org.squirrelframework.foundation.fsm.StateMachineBuilderFactory;
import org.squirrelframework.foundation.fsm.UntypedStateMachine;
import org.squirrelframework.foundation.fsm.UntypedStateMachineBuilder;
import org.squirrelframework.foundation.fsm.annotation.StateMachineParameters;
import org.squirrelframework.foundation.fsm.impl.AbstractUntypedStateMachine;

public class StateMachine {

  public static UntypedStateMachine build() {
    UntypedStateMachineBuilder builder = StateMachineBuilderFactory.create(StateMachineImpl.class);
    builder.onEntry("B").callMethod("ontoB");

    return builder.newStateMachine("InitialState");
  }

  enum States {
    Start,

    New, Bootstrapped,

    NeedNamenode, NeedJournalnode,

    Normal,
  }

  enum Events {
    ReceivedOffers,

    NamenodeReady, JournalnodeReady, ZkfcReady, DatanodeReady,

    NamenodeRunning, JournalnodeRunning, ZkfcRunning, DatanodeRunning,

    NamenodeStopped, JournalnodeStopped, ZkfcStopped, DatanodeStopped,
  }

  // 2. Define State Machine Class
  @StateMachineParameters(stateType = States.class, eventType = Events.class, contextType = Context.class)
  static class StateMachineImpl extends AbstractUntypedStateMachine {
  }

  public class Context {
    public Context() {
    }
  }
}
