package org.apache.mesos.hdfs;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.mesos.hdfs.config.ConfigServer;

public class Main {

  public static void main(String[] args) throws Exception {
    Injector injector = Guice.createInjector(new ProdConfigModule());
    Thread sched = new Thread(injector.getInstance(Scheduler.class));
    sched.start();

    injector.getInstance(ConfigServer.class);
  }

}
