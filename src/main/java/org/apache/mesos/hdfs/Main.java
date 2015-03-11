package org.apache.mesos.hdfs;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.mesos.hdfs.config.ConfigServer;

/**
 * Main entry point for the Scheduler
 */
public final class Main {

  private Main() {
  }

  public static void main(String[] args) throws Exception {
    Injector injector = Guice.createInjector();
    Thread scheduler = new Thread(injector.getInstance(HDFSScheduler.class));
    scheduler.start();

    injector.getInstance(ConfigServer.class);
  }
}
