package org.apache.mesos.hdfs.state;

import org.apache.commons.io.IOUtils;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.state.State;
import org.apache.mesos.state.Variable;
import org.apache.mesos.state.ZooKeeperState;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Manages persistence state of the application.  It does NOT have an references to Protocol Buffers.  It
 * throws exceptions to allow the manager to handle the logic.  It currently is tied to zookeeper, but should
 * be replaceable with any mesos state abstraction.
 */
public class HdfsZkStore implements IHdfsStore {

  private State state;


  public HdfsZkStore(HdfsFrameworkConfig hdfsFrameworkConfig) {

    this.state = new ZooKeeperState(hdfsFrameworkConfig.getStateZkServers(),
      hdfsFrameworkConfig.getStateZkTimeout(),
      TimeUnit.MILLISECONDS,
      "/hdfs-mesos/" + hdfsFrameworkConfig.getFrameworkName());
  }


  public byte[] getRawValueForId(String id) throws ExecutionException, InterruptedException {
    byte[] value = state.fetch(id).get().value();
    return value;
  }

  public void setRawValueForId(String id, byte[] newRawValue) throws ExecutionException, InterruptedException {
    Variable value = state.fetch(id).get();
    if (newRawValue == null) {
        value = value.mutate(new byte[]{});
    } else {
        value = value.mutate(newRawValue);
    }
    state.store(value).get();
  }

  /**
   * Get serializable object from store.
   *
   * @return serialized object or null if none
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws java.io.IOException
   * @throws ClassNotFoundException
   */
  @SuppressWarnings("unchecked")
  public <T extends Object> T get(String key) throws InterruptedException, ExecutionException,
    IOException, ClassNotFoundException {

    byte[] existingNodes = state.fetch(key).get().value();
    if (existingNodes.length > 0) {
      ByteArrayInputStream bis = new ByteArrayInputStream(existingNodes);
      ObjectInputStream in = null;
      try {
        in = new ObjectInputStream(bis);
        // generic in java lose their runtime information, there is no way to get this casted without
        // the need for the SuppressWarnings on the method.
        return (T) in.readObject();
      } finally {
        IOUtils.closeQuietly(bis);
        IOUtils.closeQuietly(in);
      }
    } else {
      return null;
    }
  }

  /**
   * Set serializable object in store.
   *
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws IOException
   */
  public <T extends Object> void set(String key, T object) throws InterruptedException,
    ExecutionException, IOException {

    Variable value = state.fetch(key).get();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream out = null;
    try {
      out = new ObjectOutputStream(bos);
      out.writeObject(object);
      value = value.mutate(bos.toByteArray());
      state.store(value).get();
    } finally {
      IOUtils.closeQuietly(bos);
      IOUtils.closeQuietly(out);
    }
  }
}
