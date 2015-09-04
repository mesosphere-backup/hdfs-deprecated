package org.apache.mesos.hdfs.scheduler;

import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value;

/**
 * Factory for generating Resources to launch HDFS Nodes.
 */
public class ResourceFactory {
  private String role;

  public ResourceFactory(String role) {
    this.role = role;
  }

  public Resource createCpuResource(double value) {
    return createScalarResource("cpus", value);
  }

  public Resource createMemResource(double value) {
    return createScalarResource("mem", value);
  }

  public Resource createPortResource(long begin, long end) {
    return createRangeResource("ports", begin, end);
  }

  private Resource createScalarResource(String name, double value) {
    return Resource.newBuilder()
      .setName(name)
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder()
          .setValue(value).build())
      .setRole(role)
      .build();
  }

  private Resource createRangeResource(String name, long begin, long end) {
    Value.Range range = Value.Range.newBuilder().setBegin(begin).setEnd(end).build();
    return Resource.newBuilder()
      .setName(name)
      .setType(Value.Type.RANGES)
      .setRanges(Value.Ranges.newBuilder().addRange(range))
      .build();
  }
}
