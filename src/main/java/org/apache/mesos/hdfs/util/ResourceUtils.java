package org.apache.mesos.hdfs.util;

import org.apache.mesos.Protos;
import org.apache.mesos.hdfs.Scheduler;

import java.util.Arrays;
import java.util.List;

public class ResourceUtils {
  private final Scheduler scheduler;

  public ResourceUtils(Scheduler scheduler) {
    this.scheduler = scheduler;
  }

  public ResourceRoles getRolesForRole(Protos.Offer offer, ResourceRoles prevRoles, double cpus,
      double mem, int ports, String role) {
    ResourceRoles roles = new ResourceRoles(prevRoles);
    for (Protos.Resource resource : offer.getResourcesList()) {
      if (!role.isEmpty() && !resource.getRole().equals(role)) {
        continue;
      }
      if (resource.getName().equals("cpus") && prevRoles.cpuRole == null
          && resource.getScalar().getValue() >= cpus) {
        roles.cpuRole = resource.getRole();
      }
      if (resource.getName().equals("mem") && prevRoles.memRole == null
          && resource.getScalar().getValue() >= mem) {
        roles.memRole = resource.getRole();
      }
      if (resource.getName().equals("ports") && prevRoles.portsRole == null) {
        long portCount;

        for (Protos.Value.Range range : resource.getRanges().getRangeList()) {
          portCount = (range.getEnd() - range.getBegin()) + 1;
          if (portCount >= ports) {
            roles.portsRole = resource.getRole();
            roles.portsBegin = range.getBegin();
            roles.portsEnd = range.getBegin() + ports;
            break;
          }
        }
      }
    }
    return roles;
  }

  public ResourceRoles getRolesFor(Protos.Offer offer, double cpus, double mem, int ports) {
    ResourceRoles roles = new ResourceRoles();
    roles = getRolesForRole(offer, roles, cpus, mem, ports, scheduler.getConf().getHdfsRole());
    if (roles.cpuRole == null || roles.memRole == null || roles.portsRole == null) {
      roles = getRolesForRole(offer, roles, cpus, mem, ports, "");
      if (roles.cpuRole == null || roles.memRole == null || roles.portsRole == null) {
        return null;
      }
    }
    return roles;
  }

  public ResourceRoles sufficientRolesForNamenode(Protos.Offer offer) {
    // NameNode heap + JN + ZKFC heap + executor + overhead
    final double memNeeded = Math
        .ceil((scheduler.getConf().getExecutorHeap() + scheduler.getConf().getNamenodeHeapSize()
            + scheduler.getConf().getZkfcHeapSize() + scheduler.getConf().getJournalnodeHeapSize())
            * scheduler.getConf().getJvmOverhead());
    final double cpusNeeded = scheduler.getConf().getExecutorCpus()
        + scheduler.getConf().getNamenodeCpus() + scheduler.getConf().getJournalnodeCpus()
        + scheduler.getConf().getZkfcCpus();
    final int portsNeeded = 4;

    return getRolesFor(offer, cpusNeeded, memNeeded, portsNeeded);
  }

  public ResourceRoles sufficientRolesForJournalnode(Protos.Offer offer) {
    final double memNeeded = Math.ceil((scheduler.getConf().getExecutorHeap() + scheduler.getConf()
        .getJournalnodeHeapSize()) * scheduler.getConf().getJvmOverhead());
    final double cpusNeeded = scheduler.getConf().getExecutorCpus()
        + scheduler.getConf().getJournalnodeCpus();
    final int portsNeeded = 2;

    return getRolesFor(offer, cpusNeeded, memNeeded, portsNeeded);
  }

  public ResourceRoles sufficientRolesForDatanode(Protos.Offer offer) {
    final double memNeeded = Math.ceil((scheduler.getConf().getExecutorHeap() + scheduler.getConf()
        .getDatanodeHeapSize()) * scheduler.getConf().getJvmOverhead());
    final double cpusNeeded = scheduler.getConf().getExecutorCpus()
        + scheduler.getConf().getDatanodeCpus();
    final int portsNeeded = 3;

    return getRolesFor(offer, cpusNeeded, memNeeded, portsNeeded);
  }

  public List<Protos.Resource> buildResources(ResourceRoles roles, double cpus, int mem) {
    return Arrays.asList(
        Protos.Resource.newBuilder().setType(Protos.Value.Type.SCALAR).setName("cpus")
            .setRole(roles.cpuRole)
            .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpus).build()).build(),
        Protos.Resource
            .newBuilder()
            .setType(Protos.Value.Type.SCALAR)
            .setName("mem")
            .setRole(roles.memRole)
            .setScalar(
                Protos.Value.Scalar.newBuilder()
                    .setValue(mem * scheduler.getConf().getJvmOverhead()).build()).build());
  }
}
