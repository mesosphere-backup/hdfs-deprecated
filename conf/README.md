Configuring your *-site.xml files
======================

Please look at [a full list of Hadoop settings](http://hadoop.apache.org/docs/r2.6.0/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml)

hdfs-site.xml
--------------------------
### dfs.datanode.dns.interface
Sets the internal interface (e.g. eth0) for your nodes to communicate

###dfs.datanode.dns.nameserver
This is the nameserver (must be in IPv4) that the nodes use to discover each other. In DCOS (and mesos DNS), this can be set to `mesos-dns.mesos`

mesos-site.xml
--------------------------

... more documentation to come