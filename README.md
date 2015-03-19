[![Build Status](https://travis-ci.org/mesosphere/hdfs.svg?branch=master)](https://travis-ci.org/mesosphere/hdfs)
HA HDFS on Apache Mesos
======================
Starts 1 active NameNode (with JournalNode and ZKFC), 1 standby NN (+JN,ZKFC), 1 JN, and everything else is DataNodes.

Building HDFS-Mesos
--------------------------
1. `./bin/build-hdfs`
2. Run `./bin/build-hdfs nocompile` to skip the `mvn clean package` step and just re-bundle the binaries.
3. To remove the project build output and downloaded binaries, run `./bin/build-hdfs clean`

Installing HDFS-Mesos on your Cluster
--------------------------
1. Upload `hdfs-mesos-*.tgz` to a node in your Mesos cluster.
2. Extract it with `tar zxvf hdfs-mesos-*.tgz`
3. Customize configuration in `hdfs-mesos-*/etc/hadoop/*-site.xml`
4. Check that `hostname` on that node resolves to a non-localhost IP; update /etc/hosts if necessary

Starting HDFS-Mesos
--------------------------
1. `cd hdfs-mesos-*`
2. `./bin/hdfs-mesos`
3. Check the Mesos web console to wait until all tasks are RUNNING (monitor status in JN sandboxes)

Using HDFS
--------------------------
See some of the many HDFS tutorials out there for more details and explore the web UI at <br>`http://<ActiveNameNode>:50070`.</br> 
Note that you can access commands through `hdfs://<mesos.hdfs.framework.name>/` (default: `hdfs://hdfs/`). 
Also here is a quick sanity check:

1. `hadoop fs -ls hdfs://hdfs/` should show nothing for starters
2. `hadoop fs -put /path/to/src_file hdfs://hdfs/`
3. `hadoop fs -ls hdfs://hdfs/` should now list src_file

Resource Reservation Instructions (Optional)
--------------------------

1. In mesos-site.xml, change mesos.hdfs.role to hdfs.
2. On master, add the role for HDFS, by running `echo hdfs > /etc/mesos-master/role` or by setting the `—-role=hdfs`.
3. Then restart the master by running `sudo service mesos-master restart`.
4. On each slave where you want to reserve resources, add specific resource reservations for the HDFS role. Here is one example:
<br>`cpus(*):4;cpus(hdfs):2;mem(*):8192;mem(hdfs):4096 > /etc/mesos-slave/resources`</br> or by setting `—-resources=cpus(*):4;cpus(hdfs):2;mem(*):8192;mem(hdfs):4096`.
5. On each slave with the new settings, stop the mesos slave by running
<br>`sudo service mesos-slave stop`</br>.
6. On each slave with the new settings, remove the old slave state by running
<br>`rm -f /tmp/mesos/meta/slaves/latest`</br>. 
<br>Note: This will also remove task state, so you will want to manually kill any running tasks as a precaution.</br>
7. On each slave with the new settings, start the mesos slave by running
<br>`sudo service mesos-slave start`</br>.

