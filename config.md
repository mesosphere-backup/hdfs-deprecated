## Configuration of HDFS framework and HDFS

The configuration of HDFS and this framework are managed via:

* hdfs-site.xml
* mesos-site.xml
* system env vars and properties

The hdfs-site.xml file is used to configure the hdfs cluster.  The values must match the configuration of the scheduler.  For this
reason the hdfs-site.xml is generally "fetched" or refreshed from the scheduler when a node is started.   The normal configuration of
the hdfs-site.xml has variables which are replaced by the scheduler when the xml file is fetched by the node.  An example of these
variables is `${frameworkName}`.   The scheduler code that does the variable replacement is handled by ConfigServer.java.  An
example of this variable replacement is `model.put("frameworkName", hdfsFrameworkConfig.getFrameworkName());`

It is possible to have the HDFS-mesos framework manage hdfs node instances on slaves that are previously provisioned with hdfs.  Under scenario
there is no way to update the `hdfs-site.xml` file.  This is indicated by setting the property `mesos.hdfs.native-hadoop-binaries` == true in the `mesos-site.xml` file.
This indicates that binaries exist on the nodes.  Because the values in the `hdfs-site.xml` are not controlled by the HDFS-Mesos framework, it
is important to make sure that all the xml files are consistent and the framework is started with property values which are consistent with the
preexisting cluster.

The mesos-site.xml file is used to configure the hdfs-mesos framework.  We are working to deprecated this file.  This general establishes
values for the scheduler and in many cases these are passed to the executors.  Although the configuration of the scheduler can be handled
via XML configuration, we encourage the use of system environment variables for this purpose.

## Configuration Options

* mesos.hdfs.framework.name -  Used to define the framework name.  This allows for 1) multi-deployments of hdfs and 2) has an impact on the dns name of the service.  The default is "hdfs".
* mesos.hdfs.user - Used to define the user to use for the scheduler and executor processes.  The default is root.
* mesos.hdfs.role - Used to determine the mesos role this framework will use.  The default is "*".
* mesos.hdfs.mesosdns - true if mesos-dns is used.  The default is false.
* mesos.hdfs.mesosdns.domain - When using mesos-dns, this value is the suffix used by mesos-dns.  The default is "mesos".
* mesos.native.library - The location of libmesos library.  The default is "/usr/local/lib/libmesos.so"
* mesos.hdfs.journalnode.count - The number of journal nodes the scheduler will maintain. The default is 3.
* mesos.hdfs.data.dir -  The location to store data on the slaves.  The default is "/var/lib/hdfs/data".
* mesos.hdfs.domain.socket.dir - The location used for a local socket used by the data nodes.  The default is "/var/run/hadoop-hdfs".
* mesos.hdfs.backup.dir - The location to replicated data to as a backup.  The default is blank.
* mesos.hdfs.native-hadoop-binaries -  This is true if hdfs is pre-installed on the slaves.  This will result in no distribution of binaries to the slaves.  It will also mean that no xml configure refresh will be provided to the slaves.  The default is false.
* mesos.hdfs.framework.mnt.path - If native-hadoop-binaries == false, this is the location a symlink will be provided to execute hdfs commands on the slave.  The default is "/opt/mesosphere"
* mesos.hdfs.state.zk - The zookeeper that the scheduler will use to store state.  The default is "localhost:2181"
* mesos.master.uri - The zookeeper or mesos-master url that will be used to discover the mesos-master for scheduler registration. The default is "localhost:2181"
* mesos.hdfs.zkfc.ha.zookeeper.quorum - The zookeeper that HDFS (not the framework) will use for HA mode.  The default is "localhost:2181"

There are additional configurations for executor jvm and resource management of the nodes.

## System Environment Variables

All of the configuration flags previously defined can be overriden with system environment variables.  The format to use to override a variable is to
upper case the string and replace dots (".") with underscores ("_").  For example, to override the `mesos.hdfs.framework.name`, the value is `MESOS_HDFS_FRAMEWORK_NAME=unicorn`.
To use this value, export the value, then start the scheduler.  If a value is overridden by the system environment variable it will be propagated to
the executors.

## Custom Configurations

### Mesos-DNS custom configuration
You can see an example configuration in the `example-conf/dcos` directory. Since Mesos-DNS provides native bindings for master detection, we can simply use those names in our mesos and hdfs configurations. The example configuration assumes your Mesos masters and your zookeeper nodes are collocated. If they aren't you'll need to specify your zookeeper nodes separately. Also, note that if you are using the example in `example-conf/dcos`, the `mesos.hdfs.native-hadoop-binaries` property needs to be set to `false` if your HDFS binaries are not predistributed.

### If you have Hadoop pre-installed in your cluster
If you have Hadoop installed across your cluster, you don't need the Mesos scheduler application to distribute the binaries. You can set the `mesos.hdfs.native-hadoop-binaries` configuration parameter in `mesos-site.xml` if you don't want the binaries distributed.
