## Conf directory readme

It is necessary to copy configuration xml files into the hadoop distribution for hdfs to work.   The default (and the original) xml files are under the `mesos` sub directory.   This is for a standard Mesos environment such as Google Elastic Mesos.

The configuration files under the `dcos` subdirectory are for dcos and have references that expect mesos-dns to be present.

## Building for your env

The build process by default builds the standard mesos build.   If you would like to build the dcos, then pass "dcos" as the first or second parameter.  

* `./bin/build-hdfs` - builds mesos version
* `./bin/build-hdfs dcos` - builds dcos version
* `./bin/build-hdfs nocompile dcos` - builds a dcos version


