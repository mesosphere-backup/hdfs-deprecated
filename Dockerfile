FROM java:7-jdk

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E56151BF && \
    echo "deb http://repos.mesosphere.io/ubuntu trusty main" | tee /etc/apt/sources.list.d/mesosphere.list && \
    apt-get update && \
    apt-get install --no-install-recommends -y --force-yes mesos=0.21.1-1.1.ubuntu1404

ADD build/hdfs-mesos-0.1.3.tgz /hdfs/build/

WORKDIR /hdfs/build/hdfs-mesos-0.1.3

ENTRYPOINT ["/bin/bash", "-c", "echo 'Arguments: ' $0 $*; python bin/hdfs-start.py --mesos.conf.path=etc/hadoop/mesos-site.xml $0 $*"]

