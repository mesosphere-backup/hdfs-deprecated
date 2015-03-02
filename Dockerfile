FROM dockerfile/java:openjdk-7-jre

ENV HDFS_MESOS_VERSION 0.0.2

ENV MESOS_NATIVE_JAVA_LIBRARY /usr/local/lib/libmesos.so
ENV MESOS_NATIVE_LIBRARY /usr/local/lib/libmesos.so

RUN echo "deb http://repos.mesosphere.io/ubuntu/ trusty main" > /etc/apt/sources.list.d/mesosphere.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF && \
    apt-get update && \
    apt-get install -y mesos

COPY hdfs-mesos-$HDFS_MESOS_VERSION.tgz /
ADD hdfs-mesos-$HDFS_MESOS_VERSION.tgz /

WORKDIR /hdfs-mesos-$HDFS_MESOS_VERSION

CMD ./bin/hdfs-mesos