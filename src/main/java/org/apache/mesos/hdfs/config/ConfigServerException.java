package org.apache.mesos.hdfs.config;

/**
 * Indicates a failure to startup the config service, likely a jetty failure
 */
public class ConfigServerException extends RuntimeException {

  private String message;

  public ConfigServerException(String msg) {
    this.message = msg;
  }

  public ConfigServerException() {
    this("");
  }
}
