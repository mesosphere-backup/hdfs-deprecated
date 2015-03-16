package org.apache.mesos.hdfs.config;

/**
 * Indicates an exception or poor request for configuration.
 */
public class ConfigurationException extends RuntimeException {

  public ConfigurationException() {
  }

  public ConfigurationException(String message) {
    super(message);
  }
}
