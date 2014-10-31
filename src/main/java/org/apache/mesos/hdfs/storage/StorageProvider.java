package org.apache.mesos.hdfs.storage;

import java.io.IOException;
import java.util.List;

public interface StorageProvider {
  public void storeObject(String sourcePath, String destinationObject) throws IOException;

  public List<String> listStoredObjects() throws IOException;

  public void retrieveObject(String object, String destinationPath) throws IOException;
}
