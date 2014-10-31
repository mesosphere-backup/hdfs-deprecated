package org.apache.mesos.hdfs.storage;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.mesos.hdfs.TestConfigModule;
import org.apache.mesos.hdfs.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

public class S3StorageProviderTest {
  private S3StorageProvider s3StorageProvider;
  private String datadir;

  @Before
  public void setUp() throws Exception {

    TestConfigModule testConfigModule = new TestConfigModule();
    testConfigModule.setCredentialsPath(Thread.currentThread().getContextClassLoader()
        .getResource("s3-credentials.json").getPath());
    testConfigModule.setDataDir(TestUtils.computeTestDataRoot(S3StorageProviderTest.class)
        .getPath() + "/" + UUID.randomUUID().toString());

    Injector injector = Guice.createInjector(testConfigModule);
    s3StorageProvider = injector.getInstance(S3StorageProvider.class);
  }

  @Test
  public void testStoreObject() throws Exception {

  }

  @Test
  public void testListStoredObjects() throws Exception {

  }

  @Test
  public void testRetrieveObject() throws Exception {

  }
}
