package org.apache.mesos.hdfs.storage;

import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.mesos.hdfs.SchedulerConfAcessor;
import org.apache.mesos.hdfs.TestConfigModule;
import org.apache.mesos.hdfs.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class GoogleStorageProviderTest {
  private GoogleStorageProvider googleStorageProvider;
  private String datadir;
  private SchedulerConfAcessor schedulerConfAcessor;

  @Before
  public void setUp() throws Exception {
    datadir = TestUtils.computeTestDataRoot(GoogleStorageProviderTest.class).getPath() + "/"
        + UUID.randomUUID().toString();
    new File(datadir).mkdirs();

    TestConfigModule testConfigModule = new TestConfigModule();
    testConfigModule.setCredentialsPath(Thread.currentThread().getContextClassLoader()
        .getResource("gcs-credentials.json").getPath());
    testConfigModule.setDataDir(datadir);

    Injector injector = Guice.createInjector(testConfigModule);
    googleStorageProvider = injector.getInstance(GoogleStorageProvider.class);
    schedulerConfAcessor = injector.getInstance(SchedulerConfAcessor.class);
  }

  @Test
  public void testStoreObject() throws Exception {
    Storage client = mock(Storage.class);

    FileOutputStream fileOutputStream = new FileOutputStream(datadir + "/test1");
    fileOutputStream.write("hello\n".getBytes());
    fileOutputStream.close();

    Storage.Objects.Insert insert = mock(Storage.Objects.Insert.class);
    Storage.Objects objects = mock(Storage.Objects.class);

    when(client.objects()).thenReturn(objects);

    String bucket = "test";
    when(objects.insert(eq(bucket), any(StorageObject.class), any(InputStreamContent.class)))
        .thenReturn(insert);

    GoogleStorageProvider.storeObject(client, schedulerConfAcessor.getSchedulerConf(), datadir
        + "/test1", "test/test1");

    verify(objects).insert(eq(bucket), any(StorageObject.class), any(InputStreamContent.class));
    verify(client).objects();
    // verify(insert).execute();
  }

  @Test
  public void testListStoredObjects() throws Exception {
    Storage client = mock(Storage.class);

    Storage.Objects objects = mock(Storage.Objects.class);
    when(client.objects()).thenReturn(objects);
    Storage.Objects.List list = mock(Storage.Objects.List.class);

    when(objects.list(eq("test/testprefix"))).thenReturn(list);

    com.google.api.services.storage.model.Objects objectsListing = new com.google.api.services.storage.model.Objects();
    List<StorageObject> listing = Arrays.asList(new StorageObject().setBucket("test").setName(
        "testprefix/test1"));
    objectsListing.setItems(listing);
    when(list.execute()).thenReturn(objectsListing).thenReturn(null);

    List<String> result = GoogleStorageProvider.listStoredObjects(client,
        schedulerConfAcessor.getSchedulerConf());

    assertEquals("test1", result.get(0));

    verify(objects).list(eq("test/testprefix"));
    verify(client).objects();
  }

  @Test
  public void testRetrieveObject() throws Exception {
    Storage client = mock(Storage.class);

    Storage.Objects objects = mock(Storage.Objects.class);
    when(client.objects()).thenReturn(objects);
    Storage.Objects.Get get = mock(Storage.Objects.Get.class);

    when(objects.get(eq("test/testprefix"), eq("test1"))).thenReturn(get);

    GoogleStorageProvider.retrieveObject(client, schedulerConfAcessor.getSchedulerConf(), "test1",
        "test1");

    verify(get).executeMediaAndDownloadTo(any(BufferedOutputStream.class));
  }
}
