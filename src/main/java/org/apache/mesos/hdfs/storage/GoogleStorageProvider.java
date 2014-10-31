package org.apache.mesos.hdfs.storage;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.api.services.storage.model.StorageObject;
import com.google.inject.Inject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.util.PathUtil;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GoogleStorageProvider implements StorageProvider {
  public static final Log log = LogFactory.getLog(GoogleStorageProvider.class);
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static HttpTransport httpTransport;
  private static Storage client;
  protected SchedulerConf schedulerConf;

  @Inject
  GoogleStorageProvider(SchedulerConf schedulerConf) throws Exception {
    this.schedulerConf = schedulerConf;

    if (httpTransport == null) {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    }

    if (client == null) {
      Credential credential = authorize();

      // Set up global Storage instance.
      client = new Storage.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName(
          "hdfs-on-mesos").build();
    }
  }

  static public void storeObject(Storage client, SchedulerConf schedulerConf, String sourcePath,
      String destinationObject) throws IOException {
    File input = new File(sourcePath);
    BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(input));

    StorageObject object = new StorageObject().setBucket(schedulerConf.getStorageBucket()).setName(
        PathUtil.combine(schedulerConf.getStoragePrefix(), destinationObject));

    InputStreamContent mediaContent = new InputStreamContent("application/octet-stream",
        bufferedInputStream);
    mediaContent.setLength(input.length());

    Storage.Objects.Insert insert = client.objects().insert(schedulerConf.getStorageBucket(),
        object, mediaContent);

    insert.execute();
  }

  static public List<String> listStoredObjects(Storage client, SchedulerConf schedulerConf)
      throws IOException {
    // List the contents of the bucket.
    Storage.Objects.List listObjects = client.objects().list(
        PathUtil.combine(schedulerConf.getStorageBucket(), schedulerConf.getStoragePrefix()));

    com.google.api.services.storage.model.Objects objects;
    List<String> result = new ArrayList<>();
    do {
      objects = listObjects.execute();
      List<StorageObject> items = objects.getItems();
      if (null == items) {
        break;
      }
      for (StorageObject object : items) {
        if (!object.getName().startsWith(schedulerConf.getStoragePrefix())) {
          continue;
        }
        result.add(object.getName().replaceFirst(schedulerConf.getStoragePrefix(), "")
            .replaceFirst("^/", ""));
      }
      listObjects.setPageToken(objects.getNextPageToken());
    } while (null != objects.getNextPageToken());

    return result;
  }

  static public void retrieveObject(Storage client, SchedulerConf schedulerConf, String object,
      String destinationPath) throws IOException {
    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(
        destinationPath));

    Storage.Objects.Get get = client.objects().get(
        PathUtil.combine(schedulerConf.getStorageBucket(), schedulerConf.getStoragePrefix()),
        object);

    get.executeMediaAndDownloadTo(bufferedOutputStream);
  }

  private Credential authorize() throws Exception {
    // Load client secrets.
    log.info(schedulerConf.getStorageCredentialsPath());
    ObjectMapper mapper = new ObjectMapper();
    GoogleCredentials credentials = mapper.readValue(
        new File(schedulerConf.getStorageCredentialsPath()), GoogleCredentials.class);

    File pem = File.createTempFile("lol", "pem");
    FileOutputStream fileOutputStream = new FileOutputStream(pem);
    fileOutputStream.write(credentials.private_key.getBytes());
    fileOutputStream.close();
    pem.deleteOnExit();

    Set<String> scopes = new HashSet<>();
    scopes.add(StorageScopes.DEVSTORAGE_FULL_CONTROL);
    scopes.add(StorageScopes.DEVSTORAGE_READ_ONLY);
    scopes.add(StorageScopes.DEVSTORAGE_READ_WRITE);

    return new GoogleCredential.Builder().setTransport(httpTransport).setJsonFactory(JSON_FACTORY)
        .setServiceAccountId(credentials.client_email)
        .setServiceAccountPrivateKeyId(credentials.private_key_id)
        .setServiceAccountPrivateKeyFromPemFile(pem.getAbsoluteFile())
        .setServiceAccountScopes(scopes).build();
  }

  public void storeObject(String sourcePath, String destinationObject) throws IOException {
    storeObject(client, schedulerConf, sourcePath, destinationObject);
  }

  public List<String> listStoredObjects() throws IOException {
    return listStoredObjects(client, schedulerConf);
  }

  public void retrieveObject(String object, String destinationPath) throws IOException {
    retrieveObject(client, schedulerConf, object, destinationPath);
  }
}
