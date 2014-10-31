package org.apache.mesos.hdfs;

import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.storage.StorageProvider;
import org.apache.mesos.hdfs.util.PathUtil;

import java.io.*;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

class BackupService {
  public static final Log log = LogFactory.getLog(BackupService.class);
  protected final StorageProvider storageProvider;
  private final String archive = "namenode.zip";
  protected SchedulerConf schedulerConf;

  @Inject
  BackupService(StorageProvider storageProvider, SchedulerConf schedulerConf) {
    this.storageProvider = storageProvider;
    this.schedulerConf = schedulerConf;
  }

  void addPathsToZip(ZipOutputStream zos, String path) throws IOException {
    File start = new File(path);
    File[] listing = start.listFiles();
    if (listing == null)
      return;

    for (File file : listing) {
      if (!file.isDirectory()) {
        log.debug("Should be adding " + file.getName() + " at path " + file.getPath());
        if (file.getName().endsWith(".lock")) {
          // skip lock files
          continue;
        }
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(
            file.getPath()));

        String entryPath = file.getAbsolutePath().replaceFirst(schedulerConf.getDataDir(), "")
            .replaceFirst("^/", ""); // remove leading '/', if needed
        log.debug("Entry path is: " + entryPath);

        ZipEntry zipEntry = new ZipEntry(entryPath);
        zipEntry.setTime(file.lastModified());
        zos.putNextEntry(zipEntry);

        IOUtils.copy(bufferedInputStream, zos);

        zos.closeEntry();
      } else {
        addPathsToZip(zos, file.getPath());
      }
    }
  }

  boolean checkForBackup() {
    try {
      List<String> storedObjects = storageProvider.listStoredObjects();

      return storedObjects.contains(archive);
    } catch (IOException e) {
      log.error("Caught exception", e);
      return false;
    }
  }

  boolean retrieveArchive() {
    try {
      File path = new File(PathUtil.combine("tmp/retrieve", archive));
      path.getParentFile().mkdirs();

      storageProvider.retrieveObject(archive, path.getAbsolutePath());

      restoreArchive(path.getAbsolutePath());

      return true;
    } catch (IOException e) {
      log.error("Caught exception", e);
      return false;
    }
  }

  boolean createAndStoreArchive() {
    try {
      File path = new File(PathUtil.combine("tmp/store", archive));
      path.getParentFile().mkdirs();

      generateArchive(path.getAbsolutePath());

      storageProvider.storeObject(path.getAbsolutePath(), archive);
      return true;
    } catch (IOException e) {
      log.error("Caught exception", e);
      return false;
    }
  }

  void generateArchive(String filename) {
    try {
      BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(
          filename));

      ZipOutputStream zipOutputStream = new ZipOutputStream(bufferedOutputStream);

      addPathsToZip(zipOutputStream, PathUtil.combine(schedulerConf.getDataDir(), "name"));

      zipOutputStream.close();
      bufferedOutputStream.close();
    } catch (IOException e) {
      log.error("Caught exception", e);
    }
  }

  void restoreArchive(String filename) throws IOException {
    BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(filename));

    ZipInputStream zipInputStream = new ZipInputStream(bufferedInputStream);

    ZipEntry zipEntry = zipInputStream.getNextEntry();

    while (zipEntry != null) {
      File output = new File(PathUtil.combine(schedulerConf.getDataDir(), zipEntry.getName()));
      log.debug("Restoring entry " + output.getPath());
      log.debug("Parent is " + output.getParent());
      new File(output.getParent()).mkdirs();

      BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(
          output));

      IOUtils.copy(zipInputStream, bufferedOutputStream);

      bufferedOutputStream.close();
      output.setLastModified(zipEntry.getTime());

      zipEntry = zipInputStream.getNextEntry();
    }
  }
}
