package org.apache.mesos.hdfs;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.mesos.hdfs.util.PathUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BackupServiceTest {
  static private String datadir;
  private BackupService backupService;

  @Before
  public void setUp() throws Exception {
    datadir = TestUtils.computeTestDataRoot(BackupServiceTest.class).getPath() + "/"
        + UUID.randomUUID().toString();
    String[] paths = {"name/current/fsimage", "name/current/fstime", "name/current/VERSION",
        "name/current/edits", "name/image/fsimage"};

    for (String path : paths) {
      File file = new File(PathUtil.combine(datadir, path));
      new File(file.getParent()).mkdirs();

      FileOutputStream fileOutputStream = new FileOutputStream(file);
      fileOutputStream.write((path + "\n").getBytes());
      fileOutputStream.write((UUID.randomUUID().toString() + "\n").getBytes());
      fileOutputStream.close();
    }

    TestConfigModule testConfigModule = new TestConfigModule();
    testConfigModule.setDataDir(datadir);

    Injector injector = Guice.createInjector(testConfigModule);

    backupService = injector.getInstance(BackupService.class);
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteQuietly(new File(datadir));
  }

  @Test
  public void verifyCorrectness() throws IOException {
    // Generate the same archive twice, verify the contents
    String archive1 = datadir + "/archive1.zip";
    String archive2 = datadir + "/archive2.zip";
    backupService.generateArchive(archive1);
    backupService.generateArchive(archive2);

    String fsimageContents1 = Files.readAllLines(
        Paths.get(PathUtil.combine(datadir, "/name/current", "fsimage")), StandardCharsets.UTF_8)
        .toString();

    FileInputStream fis = new FileInputStream(new File(archive1));
    String archive1_md5 = DigestUtils.md5Hex(fis);
    fis.close();

    fis = new FileInputStream(new File(archive2));
    String archive2_md5 = DigestUtils.md5Hex(fis);
    fis.close();

    assertEquals(archive1_md5, archive2_md5);

    // Wipe
    FileUtils.deleteQuietly(new File(PathUtil.combine(datadir, "name")));

    // Extract
    backupService.restoreArchive(archive1);

    String fsimageContents2 = Files.readAllLines(
        Paths.get(PathUtil.combine(datadir, "/name/current", "fsimage")), StandardCharsets.UTF_8)
        .toString();

    assertEquals(fsimageContents1, fsimageContents2);

    assertTrue(new File(PathUtil.combine(datadir, "/name/current/fsimage")).isFile());
  }
}
