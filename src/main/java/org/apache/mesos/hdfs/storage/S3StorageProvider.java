package org.apache.mesos.hdfs.storage;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hdfs.config.SchedulerConf;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class S3StorageProvider implements StorageProvider {
  public static final Log log = LogFactory.getLog(S3StorageProvider.class);
  protected SchedulerConf schedulerConf;
  private AmazonS3Client s3Client;

  @Inject
  S3StorageProvider(SchedulerConf schedulerConf) throws Exception {
    this.schedulerConf = schedulerConf;

    ObjectMapper mapper = new ObjectMapper();
    S3Credentials s3Credentials = mapper.readValue(
        new File(schedulerConf.getStorageCredentialsPath()), S3Credentials.class);

    s3Client = new AmazonS3Client(new BasicAWSCredentials(s3Credentials.accessKey,
        s3Credentials.secretKey));
  }

  static public void storeObject(AmazonS3Client s3Client, SchedulerConf schedulerConf,
      String sourcePath, String destinationObject) throws IOException {
    final String existingBucketName = schedulerConf.getStorageBucket();
    final String keyName = schedulerConf.getStoragePrefix() + destinationObject;
    List<PartETag> partETags = new ArrayList<>();

    InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(
        existingBucketName, keyName);
    InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

    File file = new File(sourcePath);
    final long contentLength = file.length();
    final long partSize = 5242880; // Set part size to 5 MB.

    try {
      long filePosition = 0;
      for (int i = 1; filePosition < contentLength; i++) {
        // Last part can be less than 5 MB. Adjust part size.
        long bytesToRead = Math.min(partSize, (contentLength - filePosition));

        // Create request to upload a part.
        UploadPartRequest uploadRequest = new UploadPartRequest()
            .withBucketName(existingBucketName).withKey(keyName)
            .withUploadId(initResponse.getUploadId()).withPartNumber(i)
            .withFileOffset(filePosition).withFile(file).withPartSize(bytesToRead);

        // Upload part and add response to our list.
        partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());

        filePosition += partSize;
      }

      CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
          existingBucketName, keyName, initResponse.getUploadId(), partETags);

      s3Client.completeMultipartUpload(compRequest);
    } catch (Exception e) {
      log.error(e);
      s3Client.abortMultipartUpload(new AbortMultipartUploadRequest(existingBucketName, keyName,
          initResponse.getUploadId()));
    }

  }

  static public List<String> listStoredObjects(AmazonS3Client s3Client, SchedulerConf schedulerConf)
      throws IOException {
    List<String> result = new ArrayList<>();
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(
        schedulerConf.getStorageBucket()).withPrefix(schedulerConf.getStoragePrefix());
    ObjectListing objectListing;

    do {
      objectListing = s3Client.listObjects(listObjectsRequest);

      List<S3ObjectSummary> summaries = objectListing.getObjectSummaries();
      for (S3ObjectSummary summary : summaries) {
        final String key = summary.getKey();
        int endIndex = key.lastIndexOf(schedulerConf.getStoragePrefix());
        result.add(key.substring(endIndex, key.length()));
      }
      listObjectsRequest.setMarker(objectListing.getNextMarker());
    } while (objectListing.isTruncated());

    return result;
  }

  public void storeObject(String sourcePath, String destinationObject) throws IOException {
    storeObject(s3Client, schedulerConf, sourcePath, destinationObject);
  }

  public List<String> listStoredObjects() throws IOException {
    return listStoredObjects(s3Client, schedulerConf);
  }

  public void retrieveObject(String object, String destinationPath) throws IOException {
    retrieveObject(s3Client, schedulerConf, object, destinationPath);
  }

  public void retrieveObject(AmazonS3Client s3Client, SchedulerConf schedulerConf, String object,
      String destinationPath) throws IOException {
    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(
        destinationPath));
    try {
      final String bucketName = schedulerConf.getStorageBucket();
      final String key = schedulerConf.getStoragePrefix() + object;

      S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, key));

      final long contentLength = s3object.getObjectMetadata().getContentLength();
      final long partSize = 5242880; // Set part size to 5 MB.
      long bytesRead = 0;
      while (bytesRead < contentLength) {
        GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, key);

        long bytesToRead = Math.min(bytesRead + partSize, contentLength);

        rangeObjectRequest.setRange(bytesRead, bytesToRead);
        S3Object objectPortion = s3Client.getObject(rangeObjectRequest);
        IOUtils.copy(objectPortion.getObjectContent(), bufferedOutputStream);
        bytesRead = bytesToRead;
      }

    } catch (AmazonClientException ase) {
      log.error(ase);
    }
  }
}
