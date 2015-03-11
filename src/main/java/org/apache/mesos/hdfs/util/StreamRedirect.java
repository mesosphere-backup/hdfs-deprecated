package org.apache.mesos.hdfs.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

public class StreamRedirect extends Thread {
  public static final Log log = LogFactory.getLog(StreamRedirect.class);
  InputStream stream;
  PrintStream outputStream;

  public StreamRedirect(InputStream stream, PrintStream outputStream) {
    this.stream = stream;
    this.outputStream = outputStream;
  }

  public void run() {
    try {
      InputStreamReader streamReader = new InputStreamReader(stream);
      BufferedReader streamBuffer = new BufferedReader(streamReader);

      String streamLine = null;
      while ((streamLine = streamBuffer.readLine()) != null) {
        outputStream.println(streamLine);
      }
    } catch (IOException ioe) {
      log.error("stream redirect error", ioe);
    }
  }
}
