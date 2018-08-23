/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.ycld.logit.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.fasterxml.jackson.databind.ObjectMapper;

public class LogitClient {
  private static ObjectMapper mapper = new ObjectMapper();
  private static String DEFAULT_ENDPOINT = System.getProperty("log.endpoint",
      "http://127.0.0.1:9697/log");
  private static DateTimeFormatter format = ISODateTimeFormat.basicDateTime().withZone(
      DateTimeZone.UTC);
  private final String endpoint;

  public LogitClient() {
    this(DEFAULT_ENDPOINT);
  }

  public LogitClient(String endpoint) {
    this.endpoint = endpoint;
  }

  public void append(String service, DateTime timestamp, String uuid, String hexTag, String hexRef,
      int lat, Map<String, Object> data, Map<String, Object> arch) throws Exception {
    String ts = formatDate(timestamp);
    String id = uuid.toString();
    byte[] dataBytes = mapper.writeValueAsBytes(Arrays.asList(data, arch));

    String result =
        postContentToUrl(getAppendUrl(service, ts, id, hexTag, hexRef, lat), "application/json",
            dataBytes);

    if (!result.trim().equals("OK")) {
      throw new Exception("Unable to post log record");
    }
  }

  public Scanner getLogEntries(String service, DateTime t1, DateTime t2, Long limit, Long offset,
      String hexTag, String hexRef) throws Exception {
    String ts1 = formatDate(t1);
    String ts2 = formatDate(t2);

    return getUrlContentsAsScanner(getLogUrl(service, ts1, ts2, limit, offset, hexTag, hexRef));
  }

  public Scanner getCountEntries(String service, DateTime t1, DateTime t2, String hexTag,
      String hexRef) throws Exception {
    String ts1 = formatDate(t1);
    String ts2 = formatDate(t2);

    return getUrlContentsAsScanner(getCountsUrl(service, ts1, ts2, hexTag, hexRef));
  }

  public static Scanner getUrlContentsAsScanner(String urlString) throws MalformedURLException,
      IOException {
    URL url = new URL(urlString);

    return new Scanner(url.openStream(), "UTF-8");
  }

  private String getAppendUrl(String service, String timestamp, String uuid, String hexTag,
      String hexRef, int lat) {
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(endpoint);
    urlBuilder.append("/");
    urlBuilder.append(service);
    urlBuilder.append(";ts=");
    urlBuilder.append(timestamp);
    urlBuilder.append(";id=");
    urlBuilder.append(uuid);
    urlBuilder.append(";lat=");
    urlBuilder.append(lat);

    if (hexTag != null && hexTag.length() > 0) {
      urlBuilder.append(";tag=");
      urlBuilder.append(hexTag);
    }

    if (hexRef != null && hexRef.length() > 0) {
      urlBuilder.append(";ref=");
      urlBuilder.append(hexRef);
    }

    return urlBuilder.toString();
  }

  private String getLogUrl(String service, String ts1, String ts2, Long limit, Long offset,
      String hexTag, String hexRef) {
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(endpoint);
    urlBuilder.append("/");
    urlBuilder.append(service);
    urlBuilder.append("?t1=");
    urlBuilder.append(ts1);
    urlBuilder.append("&t2=");
    urlBuilder.append(ts2);

    if (hexTag != null && hexTag.length() > 0) {
      urlBuilder.append("&tagPrefix=");
      urlBuilder.append(hexTag);
    }

    if (hexRef != null && hexRef.length() > 0) {
      urlBuilder.append("&refPrefix=");
      urlBuilder.append(hexRef);
    }

    if (limit != null) {
      urlBuilder.append("&limit=");
      urlBuilder.append(limit);
    }

    if (offset != null) {
      urlBuilder.append("&offset=");
      urlBuilder.append(offset);
    }

    return urlBuilder.toString();
  }

  private String getCountsUrl(String service, String ts1, String ts2, String hexTag, String hexRef) {
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(endpoint);
    urlBuilder.append("/");
    urlBuilder.append(service);
    urlBuilder.append("/counts");
    urlBuilder.append("?t1=");
    urlBuilder.append(ts1);
    urlBuilder.append("&t2=");
    urlBuilder.append(ts2);

    if (hexTag != null && hexTag.length() > 0) {
      urlBuilder.append("&tagPrefix=");
      urlBuilder.append(hexTag);
    }

    if (hexRef != null && hexRef.length() > 0) {
      urlBuilder.append("&refPrefix=");
      urlBuilder.append(hexRef);
    }

    return urlBuilder.toString();
  }

  public static String postContentToUrl(String urlString, String contentType, byte[] content)
      throws Exception {
    HttpURLConnection url = (HttpURLConnection) new URL(urlString).openConnection();
    url.setRequestMethod("POST");
    url.setDoInput(true);
    url.setDoOutput(true);
    url.addRequestProperty("Content-Type", contentType);

    OutputStream urlout = null;
    try {
      urlout = url.getOutputStream();
      urlout.write(content);
    } finally {
      if (urlout != null) {
        urlout.close();
      }
    }

    if (url.getResponseCode() != 200) {
      throw new Exception("remote service exception");
    }

    return getStreamContentsAsString(url.getInputStream());
  }

  private static String getStreamContentsAsString(InputStream input) {
    StringBuilder outVal = new StringBuilder();
    try {
      Scanner scanner = new Scanner(input, "UTF-8");

      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        outVal.append(line);
        outVal.append("\n");
      }

      scanner.close();
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException ignored) {}
      }
    }

    return outVal.toString();
  }

  private static String formatDate(DateTime date) {
    return format.print(date);
  }
}
