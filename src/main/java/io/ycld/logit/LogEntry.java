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
package io.ycld.logit;

import io.ycld.logit.lib.utils.Hex;

import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

public class LogEntry {
  private final String service;
  private final DateTime timestamp;
  private final String id;
  private final String hexTag;
  private final String hexRef;
  private final byte[] data;
  private final byte[] arch;
  private final int lat;

  public LogEntry(@JsonProperty("service") String service,
      @JsonProperty("timestamp") DateTime timestamp, @JsonProperty("id") String id,
      @JsonProperty("hexTag") String hexTag, @JsonProperty("hexRef") String hexRef,
      @JsonProperty("lat") int lat, @JsonProperty("data") byte[] data,
      @JsonProperty("arch") byte[] arch) {
    this.service = service;
    this.timestamp = timestamp;
    this.id = id;
    this.hexTag = hexTag == null ? "" : hexTag;
    this.hexRef = hexRef == null ? "" : hexRef;
    this.lat = lat;
    this.data = data;
    this.arch = arch;
  }

  public String getService() {
    return service;
  }

  public DateTime getTimestamp() {
    return timestamp;
  }

  public String getId() {
    return id;
  }

  public byte[] getData() {
    return data;
  }

  public byte[] getArch() {
    return arch;
  }

  public String getHexTag() {
    return hexTag;
  }

  public String getHexRef() {
    return hexRef;
  }

  public int getLat() {
    return lat;
  }

  @Override
  public String toString() {
    return asDbRow().toString();
  }

  @JsonIgnore
  public Map<String, Object> asDbRow() {
    try {
      DateTime ts = timestamp.withZone(DateTimeZone.UTC);

      byte[] uuid = Hex.decodeHex(id.replace("-", "").toCharArray());
      byte[] tag = Hex.decodeHex(hexTag.toCharArray());
      byte[] ref = Hex.decodeHex(hexRef.toCharArray());

      return ImmutableMap.<String, Object>builder().put(JdbcWriter.TIMESTAMP_COL, ts.getMillis())
          .put(JdbcWriter.ID_COL, uuid).put(JdbcWriter.TAG_COL, tag).put(JdbcWriter.REF_COL, ref)
          .put(JdbcWriter.LAT_COL, lat).put(JdbcWriter.DATA_COL, data)
          .put(JdbcWriter.ARCH_COL, arch).build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
