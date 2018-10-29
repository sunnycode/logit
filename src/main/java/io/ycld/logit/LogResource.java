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

import io.ycld.logit.lib.utils.JsonUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.MatrixParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import com.google.inject.Inject;
import com.google.inject.name.Named;

@Path("/log")
public class LogResource {
  @Inject
  @Named("external.datetimeformat")
  private DateTimeFormatter format;

  @Inject
  private JdbcWriterAurora logger;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{service}/counts")
  public Response getCountEntries(final @PathParam("service") String service,
      final @QueryParam("t1") String t1, final @QueryParam("t2") String t2,
      final @QueryParam("tagPrefix") String tagPrefix,
      final @QueryParam("refPrefix") String refPrefix) {
    return Response.status(Status.OK).entity(new StreamingOutput() {
      @Override
      public void write(OutputStream output) throws IOException, WebApplicationException {
        try {
          logger.getCounterEntries(service, format.parseDateTime(t1), format.parseDateTime(t2),
              tagPrefix, refPrefix, output);
        } finally {
          try {
            output.close();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }).build();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{service}")
  public Response getLogEntries(final @PathParam("service") String service,
      final @QueryParam("t1") String t1, final @QueryParam("t2") String t2,
      final @QueryParam("tagPrefix") String tagPrefix,
      final @QueryParam("refPrefix") String refPrefix, final @QueryParam("limit") Long limit,
      final @QueryParam("offset") Long offset, final @QueryParam("reverse") boolean reverse) {
    return Response.status(Status.OK).entity(new StreamingOutput() {
      @Override
      public void write(OutputStream output) throws IOException, WebApplicationException {
        try {
          logger.getLogEntries(service, format.parseDateTime(t1), format.parseDateTime(t2),
              tagPrefix, refPrefix, limit, offset, output, reverse);
        } finally {
          try {
            output.close();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }).build();
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/{service}")
  public Response postLogEntry(@PathParam("service") final String service,
      @MatrixParam("ts") final String timestampString, @MatrixParam("id") final String id,
      @MatrixParam("tag") final String hexTag, @MatrixParam("ref") final String hexRef,
      @MatrixParam("lat") final int lat, byte[] data) {
    try {
      Map<String, Object> dataMap = null;
      Map<String, Object> archMap = null;

      Object inVal = JsonUtils.fromJson(Object.class, data);

      if (inVal instanceof Map) {
        dataMap = (Map<String, Object>) inVal;
        archMap = Collections.emptyMap();
      } else if (inVal instanceof List) {
        dataMap = (Map<String, Object>) ((List) inVal).get(0);
        archMap = (Map<String, Object>) ((List) inVal).get(1);
      }

      byte[] dSmileData = JsonUtils.asSmile(dataMap);
      byte[] aSmileData = JsonUtils.asSmile(archMap);

      DateTime timestamp = format.parseDateTime(timestampString);

      logger.append(new LogEntry(service, timestamp, id, hexTag, hexRef, lat, dSmileData,
          aSmileData));

      return Response.status(Status.OK).entity("OK").build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
