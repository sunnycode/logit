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
import io.ycld.logit.lib.utils.JsonUtils;
import io.ycld.logit.lib.utils.UuidUtils;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.Update;
import org.skife.jdbi.v2.tweak.HandleCallback;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class JdbcWriterAurora {
  private static final String TABLE_SPACE = System.getProperty("jdbc.tablespace", "logit_service");

  public static final String TIMESTAMP_COL = "ts";
  public static final String ID_COL = "id";
  public static final String TAG_COL = "tag";
  public static final String REF_COL = "ref";
  public static final String LAT_COL = "lat";
  public static final String DATA_COL = "d";
  public static final String ARCH_COL = "a";

  private Cache<String, Boolean> exists = CacheBuilder.newBuilder()
      .expireAfterWrite(2, TimeUnit.HOURS).build();
  private DateTimeFormatter format = DateTimeFormat.forPattern("yyyyMMddHH").withZone(
      DateTimeZone.UTC);
  private DateTimeFormatter outFormat = ISODateTimeFormat.basicDateTime()
      .withZone(DateTimeZone.UTC);

  private final IDBI db;

  @Inject
  public JdbcWriterAurora(IDBI db) {
    this.db = db;
  }

  public void append(LogEntry entry) {
    try {
      doAppend(entry);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void truncate(final String service, final DateTime timestamp) {
    db.withHandle(new HandleCallback<Void>() {
      @Override
      public Void withHandle(Handle handle) throws Exception {
        String partition = getPartition(service, timestamp);

        ensureExists(partition, getCreateTable(partition));
        handle.createStatement("truncate table " + partition).execute();

        return null;
      }
    });
  }

  public void ping() throws Exception {
    db.withHandle(new HandleCallback<Void>() {
      @Override
      public Void withHandle(Handle handle) throws Exception {
        handle.createStatement("select 1").execute();

        return null;
      }
    });
  }

  private void ensureExists(final String partition, final String createStatement) {
    Boolean alreadyThere = exists.getIfPresent(partition);

    if (alreadyThere != null && alreadyThere) {
      return;
    }

    db.withHandle(new HandleCallback<Void>() {
      @Override
      public Void withHandle(Handle handle) throws Exception {
        handle.createStatement(createStatement).execute();
        return null;
      }
    });

    exists.put(partition, Boolean.TRUE);
  }

  private void doAppend(final LogEntry entry) throws Exception {
    long t1 = System.currentTimeMillis();

    final Map<String, Object> dbRow;

    try {
      dbRow = entry.asDbRow();
    } catch (Exception e) {
      System.out.println("IGNORE: " + (entry != null ? entry.toString() : "null"));
      return;
    }

    db.inTransaction(new TransactionCallback<Void>() {
      @Override
      public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
        try {
          String partition = getPartition(entry.getService(), entry.getTimestamp());
          String countsPartition = getCountsPartition(entry.getService());

          //
          // make sure table and counts table exist
          //
          ensureExists(partition, getCreateTable(partition));
          ensureExists(countsPartition, getCreateCountsTable(countsPartition));

          boolean successful = false;
          int inserted = 0;

          try {
            //
            // insert primary log row
            //
            Update insert =
                handle.createStatement("insert into " + partition
                    + "(ts, id, tag, ref, lat, d, a) values (?, ?, ?, ?, ?, ?, ?)");

            insert.bind(0, dbRow.get(TIMESTAMP_COL));
            insert.bind(1, dbRow.get(ID_COL));
            insert.bind(2, dbRow.get(TAG_COL));
            insert.bind(3, dbRow.get(REF_COL));
            insert.bind(4, dbRow.get(LAT_COL));
            insert.bind(5, dbRow.get(DATA_COL));
            insert.bind(6, dbRow.get(ARCH_COL));

            inserted = insert.execute();
            successful = (inserted == 1);
          } catch (Exception e) {
            if (e.getMessage().toLowerCase().contains("duplicate")) {
              successful = true;
              System.out.println(new DateTime() + " processed duplicate : " + entry.getId() + " @ "
                  + entry.getTimestamp());
            } else {
              e.printStackTrace();

              throw Throwables.propagate(e);
            }
          }

          if (inserted == 1 && entry.getHexTag().length() > 0) {
            long ts = entry.getTimestamp().withZone(DateTimeZone.UTC).getMillis();
            long tsTruncated =
                entry.getTimestamp().withMillisOfSecond(0).withSecondOfMinute(0)
                    .withMinuteOfHour(0).withZone(DateTimeZone.UTC).getMillis();

            //
            // update counts row
            //
            int updated = 0;

            {
              Update i3 =
                  handle
                      .createStatement("update "
                          + countsPartition
                          + " set ts_min = least(ts_min, ?), ts_max = greatest(ts_max, ?), c = c + 1, l_tot = l_tot + ?, l_sq_tot = l_sq_tot + ?, l_max = greatest(l_max, ?) "
                          + " where tag = ? and ts = ? and ref = ?");

              long lat = ((Number) dbRow.get(LAT_COL)).longValue();

              i3.bind(0, ts);
              i3.bind(1, ts);
              i3.bind(2, lat);
              i3.bind(3, lat * lat);
              i3.bind(4, lat);
              i3.bind(5, dbRow.get(TAG_COL));
              i3.bind(6, tsTruncated);
              i3.bind(7, dbRow.get(REF_COL));

              updated = i3.execute();
            }

            if (updated < 1) {
              //
              // insert counts row
              //
              Update i2 =
                  handle
                      .createStatement("insert ignore into "
                          + countsPartition
                          + "(tag, ts, ref, ts_min, ts_max, c, l_tot, l_sq_tot, l_max) values (?, ?, ?, ?, ?, 0, 0, 0, 0)");
              i2.bind(0, dbRow.get(TAG_COL));
              i2.bind(1, tsTruncated);
              i2.bind(2, dbRow.get(REF_COL));
              i2.bind(3, ts);
              i2.bind(4, ts);

              i2.execute();

              //
              // update counts row
              //
              Update i3 =
                  handle
                      .createStatement("update "
                          + countsPartition
                          + " set ts_min = least(ts_min, ?), ts_max = greatest(ts_max, ?), c = c + 1, l_tot = l_tot + ?, l_sq_tot = l_sq_tot + ?, l_max = greatest(l_max, ?) "
                          + " where tag = ? and ts = ? and ref = ?");

              long lat = ((Number) dbRow.get(LAT_COL)).longValue();

              i3.bind(0, ts);
              i3.bind(1, ts);
              i3.bind(2, lat);
              i3.bind(3, lat * lat);
              i3.bind(4, lat);
              i3.bind(5, dbRow.get(TAG_COL));
              i3.bind(6, tsTruncated);
              i3.bind(7, dbRow.get(REF_COL));

              updated = i3.execute();
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
          System.out.println("IGNORE: " + entry.getData());

          throw Throwables.propagate(e);
        }
        return null;
      }
    });

    long t2 = System.currentTimeMillis();

    System.out.println(new DateTime() + " wrote " + entry.getId() + " in " + (t2 - t1) + " ms");
  }

  public void getCounterEntries(final String service, final DateTime t1, final DateTime t2,
      final String hexTag, final String hexRef, final OutputStream output) {
    try {
      DateTime current = t1.withZone(DateTimeZone.UTC);
      DateTime finish = t2.withZone(DateTimeZone.UTC);

      final long currentMillis = current.getMillis();
      final long finishMillis = finish.getMillis();

      final PrintWriter out = new PrintWriter(output);
      try {
        final String partition = getCountsPartition(service);

        String query =
            "select lower(hex(tag)) as tag, ts, lower(hex(ref)) as ref, ts_min, ts_max, c, l_tot, l_sq_tot, l_max from "
                + partition + " where ";

        if (hexTag != null && hexTag.length() > 0) {
          query += " tag like binary concat(replace(unhex(?), ?, ?), '%') and ";

          if (hexRef != null && hexRef.length() > 0) {
            query += " ref like binary concat(replace(unhex(?), ?, ?), '%') and ";
          }
        }

        query += " ts >= ? and ts < ?";
        query += " order by tag, ts, ref";

        final String querySelect = query;

        db.withHandle(new HandleCallback<Void>() {
          public Void withHandle(Handle handle) throws Exception {
            try {
              if (!tableExists(handle, partition)) {
                return null;
              }

              Query<Map<String, Object>> query = handle.createQuery(querySelect);
              int i = 0;
              if (hexTag != null && hexTag.length() > 0) {
                query.bind(i++, hexTag);
                query.bind(i++, "\\");
                query.bind(i++, "\\\\");

                if (hexRef != null && hexRef.length() > 0) {
                  query.bind(i++, hexRef);
                  query.bind(i++, "\\");
                  query.bind(i++, "\\\\");
                }
              }

              query.bind(i++, currentMillis);
              query.bind(i++, finishMillis);

              for (Map<String, Object> entry : query) {
                LinkedHashMap<String, Object> ordered = new LinkedHashMap<String, Object>();
                ordered.put("tag", entry.get("tag"));
                ordered.put("ts", outFormat.print(((Number) entry.get("ts")).longValue()));
                // ordered.put("ref", entry.get("ref"));
                ordered.put("ts_min", outFormat.print(((Number) entry.get("ts_min")).longValue()));
                ordered.put("ts_max", outFormat.print(((Number) entry.get("ts_max")).longValue()));
                ordered.put("c", entry.get("c"));
                ordered.put("l_tot", entry.get("l_tot"));
                ordered.put("l_sq_tot", entry.get("l_sq_tot"));
                ordered.put("l_max", entry.get("l_max"));

                out.println(JsonUtils.asJson(ordered));
              }
            } catch (Exception e) {
              e.printStackTrace();
            } finally {
              out.flush();
            }

            return null;
          }
        });
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        out.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void getLogEntries(final String service, final DateTime t1, final DateTime t2,
      final String hexTag, final String hexRef, final Long limit, final Long offset,
      final OutputStream output, final boolean reverse) {
    try {
      DateTime current = (!reverse) ? t1.withZone(DateTimeZone.UTC) : t2.withZone(DateTimeZone.UTC);
      DateTime finish = (!reverse) ? t2.withZone(DateTimeZone.UTC) : t1.withZone(DateTimeZone.UTC);

      final long currentMillis = current.getMillis();
      final long finishMillis = finish.getMillis();

      final PrintWriter out = new PrintWriter(output);
      try {
        final AtomicLong toSkip = new AtomicLong((offset != null) ? offset.longValue() : 0);

        final AtomicLong toPrint =
            new AtomicLong((limit != null) ? limit.longValue() : Long.MAX_VALUE);

        while (((!reverse && current.isBefore(finish)) || (reverse && !current.isBefore(finish)))
            && toPrint.get() > 0) {
          final String partition = getPartition(service, current);
          final int hoursIncrement = (!reverse) ? 1 : -1;

          current = current.plusHours(hoursIncrement).withZone(DateTimeZone.UTC);

          String query = "select ts, id, tag, ref, lat, d, a from " + partition + " where ";

          if (hexTag != null && hexTag.length() > 0) {
            query += " tag like binary concat(replace(unhex(?), ?, ?), '%') and ";

            if (hexRef != null && hexRef.length() > 0) {
              query += " ref like binary concat(replace(unhex(?), ?, ?), '%') and ";
            }
          }

          if (!reverse) {
            query += " ts >= ? and ts < ? order by ts ";
          } else {
            query += " ts < ? and ts >= ? order by ts ";
          }

          query += (!reverse) ? "asc" : "desc";

          if ((offset == null || offset == 0L) && toPrint.get() == 1L) {
            query += " limit 1";
          }

          final String querySelect = query;

          db.withHandle(new HandleCallback<Void>() {
            public Void withHandle(Handle handle) throws Exception {
              try {
                if (!tableExists(handle, partition)) {
                  return null;
                }

                Query<Map<String, Object>> query = handle.createQuery(querySelect);
                int i = 0;
                if (hexTag != null && hexTag.length() > 0) {
                  query.bind(i++, hexTag);
                  query.bind(i++, "\\");
                  query.bind(i++, "\\\\");

                  if (hexRef != null && hexRef.length() > 0) {
                    query.bind(i++, hexRef);
                    query.bind(i++, "\\");
                    query.bind(i++, "\\\\");
                  }
                }
                query.bind(i++, currentMillis);
                query.bind(i++, finishMillis);

                for (Map<String, Object> entry : query) {
                  if (toPrint.get() < 1) {
                    break;
                  }

                  if (toSkip.get() > 0) {
                    toSkip.getAndDecrement();
                    continue;
                  }

                  StringBuilder builder = new StringBuilder();

                  builder.append("{\"ts\":\"");
                  builder.append(outFormat.print(((Number) entry.get("ts")).longValue()));
                  builder.append("\",\"id\":\"");
                  builder.append(UuidUtils.getUUID((byte[]) entry.get("id")));
                  builder.append("\",\"tag\":\"");
                  builder.append(new String(Hex.encodeHex((byte[]) entry.get("tag"))));
                  // builder.append("\",\"ref\":\"");
                  // builder.append(new String(Hex.encodeHex((byte[]) entry.get("ref"))));
                  builder.append("\",\"lat\":");
                  builder.append(entry.get("lat"));
                  builder.append(",\"d\":");
                  builder.append(JsonUtils.asJson(JsonUtils.fromSmile(LinkedHashMap.class,
                      (byte[]) entry.get("d"))));
                  // builder.append(",\"a\":");
                  // builder.append(JsonUtils.asJson(JsonUtils.fromSmile(LinkedHashMap.class,
                  // (byte[]) entry.get("a"))));
                  builder.append("}");

                  out.println(builder.toString());

                  toPrint.getAndDecrement();
                }
              } catch (Exception e) {
                e.printStackTrace();
              } finally {
                out.flush();
              }

              return null;
            }
          });
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        out.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void rebuildCounts(final String service, final DateTime t1, final DateTime t2) {
    DateTime current = t1.withZone(DateTimeZone.UTC);
    DateTime end = t2.withZone(DateTimeZone.UTC);

    while (current.isBefore(end)) {
      final DateTime timestamp = current;
      db.inTransaction(new TransactionCallback<Void>() {
        @Override
        public Void inTransaction(Handle handle, TransactionStatus status) throws Exception {
          String countsPartition = getCountsPartition(service);
          String partition = getPartition(service, timestamp);
          System.out.println(new DateTime() + " rebuild " + partition);

          try {
            try {
              int indexed =
                  handle.createStatement(
                      "alter table " + partition + " add index " + partition + "_idx"
                          + "(tag, ts, ref);").execute();
              System.out.println("  indexed where " + timestamp + " -> " + indexed);
            } catch (Exception e) {
              System.out.println("  index already there probably.");
            }

            int deleted =
                handle
                    .createStatement("delete from " + countsPartition + " where ts = ?")
                    .bind(
                        0,
                        timestamp.withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0)
                            .getMillis()).execute();

            System.out.println("  deleted where " + timestamp + " -> " + deleted);

            int inserted =
                handle
                    .createStatement(
                        "insert into "
                            + countsPartition
                            + " (tag, ts, ref, ts_min, ts_max, c, l_tot, l_sq_tot, l_max) "
                            + " select tag, (ts - (ts % 3600000)), ref, min(ts), max(ts), count(1), sum(lat), sum(lat * lat), max(lat) "
                            + " from " + partition + " group by tag, (ts - (ts % 3600000)), ref;")
                    .execute();

            System.out.println("  inserted where " + timestamp + " -> " + inserted);

            handle.createStatement("optimize table " + partition + ";").execute();
            System.out.println("  optimized where " + timestamp);


            System.out.println(new DateTime() + " finished " + partition);
          } catch (Exception e) {
            e.printStackTrace();
            System.out.println(new DateTime() + " FAIL: " + partition);
          }

          return null;
        }
      });

      current = current.plusHours(1);
    }
  }

  private boolean tableExists(Handle handle, String table) {
    Boolean alreadyThere = exists.getIfPresent(table);

    if (alreadyThere != null && alreadyThere) {
      return true;
    }

    boolean result =
        handle
            .createQuery(
                "select 1 from information_schema.tables where table_schema = ? and table_name = ?")
            .bind(0, TABLE_SPACE).bind(1, table).iterator().hasNext();

    if (result) {
      exists.put(table, Boolean.TRUE);
    }

    return result;
  }

  private String getPartition(String service, DateTime timestamp) {
    return service + "__" + format.print(timestamp.withZone(DateTimeZone.UTC));
  }

  private String getCountsPartition(String service) {
    return service + "__counts";
  }

  private String getCreateTable(String partition) {
    return "create table if not exists " + partition + " (ts bigint not null, "
        + "id binary(16) not null, " + "tag varbinary(16), " + "ref varbinary(128), "
        + "lat int default null, " + "d BLOB(8096), " + "a BLOB(8096), " + "PRIMARY KEY(ts, id) "
        + ", INDEX " + partition + "_idx" + " (tag, ts, ref) "
        + ") ENGINE=InnoDB ROW_FORMAT=DYNAMIC CHARACTER SET utf8;";
  }

  private String getCreateCountsTable(String countsPartition) {
    return "create table if not exists " + countsPartition + " (tag varbinary(16) not null, "
        + "ref varbinary(128) not null, " + "ts bigint not null, " + "ts_min bigint not null, "
        + "ts_max bigint not null, " + "c bigint, " + "l_tot bigint, " + "l_sq_tot bigint, "
        + "l_max int, "
        + "PRIMARY KEY(tag, ts, ref)) ENGINE=InnoDB ROW_FORMAT=DYNAMIC CHARACTER SET utf8;";
  }
}
