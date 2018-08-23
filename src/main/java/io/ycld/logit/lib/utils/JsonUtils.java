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
package io.ycld.logit.lib.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Throwables;

public class JsonUtils {
  private static final ObjectMapper jsonMapper = new ObjectMapper();
  private static final SmileFactory smile = new SmileFactory();
  private static final ObjectMapper smileMapper = new ObjectMapper(smile);

  static {
    jsonMapper.registerModule(new JodaModule());
    jsonMapper.getFactory().configure(JsonGenerator.Feature.ESCAPE_NON_ASCII, true);
    smileMapper.registerModule(new JodaModule());
  }

  public static String asJson(Object obj) {
    try {
      return jsonMapper.writeValueAsString(obj);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static <T> T fromJson(Class<T> clazz, String value) {
    try {
      return jsonMapper.readValue(value, clazz);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static <T> T fromJson(Class<T> clazz, byte[] json) {
    try {
      return jsonMapper.readValue(json, clazz);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static <T> T fromSmile(Class<T> clazz, byte[] bytes) {
    try {
      return smileMapper.readValue(bytes, clazz);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static byte[] asSmile(Object instance) {
    try {
      return smileMapper.writeValueAsBytes(instance);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
