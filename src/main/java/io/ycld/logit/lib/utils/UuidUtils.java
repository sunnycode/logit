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

import com.google.common.base.Throwables;

public class UuidUtils {
  public static final String getUUID(byte[] uuidBytes) {
    char[] hexString = Hex.encodeHex(uuidBytes);

    StringBuilder builder = new StringBuilder();
    builder.append(hexString, 0, 8);
    builder.append("-");
    builder.append(hexString, 8, 4);
    builder.append("-");
    builder.append(hexString, 12, 4);
    builder.append("-");
    builder.append(hexString, 16, 4);
    builder.append("-");
    builder.append(hexString, 20, 12);

    return builder.toString();
  }

  public static final byte[] getUUID(String uuid) {
    try {
      return Hex.decodeHex(uuid.replace("-", "").toCharArray());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
