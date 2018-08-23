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

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.eclipse.jetty.server.Server;

import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Starts the embedded Jetty server.
 */
public class Main {
  public static void main(String[] args) throws Exception {
    if (System.getProperty("java.net.preferIPv4Stack") == null) {
      System.setProperty("java.net.preferIPv4Stack", "true");
    }

    Executor exec = Executors.newFixedThreadPool(1);
    exec.execute(new GCTask());

    final Injector injector = Guice.createInjector(new LogServiceModule());
    injector.getInstance(Server.class).start();
  }

  private static class GCTask implements Runnable {
    public void run() {
      while (true) {
        try {
          System.gc();
          Thread.sleep(120000);
        } catch (InterruptedException ignored) {}
      }
    }
  }
}
