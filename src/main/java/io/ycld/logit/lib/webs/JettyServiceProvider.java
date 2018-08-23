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
package io.ycld.logit.lib.webs;

import java.util.EnumSet;

import javax.servlet.DispatcherType;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.servlet.GuiceFilter;

public class JettyServiceProvider implements Provider<Server> {
  private Config config;

  @Inject
  public JettyServiceProvider(Config config) {
    this.config = config;
  }

  @Override
  public Server get() {
    HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setOutputBufferSize(32768);

    int minThreads = Integer.parseInt(System.getProperty("minThreads", "10"));
    int maxThreads = Integer.parseInt(System.getProperty("maxThreads", "10"));

    QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads);
    Server server = new Server(threadPool);

    ServerConnector connector = new ServerConnector(server, new HttpConnectionFactory(httpConfig));

    connector.setHost(config.getString("http.host"));
    connector.setPort(config.getInt("http.port"));
    connector.setAcceptQueueSize(config.getInt("http.accept_queue_size"));

    // connector.setThreadPool(new QueuedThreadPool(config.getInt("http.threads")));
    // connector.setAcceptors(config.getInt("http.acceptors"));
    // connector.setMaxIdleTime(config.getInt("http.max_idle_time"));
    // connector.setLowResourcesConnections(config.getInt("http.low_resources_connections"));

    server.setConnectors(new Connector[] {connector});

    ServletContextHandler root =
        new ServletContextHandler(server, "/", ServletContextHandler.NO_SESSIONS);

    root.addFilter(GuiceFilter.class, "/*", EnumSet.<DispatcherType>of(DispatcherType.REQUEST));
    root.addServlet(EmptyServlet.class, "/*");

    HandlerCollection handlers = new HandlerCollection();

    String localLogPath = System.getProperty("logBase", "./log");

    handlers.setHandlers(new Handler[] {root, getSecondaryLogHandler(localLogPath)});
    server.setHandler(handlers);

    return server;
  }

  private static RequestLogHandler getSecondaryLogHandler(String localLogPath) {
    RequestLogHandler logHandler = new RequestLogHandler();
    NCSARequestLog requestLog =
        new NCSARequestLog(localLogPath + "/jetty/jetty-yyyy_mm_dd.request.log");
    requestLog.setRetainDays(32);
    requestLog.setPreferProxiedForAddress(true);
    requestLog.setAppend(true);
    requestLog.setExtended(true);
    requestLog.setLogLatency(true);
    requestLog.setLogTimeZone("UTC");
    logHandler.setRequestLog(requestLog);

    return logHandler;
  }
}
