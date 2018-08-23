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

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Responds "OK" to every request.
 */
public class PingServlet extends HttpServlet {
  /** Use serialVersionUID for interoperability. */
  private static final long serialVersionUID = -8324672619953408748L;

  public void doWork(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
      IOException {
    PrintWriter output = resp.getWriter();
    output.println("OK");
    output.close();
  }

  /** @see HttpServlet#doGet(HttpServletRequest, HttpServletResponse) */
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
      IOException {
    doWork(req, resp);
  }

  /** @see HttpServlet#doPost(HttpServletRequest, HttpServletResponse) */
  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
      IOException {
    doWork(req, resp);
  }

  @Override
  protected void doTrace(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
      IOException {
    try (PrintWriter writer = resp.getWriter()) {
      writer.println("<html><h1>403 ACCESS FORBIDDEN</h1><p>Not Authorized</p></html>");
    }
  }
}
