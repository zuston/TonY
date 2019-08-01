/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.tensorflow;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class TensorFlowRedirectServer {

  private static final Log LOG = LogFactory.getLog(TensorFlowRedirectServer.class);

  private HttpServer server; // TODO Use non-restricted API instead
  private String tensorBoardUrl;
  private String amLogUrl;
  private String hostName;
  private int port;

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getTensorBoardUrl() {
    return tensorBoardUrl;
  }

  public void setTensorBoardUrl(String tensorBoardUrl) {
    this.tensorBoardUrl = tensorBoardUrl;
  }

  public String getAmLogUrl() {
    return amLogUrl;
  }

  public void setAmLogUrl(String amLogUrl) {
    this.amLogUrl = amLogUrl;
  }

  public void run() {
    new Thread(new Runnable() {
      @Override
      public void run() {
        server.start();
      }
    }).start();
  }

  public void init(String hostName, int amRpcPort) throws Exception {
    this.hostName = hostName;
    this.port = findUsablePort(amRpcPort);
    if (this.port == 0) {
      throw new Exception("Failed to get available port for redirect server");
    }
    this.server = HttpServer.create(new InetSocketAddress(this.hostName, this.port), 0);
    this.server.createContext("/", new JumpHandler());
  }

  public void stop() {
    server.stop(10);
  }

  private int findUsablePort(int amRpcPort) {
    int tmpPort = 0;
    try {
      ServerSocket tmpSocket = null;
      do {
        tmpSocket = new ServerSocket(0);
        tmpSocket.setReuseAddress(true);
        tmpPort = tmpSocket.getLocalPort();
      } while (tmpPort == amRpcPort);

      tmpSocket.close();
    } catch (IOException e) {
      // Ignore Exception on close()
    } finally {
      return tmpPort;
    }
  }

  private class JumpHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange exchange) {
      new Thread(new Runnable() {

        @Override
        public void run() {
          try {
            if (exchange.getRequestMethod().equals("GET")) {
              StringBuilder sb = new StringBuilder();
              sb.append("<!DOCTYPE html><html>");
              sb.append("<head><title>TensorFlow Redirect Page</title></head>");
              sb.append("<body>");
              sb.append("<h1>TensorFlow Redirect Page</h1>");
              sb.append("<p>TensorBoard: ");
              if (tensorBoardUrl == null) {
                sb.append("(not started yet)");
              } else {
                sb.append("<a href=\"");
                sb.append(tensorBoardUrl);
                sb.append("\">");
                sb.append(tensorBoardUrl);
                sb.append("</a>");
              }
              sb.append("</p>");
              sb.append("<p>ApplicationMaster log: <a href=\"");
              sb.append(amLogUrl);
              sb.append("\">");
              sb.append(amLogUrl);
              sb.append("</a></p>");
              sb.append("</body>");
              sb.append("</html>");
              String html = sb.toString();
              Headers headers = exchange.getResponseHeaders();
              headers.set("Content-Type", "text/html; charset=UTF-8");
              exchange.sendResponseHeaders(200, html.length());
              OutputStream os = exchange.getResponseBody();
              os.write(html.getBytes("utf-8"));
              os.close();
            } else {
              exchange.sendResponseHeaders(404, 0);
            }
          } catch (IOException e) {
            LOG.error("Fail to redirect!", e);
          }
        }

      }).start();
    }

  }

}
