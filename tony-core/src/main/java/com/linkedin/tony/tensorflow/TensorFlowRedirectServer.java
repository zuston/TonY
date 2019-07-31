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
              String response = String.format(
                  "<!DOCTYPE html>%n" + "<html>%n" + "<head>%n" + "<title>TensorFlow Redirect Page</title>"
                      + "</head>%n" + "<body>%n <h1>TensorFlow Redirect Page</h1>"
                      + "<p>TensorBoard: <a href=http://%s>http://%s</a></p>"
                      + "<p>ApplicationMaster log: <a href=http://%s>http://%s</a></p>" + "</body>%n" + "</html>",
                  tensorBoardUrl, tensorBoardUrl, amLogUrl, amLogUrl);
              Headers headers = exchange.getResponseHeaders();
              headers.set("Content-Type", "text/html; charset=UTF-8");
              exchange.sendResponseHeaders(200, response.length());
              OutputStream os = exchange.getResponseBody();
              os.write(response.getBytes("utf-8"));
              os.close();
            } else {
              exchange.sendResponseHeaders(404, 0);
            }
          } catch (IOException e) {
            LOG.warn("Fail to redirect!");
          }
        }

      }).start();
    }

  }

}
