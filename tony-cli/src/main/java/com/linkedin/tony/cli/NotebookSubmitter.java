/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.cli;

import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyClient;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.client.CallbackHandler;
import com.linkedin.tony.client.TaskUpdateListener;
import com.linkedin.tony.rpc.TaskInfo;
import com.linkedin.tonyproxy.ProxyServer;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.cli.ParseException;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;

import static com.linkedin.tony.Constants.TONY_FOLDER;


/**
 * NotebookSubmitter is used to submit a python pex file (for example Jupyter Notebook) to run inside a cluster.
 *
 * It would first kick off a container inside the cluster that matches the resource request
 * (am GPU/Memory/CPU) and run the specified script inside that node. To make it easier for
 * Jupyter Notebook, we also bake in a proxy server in the submitter which would automatically
 * proxy the request to that node.
 *
 * Usage:
 * // Suppose you have a folder named bin/ at root directory which contains a notebook pex file: linotebook, you can use
 * // this command to start the notebook and follow the output message to visit the jupyter notebook page.
 * CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath --glob):./:/home/khu/notebook/tony-cli-0.1.0-all.jar \
 * java com.linkedin.tony.cli.NotebookSubmitter --src_dir bin/ --executes "'bin/linotebook --ip=* $DISABLE_TOKEN'"
 */
public class NotebookSubmitter extends TonySubmitter {
  private static final Log LOG = LogFactory.getLog(NotebookSubmitter.class);
  private static final String OPAL_URL = "http://opal-prod.online.qiyi.qae/api/v1";
  private static final String OPAL_TEST_URL = "http://opal-test.online.qiyi.qae/api/v1";
  private static final String OPAL_TOKEN = "78a6841d97b4451d89f3822a3c403734";
  private static final String GEAR_URL = "http://gear.cloud.qiyi.domain";

  private class AppRegisterHandler implements CallbackHandler {

    @Override
    public void onApplicationIdReceived(final ApplicationSubmissionContext appContext) {
      registerToGear(appContext.getApplicationId());
      registerToOpal(appContext);
    }

    @Override
    public void afterApplicationSubmitted(ApplicationId appId, Set<TaskInfo> taskInfoSet) {

    }

    @Override
    public void whenApplicationFailed(ApplicationReport report) {

    }
  }

  private static class NotebookUpdateListener implements TaskUpdateListener {
    private Set<TaskInfo> taskInfos;

    Set<TaskInfo> getTaskInfos() {
      return taskInfos;
    }

    @Override
    public void onTaskInfosUpdated(Set<TaskInfo> taskInfoSet) {
      this.taskInfos = taskInfoSet;
    }
  }

  private void registerToOpal(ApplicationSubmissionContext appContext) {
    String appId = appContext.getApplicationId().toString();
    String url = getOpalEnvUrl() + "/tony/register?appId=" + appId;
    String gearId = null;
    String gearCluster = null;
    if (fromGear()) {
      String workflowActionId = System.getenv("GEAR_WORKFLOW_ACTION_ID");
      gearId = workflowActionId.split("@")[0];
      gearCluster = System.getenv("GEAR_CLUSTER");
    }
    url += gearId == null ? "" : "&workflowId=" + gearId;
    url += gearCluster == null ? "" : "&cluster=" + gearCluster;
    url += "&token=" + OPAL_TOKEN;
    LOG.info("Registering to Opal, url is " + url);
    httpPost(url, null);
  }

  private void updateStatusToOpal() {
    if (!StringUtils.isEmpty(System.getenv("GEAR_WORKFLOW_ACTION_ID"))) {
      String oozieJobId = System.getenv("GEAR_WORKFLOW_ACTION_ID");
      String url = getOpalEnvUrl() + "/tony/kill/" + oozieJobId;
      url += "&token=" + OPAL_TOKEN;
      LOG.info("Update Status to Opal, url is " + url);
      httpPost(url, null);
    }
  }

  private static String getOpalEnvUrl() {
    String testEnv = System.getenv("TEST_ENV");
    String opalUrl = OPAL_URL;
    if (!StringUtils.isEmpty(testEnv) && testEnv.equals("test")) {
      opalUrl = OPAL_TEST_URL;
    }
    return opalUrl;
  }

  private boolean httpPost(String url, String body) {
    PostMethod postMethod = null;
    try {
      HttpClient client = new HttpClient();
      postMethod = new PostMethod(url);
      if (body != null) {
        postMethod.setRequestBody(body);
      }
      postMethod.addRequestHeader("Content-Type", "application/json;charset=utf-8");
      int statusCode = client.executeMethod(postMethod);
      postMethod.getResponseBody();
      LOG.info("request to " + url + ", statusCode=" + statusCode);
    } catch (Exception e) {
      LOG.warn("Error request to " + url, e);
      return false;
    } finally {
      if (postMethod != null) {
        postMethod.releaseConnection();
      }
    }
    return true;
  }

  private void registerToGear(ApplicationId appId) {
    String gearCluster = System.getenv("GEAR_CLUSTER");
    if (gearCluster != null) {
      LOG.info("Env: GEAR_CLUSTER=" + gearCluster);
    }
    String gearWorkflowActionId = System.getenv("GEAR_WORKFLOW_ACTION_ID");
    if (gearWorkflowActionId != null) {
      LOG.info("Env: GEAR_WORKFLOW_ACTION_ID=" + gearWorkflowActionId);
    }

    if (gearWorkflowActionId != null) {
      GetMethod method = null;
      try {
        // Register back to Gear
        String[] split = gearWorkflowActionId.split("@");
        String jobCluster = hdfsConf.get("fs.defaultFS").split("://", 2)[1];
        String user = UserGroupInformation.getCurrentUser().getShortUserName();
        String url = GEAR_URL + "/hadoop/doRegister?wf_cluster=" + gearCluster + "&wf_id=" + split[0]
                + "&action_name=" + split[1] + "&job_cluster_nameservice=" + jobCluster + "&hadoop_job_id="
                + appId.toString() + "&user=" + user;
        LOG.info("Registering to Gear, url is " + url);
        HttpClient client = new HttpClient();
        method = new GetMethod(url);
        method.addRequestHeader("Connection", "close");

        int statusCode = client.executeMethod(method);
        method.getResponseBody();
        LOG.info("Registered to Gear, statusCode=" + statusCode);
      } catch (Exception e) {
        LOG.warn("Error registering to Gear", e);
      } finally {
        if (method != null) {
          method.releaseConnection();
        }
      }
    }
  }

  private boolean fromGear() {
    if (System.getenv("GEAR_WORKFLOW_ACTION_ID") != null) {
      return true;
    }
    return false;
  }

  private NotebookUpdateListener listener;
  private AppRegisterHandler handler;
  private TonyClient client;

  public NotebookSubmitter() {
    handler = new AppRegisterHandler();
    listener = new NotebookUpdateListener();
    client = new TonyClient(handler, new Configuration());
    client.addListener(listener);
  }


  Configuration hdfsConf = new Configuration();

  public int submit(String[] args) throws ParseException, URISyntaxException, IOException, InterruptedException {
    LOG.info("Starting NotebookSubmitter..");
    String jarPath = new File(NotebookSubmitter.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();

    int exitCode = 0;
    Path cachedLibPath;
    try (FileSystem fs = FileSystem.get(hdfsConf)) {
      cachedLibPath = new Path(fs.getHomeDirectory(), TONY_FOLDER + Path.SEPARATOR + UUID.randomUUID().toString());
      fs.mkdirs(cachedLibPath);
      fs.copyFromLocalFile(new Path(jarPath), cachedLibPath);
      LOG.info("Copying " + jarPath + " to: " + cachedLibPath);
    } catch (IOException e) {
      LOG.fatal("Failed to create FileSystem: ", e);
      return -1;
    }

    String[] updatedArgs = Arrays.copyOf(args, args.length + 8);
    updatedArgs[args.length] = "--hdfs_classpath";
    updatedArgs[args.length + 1] = cachedLibPath.toString();
    updatedArgs[args.length + 2] = "--conf";
    updatedArgs[args.length + 3] = TonyConfigurationKeys.APPLICATION_TIMEOUT + "=" + (24 * 60 * 60 * 1000);
    updatedArgs[args.length + 4] = "--conf";
    updatedArgs[args.length + 5] = Constants.TONY_SUBMIT_TYPE_TAG + "=notebook";
    updatedArgs[args.length + 6] = "--conf";
    String testEnv = System.getenv("TEST_ENV");
    if (!StringUtils.isEmpty(testEnv) && testEnv.equals("test")) {
      updatedArgs[args.length + 7] = Constants.TONY_OPAL_TEST + "=true";
    } else {
      updatedArgs[args.length + 7] = Constants.TONY_OPAL_TEST + "=false";
    }

    client.init(updatedArgs);
    Thread clientThread = new Thread(client::start);

    // ensure notebook application is killed when this client process terminates
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        client.forceKillApplication();
      } catch (YarnException | IOException e) {
        LOG.error("Failed to kill application during shutdown.", e);
      }
    }));
    clientThread.start();
    while (clientThread.isAlive()) {
      Set<TaskInfo> taskInfos = listener.getTaskInfos();
      if (taskInfos != null) {
        for (TaskInfo taskInfo : taskInfos) {
          if (taskInfo.getName().equals(Constants.NOTEBOOK_JOB_NAME)) {
            URL url = new URL(taskInfo.getUrl());
            String host = url.getHost();
            int port = url.getPort();
            ServerSocket localSocket = new ServerSocket(0);
            int localPort = localSocket.getLocalPort();
            localSocket.close();
            ProxyServer server = new ProxyServer(host, port, localPort);
            LOG.info("If you are running NotebookSubmitter in your local box, please open [localhost:" + localPort
                + "] in your browser to visit the page. Otherwise, if you're running NotebookSubmitter in a remote "
                + "machine (like a gateway), please run" + " [ssh -L 18888:localhost:" + localPort
                + " name_of_this_host] in your laptop and open [localhost:18888] in your browser to visit Jupyter "
                + "Notebook. If the 18888 port is occupied, replace that number with another number.");
            server.start();
            break;
          }
        }
      }
      Thread.sleep(1000);
    }
    clientThread.join();
    updateStatusToOpal();
    return exitCode;
  }

  public TonyClient getClient() {
    return client;
  }

  public static void main(String[] args) throws  Exception {
    int exitCode;
    NotebookSubmitter submitter = new NotebookSubmitter();
    exitCode = submitter.submit(args);
    System.exit(exitCode);
  }

}
