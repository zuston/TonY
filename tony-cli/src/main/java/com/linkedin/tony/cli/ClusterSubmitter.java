/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */

package com.linkedin.tony.cli;

import com.google.gson.Gson;
import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyClient;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.client.CallbackHandler;
import com.linkedin.tony.rpc.TaskInfo;
import com.linkedin.tony.util.Utils;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.cli.ParseException;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.exceptions.YarnException;

import static com.linkedin.tony.Constants.CORE_SITE_CONF;
import static com.linkedin.tony.Constants.HADOOP_CONF_DIR;
import static com.linkedin.tony.Constants.HDFS_SITE_CONF;
import static com.linkedin.tony.Constants.TONY_FOLDER;
import static com.linkedin.tony.Constants.TONY_JAR_NAME;

/**
 * ClusterSubmitter is used to submit a distributed Tony
 * job on the cluster.
 *
 * Usage:
 * java -cp tony-cli-x.x.x-all.jar com.linkedin.tony.cli.ClusterSubmitter
 * --src_dir /Users/xxx/hadoop/li-tony_trunk/tony-core/src/test/resources/ \
 * --executes /Users/xxx/hadoop/li-tony_trunk/tony/src/test/resources/exit_0_check_env.py \
 * --python_binary_path python
 */
public class ClusterSubmitter extends TonySubmitter {
  private static final Log LOG = LogFactory.getLog(ClusterSubmitter.class);
  private static final String OPAL_URL = "http://opal-prod.online.qiyi.qae/api/v1";
  private static final String OPAL_TEST_URL = "http://opal-test.online.qiyi.qae/api/v1";
  private static final String OPAL_TOKEN = "78a6841d97b4451d89f3822a3c403734";
  private static final String GEAR_URL = "http://gear.cloud.qiyi.domain";
  private static final String TONY_OPAL_TEST = "tony.opal.test";
  private static class AppRegisterHandler implements CallbackHandler {

    @Override
    public void onApplicationIdReceived(final ApplicationSubmissionContext appContext) {
        LOG.info("Registering to gear");
      registerToGear(appContext.getApplicationId());
        LOG.info("Registering to Opal");

      registerToOpal(appContext);
    }

    @Override
    public void afterApplicationSubmitted(ApplicationId appId, Set<TaskInfo> taskInfoSet) {
      updateTaskInfoToOpal(appId, taskInfoSet);
    }

    private void updateTaskInfoToOpal(ApplicationId appId, Set<TaskInfo> taskInfoSet) {
      Gson gson = new Gson();
      String params = gson.toJson(taskInfoSet);
      try {
        String url = getOpalEnvUrl() + "/tfjob/update/log?&appId=" + appId;
        url += "&token=" + OPAL_TOKEN;
        httpPost(url, params);
      } catch (Exception e) {
        LOG.error("Failed to update task log info to opal", e);
      }
    }

    private void registerToOpal(ApplicationSubmissionContext appContext) {
      String appId = appContext.getApplicationId().toString();
      String appName = appContext.getApplicationName();
      String queue = appContext.getQueue();
      String user = null;
      try {
        user = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException e) {
        LOG.error("Failed to get app user", e);
      }
      String origin = "unknown";
      String opalUrl = getOpalEnvUrl();
      String url = opalUrl + "/tfjob/register?appId=" + appId + "&appName=" + appName + "&queue=" + queue + "&user=" + user;
      try {
          url += "&tensorboardUrl=" + URLEncoder.encode(client.getTensorboardProxyUrl(), "UTF-8");
      } catch (Exception e) {
          LOG.error("The tensorflow uri encode fail, url is " + client.getTensorboardProxyUrl());
      }
      String gearId = null;
      String cluster = hdfsConf.get("fs.defaultFS").split("://", 2)[1];;
      url += "&cluster=" + cluster;
      String gearCluster = null;
      String gearActionName = null;

      if (fromGear()) {
        origin = "gear";
        String workflowActionId = System.getenv("GEAR_WORKFLOW_ACTION_ID");
        gearId = workflowActionId.split("@")[0];
        gearActionName = workflowActionId.split("@")[1];
        gearCluster = System.getenv("GEAR_CLUSTER");
      }

      url += "&origin=" + origin;
      url += gearId == null ? "" : "&gearWorkflowId=" + gearId;
      url += gearCluster == null ? "" : "&gearCluster=" + gearCluster;
      url += gearActionName == null ? "" : "&gearActionName=" + gearActionName;

      url += "&token=" + OPAL_TOKEN;
      LOG.info("Registering to Opal, url is " + url);
      httpPost(url, client.getTonyUserConfStr());
    }

      private static String getOpalEnvUrl() {
        String opalUrl = OPAL_URL;
        LOG.info("The value is " + client.getTonyConf().get("tony.application.name"));
          if (client.getTonyConf().getBoolean(TONY_OPAL_TEST, false)) {
              opalUrl = OPAL_TEST_URL;
          }
          return opalUrl;
      }

      private boolean fromGear() {
      if (System.getenv("GEAR_WORKFLOW_ACTION_ID") != null) {
        return true;
      }
      return false;
    }

    private static boolean httpPost(String url, String body) {
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

  }

    private void pushStatus2Opal(String status) {
      String appId = client.getAppId().toString();
      String updateStatusUrl = AppRegisterHandler.getOpalEnvUrl() + "/tfjob/update/status?status=" + status + "&appId=" + appId;
      updateStatusUrl += "&token=" + OPAL_TOKEN;
      AppRegisterHandler.httpPost(updateStatusUrl, null);
    }

    private void pushStatus2Opal(int exitCode) {
        String status = exitCode == 0 ? "SUCCEEDED" : "FAILED";
        pushStatus2Opal(status);
    }

  private static Configuration hdfsConf;

  static {
    hdfsConf = new Configuration();
    hdfsConf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + CORE_SITE_CONF));
    hdfsConf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + HDFS_SITE_CONF));
    LOG.info(hdfsConf);
  }

  private static TonyClient client;

  public ClusterSubmitter(TonyClient tonyClient) {
    setTonyClient(tonyClient);
  }

  private static void setTonyClient(TonyClient tonyClient) {
      client = tonyClient;
  }

  public int submit(String[] args) throws ParseException, URISyntaxException {
    LOG.info("Starting ClusterSubmitter..");
    String jarLocation = new File(ClusterSubmitter.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
    int exitCode;
    Path cachedLibPath = null;
    try (FileSystem fs = FileSystem.get(hdfsConf)) {
      cachedLibPath = new Path(fs.getHomeDirectory(), TONY_FOLDER + Path.SEPARATOR + UUID.randomUUID().toString());
      Utils.uploadFileAndSetConfResources(cachedLibPath, new Path(jarLocation), TONY_JAR_NAME, client.getTonyConf(), fs,
          LocalResourceType.FILE, TonyConfigurationKeys.getContainerResourcesKey());
      LOG.info("Copying " + jarLocation + " to: " + cachedLibPath);
      boolean sanityCheck = client.init(args);
      if (!sanityCheck) {
        LOG.error("Arguments failed to pass sanity check");
        return -1;
      }
      // ensure application is killed when this process terminates
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          client.forceKillApplication();
//          pushStatus2Opal("KILLED");
        } catch (YarnException | IOException e) {
          LOG.error("Failed to kill application during shutdown.", e);
        }
      }));
      exitCode = client.start();
      pushStatus2Opal(exitCode);
    } catch (IOException e) {
      LOG.fatal("Failed to create FileSystem: ", e);
      exitCode = -1;
    } finally {
      Utils.cleanupHDFSPath(hdfsConf, cachedLibPath);
    }
    return exitCode;
  }

    public static void main(String[] args) throws ParseException, URISyntaxException {
    int exitCode;
    Configuration configuration = new Configuration();
    setGearToConf(configuration);
    try (TonyClient tonyClient = new TonyClient(new AppRegisterHandler(), configuration)) {
      ClusterSubmitter submitter = new ClusterSubmitter(tonyClient);
      exitCode = submitter.submit(args);
    }
    System.exit(exitCode);
  }

  private static void setGearToConf(Configuration configuration) {
    String gearWorkflowId = System.getenv("GEAR_WORKFLOW_ACTION_ID");
    if (StringUtils.isNotEmpty(gearWorkflowId)) {
      configuration.set(Constants.GEAR_WORKFLOW_ID_KEY, gearWorkflowId);
    }
  }
}
