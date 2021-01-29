/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.google.gson.Gson;
import com.linkedin.tony.events.Metric;
import com.linkedin.tony.rpc.impl.MetricsRpcServer;
import com.linkedin.tony.tensorflow.TonySession.TonyTask;
import com.linkedin.tony.util.OpalUtils;

public class MetricsReporter implements Runnable {

    private static final Log LOG = LogFactory.getLog(MetricsReporter.class);

    private MetricsRpcServer metricsRpcServer;
    private Set<TonyTask> tonyTasks;
    private Configuration tonyConf;
    private String appId;

    MetricsReporter(MetricsRpcServer metricsRpc, String appId, Configuration tonyConf) {
        this.tonyTasks = new HashSet<>();
        this.metricsRpcServer = metricsRpc;
        this.appId = appId;
        this.tonyConf = tonyConf;
    }

    public synchronized void addTask(TonyTask task) {
        try {
            this.tonyTasks.add(task);
        } catch (Exception e) {
            LOG.info("Failed to add task " + task.getJobName() + " - " + task.getTaskIndex() + "to metricsReporter");
        }
    }

    public synchronized void deleteTask(TonyTask task) {
        try {
            for (TonyTask t : tonyTasks) {
                if (t.getTaskIndex().equals(task.getTaskIndex()) && t.getJobName().equals(task.getJobName())) {
                    tonyTasks.remove(t);
                    return;
                }
            }
        } catch (Exception e) {
            LOG.info("Failed to delete task " + task.getJobName() + " - "
                    + task.getTaskIndex() + "from metricsReporter");
        }
    }

    @Override
    public void run() {
        try {
            doMetricReport();
        } catch (Exception e) {
            LOG.error("Errors on schedule reporting metric.", e);
        }
    }

    private void doMetricReport() {
        if (tonyTasks == null || tonyTasks.size() == 0) {
            LOG.error("Registered tony executors are empty. " + tonyTasks);
            return;
        }
        long currentTime = new Date().getTime();
        for (TonyTask task : tonyTasks) {
            try {
                List<Metric> metrics = metricsRpcServer.getMetrics(task.getJobName(), Integer.parseInt(task.getTaskIndex()));
                if (metrics != null && metrics.size() != 0) {
                    String url = getOpalEnvUrl() + "/api/v1/tfjob/resource/push?appId=" + appId + "&taskType="
                            + task.getJobName() + "&taskIndex=" + task.getTaskIndex() + "&currentTime=" + currentTime;
                    Gson gson = new Gson();
                    String body = gson.toJson(metrics);

                    boolean httpOk = OpalUtils.httpPost(url, body);
                    if (!httpOk) {
                        LOG.error("Errors on reporting metrics: " + task.getJobName() + " - " + task.getTaskIndex());
                    }
                }
            } catch (Exception e) {
                LOG.info("Failed to get and upload metrics of task: " + task.getJobName() + " - " + task.getTaskIndex(), e);
            }
        }
    }

    private String getOpalEnvUrl() {
        String opalUrl = Constants.OPAL_URL_PROD;
        if (tonyConf.getBoolean(Constants.TONY_OPAL_TEST, false)) {
            opalUrl = Constants.OPAL_URL_TEST;
        }
        return opalUrl;
    }
}
