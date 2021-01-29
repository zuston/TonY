/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.plugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.tensorflow.framework.Summary;
import org.tensorflow.spark.shaded.org.tensorflow.hadoop.util.TFRecordReader;
import org.tensorflow.util.Event;

import com.google.gson.Gson;
import com.linkedin.tony.Constants;
import com.linkedin.tony.TonyConfigurationKeys;
import com.linkedin.tony.plugin.models.MetricModel;
import com.linkedin.tony.tensorflow.TonySession;
import com.linkedin.tony.util.OpalUtils;

import static com.linkedin.tony.TonyConfigurationKeys.APPLICATION_TF_EVENT_COLLECT_INTERVAL;
import static com.linkedin.tony.TonyConfigurationKeys.APPLICATION_TF_EVENT_OUTPUT;

/**
 * @author zhangjunfan
 * @date 1/28/21
 */
public abstract class TFMetricCollector implements Runnable {
    // for test, when need to directly read hdfs event files instead of generating by current task .
    private static final String IGNORE_VALIDATE_ROLE_HOST =
            TonyConfigurationKeys.TONY_APPLICATION_PREFIX + "tfcollector.ignore.validate.role.host";
    private static final List<String> IGNORE_TFRECORD_TAGS = Arrays.asList("checkpoint_path");

    private String appId;
    private boolean ignoreValidationHost;
    private long collectInterval;

    protected Log log = LogFactory.getLog(this.getClass());
    protected final String eventOutputPath;
    protected final TonySession session;

    public TFMetricCollector(final String appIdString, final TonySession session) {
        this.eventOutputPath = session.getTonyConf().get(APPLICATION_TF_EVENT_OUTPUT);
        this.session = session;
        this.appId = appIdString;
        this.ignoreValidationHost = session.getTonyConf().getBoolean(IGNORE_VALIDATE_ROLE_HOST, false);
        this.collectInterval = session.getTonyConf().getLong(APPLICATION_TF_EVENT_COLLECT_INTERVAL, 10 * 1000);
    }

    @Override
    public void run() {
        try {
            start();
        } catch (Exception e) {
            log.error("Errors on collecting tf metrics.", e);
        }
    }

    private void start() throws Exception {
        log.info("TensorFlow job output basic path: " + eventOutputPath);
        if (this.eventOutputPath == null) {
            return;
        }

        boolean existRole = existRole();

        log.info("Tensorflow job role [" + getCollectorMode() + "] exist: " + existRole);

        if (!existRole) {
            return;
        }

        /**
         * 1. read file
         * 2. get metric and report to opal
         */
        String eventDir = getEventPath();

        Path eventFilePath = findLatestEventFile(eventDir);

        log.info("Found event file path: " + eventFilePath);

        if (eventFilePath == null) {
            return;
        }

        readingSummaryIterator(eventFilePath);

        log.info("Finish getting TFRecord stream.");
        return;
    }

    public abstract boolean existRole();

    public abstract String getEventPath();

    private void readingSummaryIterator(Path accessEventPath) throws Exception {
        int fetchIndex = 1;
        long fileOffset = 0;
        while (true) {
            log.debug("Fetching event file stream. The fetching index: " + fetchIndex);
            fileOffset = readBatchData(accessEventPath, fileOffset);
            fetchIndex += fetchIndex;
            log.info("Getting data input stream from HDFS..., offset: " + fileOffset);

            if (this.session.isTrainingFinished()) {
                log.info("All task finished.");
                break;
            }

            try {
                log.info("Reading sleeping...");
                Thread.sleep(collectInterval);
            } catch (Exception e) {
                // ignore.
            }
        }
    }

    private long readBatchData(Path accessEventPath, long fileOffset) throws Exception {
        FileSystem fileSystem = null;
        FSDataInputStream dataInputStream = null;
        try {
            fileSystem = accessEventPath.getFileSystem(getDefaultConf());
            dataInputStream = fileSystem.open(accessEventPath);
            dataInputStream.seek(fileOffset);

            TFRecordReader recordReader = new TFRecordReader(dataInputStream, true);
            byte[] record = recordReader.read();

            log.debug("Getting TFRecord bytes.., fileOffset: " + fileOffset);

            // TODO: 1/22/21 only using the latest event when collecting the same metric.
            while (record != null) {
                Event event = Event.parseFrom(record);
                fileOffset = dataInputStream.getPos();
                report2Opal(event, getCollectorMode());
                record = recordReader.read();
            }

        } catch (Exception e) {
            log.error("Errors on getting event data.", e);
            if (dataInputStream != null) {
                log.error("DataInputStream is available ? " + (dataInputStream.available() > 0));
            }
        } finally {
            if (dataInputStream != null) {
                try {
                    dataInputStream.close();
                } catch (IOException e) {
                    log.error("Errors on closing data input stream.", e);
                }
            }

            if (fileSystem != null) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    log.error("Errors on closing filesystem.", e);
                }
            }
        }
        return fileOffset;
    }

    public abstract String getCollectorMode();

    private void report2Opal(Event event, String mode) {
        if (event == null) {
            return;
        }

        if (event.getSummary() == null || event.getSummary().getValueCount() == 0) {
            return;
        }

        Double wallTime = event.getWallTime();
        Long step = event.getStep();

        Map<String, Float> metrics = new HashMap<>();
        for (Summary.Value value : event.getSummary().getValueList()) {
            String tag = value.getTag();
            if (tag == null || IGNORE_TFRECORD_TAGS.contains(tag)) {
                continue;
            }
            float simpleValue = value.getSimpleValue();
            metrics.put(tag, simpleValue);
        }

        if (metrics.size() == 0) {
            return;
        }

        Gson gson = new Gson();
        String metricJson = gson.toJson(metrics);

        mode = mode.toUpperCase();

        log.info("===============================");
        log.info("mode: " + mode);
        log.info("wall time: " + wallTime);
        log.info("global step: " + step);
        log.info("metrics: " + metricJson);
        log.info("===============================");

        MetricModel metricModel = MetricModel.MetricModelBuilder.builder()
                .appId(appId)
                .mode(mode)
                .step(step)
                .metrics(metricJson)
                .timestamp(wallTime.toString())
                .gearId(getGearWorkflowId())
                .build();

        String registerJson = gson.toJson(metricModel);
        log.info("Registering metric json: " + registerJson);
        boolean httpOk = OpalUtils.httpPost(getOpalURL(), registerJson);
        if (!httpOk) {
            log.error("Errors on reporting to Opal, report body: " + registerJson);
        }
    }

    private Path findLatestEventFile(String eventDir) throws InterruptedException {
        Path accessEventPath = null;

        while (true) {
            log.info("Finding event file..., path: " + eventDir);
            Thread.sleep(1000 * 20);
            try {
                List<FileStatus> fileStatusList = listDir(eventDir);
                if (fileStatusList == null) {
                    continue;
                }
                String roleHost = getRoleHostName();
                FileStatus latestFileStatus = fileStatusList.stream()
                        .filter(fileStatus -> {
                            boolean prefixFilter = fileStatus.getPath().toString().contains("events.out.tfevents");

                            // for online test.
                            if (ignoreValidationHost) {
                                return prefixFilter;
                            }

                            // roleHost is null when not registering to AppMaster
                            boolean suffixFilter =
                                    roleHost == null ? false : fileStatus.getPath().toString().endsWith(roleHost);
                            return prefixFilter && suffixFilter;
                        })
                        .max(Comparator.comparing(FileStatus::getModificationTime))
                        .orElse(null);

                if (latestFileStatus == null) {
                    log.info("Event folder is empty. path: " + eventDir);
                    continue;
                }

                accessEventPath = latestFileStatus.getPath();
                break;
            } catch (Exception e) {
                log.info("Event path not found. path: " + eventDir, e);
            }
        }
        return accessEventPath;
    }

    public abstract String getRoleHostName();

    private List<FileStatus> listDir(String filePath) throws IOException {
        Path path = new Path(filePath);
        FileSystem fileSystem = path.getFileSystem(getDefaultConf());
        if (fileSystem.isDirectory(path)) {
            FileStatus[] statuses = fileSystem.listStatus(path);
            return Arrays.asList(statuses);
        }
        return null;
    }

    private Configuration getDefaultConf() {
        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl.disable.cache", "true");
        return configuration;
    }

    private String getOpalURL() {
        String baseUrl = isOpalTest() ? Constants.OPAL_URL_TEST : Constants.OPAL_URL_PROD;
        return String.format("%s%s", baseUrl, "/api/v1/tfjob/register/metrics");
    }

    private boolean isOpalTest() {
        return session.getTonyConf().getBoolean(Constants.TONY_OPAL_TEST, false);
    }

    private String getGearWorkflowId() {
        return session.getTonyConf().get(Constants.GEAR_WORKFLOW_ID_KEY);
    }
}
