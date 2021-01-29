/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.plugin.models;

/**
 * @author zhangjunfan
 * @date 1/22/21
 */
public class MetricModel {
    private String appId;
    private String mode;
    private String metrics;
    private Long step;
    private String timestamp;
    private String gearId;
    private String platform = "TONY";

    public String getAppId() {
        return appId;
    }

    public String getMode() {
        return mode;
    }

    public String getMetrics() {
        return metrics;
    }

    public Long getStep() {
        return step;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getGearId() {
        return gearId;
    }

    public String getPlatform() {
        return platform;
    }

    public static class MetricModelBuilder {
        private MetricModel metricModel;

        private MetricModelBuilder() {
            this.metricModel = new MetricModel();
        }

        public static MetricModelBuilder builder() {
            return new MetricModelBuilder();
        }

        public MetricModelBuilder appId(String appId) {
            this.metricModel.appId = appId;
            return this;
        }

        public MetricModelBuilder mode(String mode) {
            this.metricModel.mode = mode;
            return this;
        }

        public MetricModelBuilder metrics(String metrics) {
            this.metricModel.metrics = metrics;
            return this;
        }

        public MetricModelBuilder timestamp(String timestamp) {
            this.metricModel.timestamp = timestamp;
            return this;
        }

        public MetricModelBuilder step(Long step) {
            this.metricModel.step = step;
            return this;
        }

        public MetricModelBuilder gearId(String gearId) {
            this.metricModel.gearId = gearId;
            return this;
        }

        public MetricModel build() {
            return this.metricModel;
        }
    }
}
