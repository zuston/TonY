/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.plugin;

import com.linkedin.tony.tensorflow.TonySession;

import static com.linkedin.tony.Constants.EVALUATOR_JOB_NAME;

/**
 * @author zhangjunfan
 * @date 1/21/21
 */
public class TFEvalMetricCollector extends TFMetricCollector {
    public TFEvalMetricCollector(String appIdString, TonySession session) {
        super(appIdString, session);
    }

    @Override
    public String getCollectorMode() {
        return "EVAL";
    }

    @Override
    public String getRoleHostName() {
        String hostPort = session.getClusterSpec().get(EVALUATOR_JOB_NAME).get(0);
        String host = hostPort.split(":")[0];
        log.info("Current role [" + getCollectorMode() + "], host: " + host);
        return host;
    }

    @Override
    public boolean existRole() {
        return session.getClusterSpec().containsKey(EVALUATOR_JOB_NAME);
    }

    @Override
    public String getEventPath() {
        return eventOutputPath + "/eval";
    }
}
