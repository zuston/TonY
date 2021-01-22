/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.plugin;

import java.util.List;

import com.linkedin.tony.tensorflow.TonySession;

import static com.linkedin.tony.Constants.CHIEF_JOB_NAME;

/**
 * @author zhangjunfan
 * @date 1/22/21
 */
public class TFTrainMetricCollector extends TFMetricCollector {
    public TFTrainMetricCollector(String appIdString, TonySession session) {
        super(appIdString, session);
    }

    @Override
    public String getEventPath() {
        return this.eventOutputPath;
    }

    @Override
    public String getCollectorMode() {
        return "TRAIN";
    }

    @Override
    public String getRoleHostName() {
        List<String> chiefList = this.session.getClusterSpec().get(CHIEF_JOB_NAME);
        log.info("Role of Chief: " + chiefList);
        return chiefList.get(0).split(":")[0];
    }

    @Override
    public boolean existRole() {
        return session.getClusterSpec().containsKey(CHIEF_JOB_NAME);
    }
}
