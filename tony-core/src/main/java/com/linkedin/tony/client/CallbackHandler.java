/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.client;

import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;

import com.linkedin.tony.rpc.TaskInfo;

/**
 * CallbackHandler is used to get callbacks from TonyClient when some
 * asynchronous information becomes available.
 */
public interface CallbackHandler {
    // Called when TonyClient gets an application id response from RM.
    public void onApplicationIdReceived(final ApplicationSubmissionContext appContext);

    public void afterApplicationSubmitted(final ApplicationId appId, final Set<TaskInfo> taskInfoSet);
}
