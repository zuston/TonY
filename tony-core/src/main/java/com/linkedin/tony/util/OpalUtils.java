/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author zhangjunfan
 * @date 1/22/21
 */
public final class OpalUtils {
    private static final Log LOG = LogFactory.getLog(OpalUtils.class);

    private OpalUtils() {
        // ignore
    }

    public static boolean httpPost(String url, String body) {
        PostMethod postMethod = null;
        try {
            HttpClient client = new HttpClient();
            postMethod = new PostMethod(url);
            if (body != null) {
                postMethod.setRequestBody(body);
            }
            postMethod.addRequestHeader("Content-Type", "application/json;charset=utf-8");
            int statusCode = client.executeMethod(postMethod);
            postMethod.getResponseBodyAsStream();
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
}
