/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.tony.util;

import java.io.IOException;

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

    private static final int RETRY_MAX_TIME = 3;

    private OpalUtils() {
        // ignore
    }

    public static boolean basicHttpPost(String url, String body) throws IOException {
        boolean isSuccess = false;
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
            LOG.debug("request to " + url + ", statusCode=" + statusCode);

            if (statusCode == 200) {
                isSuccess = true;
            }
        } catch (Exception e) {
            LOG.error("Error request: " + url, e);
            throw e;
        } finally {
            if (postMethod != null) {
                postMethod.releaseConnection();
            }
        }
        return isSuccess;
    }

    public static boolean httpPost(String url, String body) {
        try {
            return basicHttpPost(url, body);
        } catch (Exception e) {
            LOG.error("Errors on post to url: " + url + ", body: " + body, e);
        }
        return false;
    }

    public static boolean retryHttpPost(String url, String body) {
        int i = 0;
        while (i < RETRY_MAX_TIME) {
            try {
                return basicHttpPost(url, body);
            } catch (IOException e) {
                i++;
            }
        }
        return false;
    }
}
