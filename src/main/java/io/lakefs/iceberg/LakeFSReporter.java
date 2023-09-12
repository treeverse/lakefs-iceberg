package io.lakefs.iceberg;

import java.net.URI;
import java.util.Base64;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.core5.http.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

public class LakeFSReporter {
    private URI lakeFSEndpoint;
    private String authHeader;
    private CloseableHttpAsyncClient httpClient;
    private final String reportClient = "iceberg-catalog/" + getClass().getPackage().getImplementationVersion();
    private final Logger logger = LoggerFactory.getLogger(LakeFSReporter.class);
    private LakeFSReporter() {
    }

    public LakeFSReporter(Configuration hadoopConfig) {
        String lakeFSServerURL = hadoopConfig.get("fs.s3a.endpoint");
        this.lakeFSEndpoint = URI.create(StringUtils.stripEnd(lakeFSServerURL, "/") + "/api/v1/statistics");
        this.authHeader = generateBasicAuthHeader(hadoopConfig);
        this.httpClient = HttpAsyncClients.createDefault();
    }

    public void logOp(String op) {
        try {
            Map<String, Object> reportMap =  ImmutableMap.of("class", "integration", "name", op, "count", 1);
            SimpleHttpRequest request = generateRequest(reportMap);
            httpClient.execute(request, null);
        } catch (Throwable ignored) {
                logger.warn("Failed to report operation", ignored);
        }
    }

    private SimpleHttpRequest generateRequest(Map<String, Object> body) throws JsonProcessingException {
        String requestBody = prepareRequestBody(body);
        return SimpleRequestBuilder.post(this.lakeFSEndpoint).setBody(requestBody, ContentType.APPLICATION_JSON)
                .addHeader("Accept", "application/json")
                .addHeader("Authorization", authHeader)
                .addHeader("X-Lakefs-Client", reportClient)
                .build();
    }

    private String prepareRequestBody(Map<String, Object> requestMap) throws JsonProcessingException {
        return new ObjectMapper().writeValueAsString(ImmutableMap.of("events", new Map[] { requestMap }));
    }
    
    private String generateBasicAuthHeader(Configuration hadoopConfig) {
        String key = hadoopConfig.get("fs.s3a.access.key");
        String secret = hadoopConfig.get("fs.s3a.secret.key");
        String keySecret = key + ":" + secret;
        return "Basic " + Base64.getUrlEncoder().encodeToString(keySecret.getBytes());
    }
}
