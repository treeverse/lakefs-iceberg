package io.lakefs.iceberg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.core5.http.ContentType;

import java.net.URI;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class LakeFSReporter {
    private URI lakeFSEndpoint;
    private String authHeader;
    private CloseableHttpAsyncClient httpClient;
    private final String reportClient = "iceberg-catalog/" + getClass().getPackage().getImplementationVersion();

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
            Map<String, Object> reportMap = new HashMap<>();
            reportMap.put("class", "integration");
            reportMap.put("name", op);
            reportMap.put("count", 1);
            SimpleHttpRequest request = generateRequest(reportMap);
            httpClient.execute(request, null);
        } catch (JsonProcessingException ignored) {
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
        Map<String, Map<String, Object>[]> statisticsRequest = new HashMap<String, Map<String, Object>[]>() {
            {
                put("events", new Map[]{requestMap});
            }
        };
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(statisticsRequest);
    }

    private String generateBasicAuthHeader(Configuration hadoopConfig) {
        String key = hadoopConfig.get("fs.s3a.access.key");
        String secret = hadoopConfig.get("fs.s3a.secret.key");
        String keySecret = key + ":" + secret;
        return "Basic " + Base64.getUrlEncoder().encodeToString(keySecret.getBytes());
    }
}
