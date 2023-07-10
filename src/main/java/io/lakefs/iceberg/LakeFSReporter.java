package io.lakefs.iceberg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class LakeFSReporter {
    private URI lakeFSEndpoint;
    private Configuration hadoopConfig;
    private HttpClient httpClient;
    private final String reportClient = "iceberg-catalog/" + getClass().getPackage().getImplementationVersion();

    private LakeFSReporter(){}
    public LakeFSReporter(Configuration hadoopConfig) {
        String lakeFSServerURL = hadoopConfig.get("fs.s3a.endpoint");
        if (lakeFSServerURL.endsWith("/")) {
            lakeFSServerURL = lakeFSServerURL.substring(lakeFSServerURL.length()-1);
        }
        lakeFSServerURL += "/api/v1/statistics";
        this.lakeFSEndpoint = URI.create(lakeFSServerURL);
        this.hadoopConfig = hadoopConfig;
        this.httpClient = HttpClient.newHttpClient();
    }

    public void logOp(String op) {
        Map<String, Object> reportMap = new HashMap<>() {
            {
                put("class", "integration");
                put("name", op);
                put("count", 1);
            }
        };
        try {
            log(reportMap);
        } catch (JsonProcessingException ignored) { }
    }
    private void log(Map<String, Object> reportMap) throws JsonProcessingException {
        HttpRequest request = generateRequest(reportMap);
        httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());
    }

    private HttpRequest generateRequest(Map<String, Object> body) throws JsonProcessingException {
        String requestBody = prepareRequestBody(body);
        String authHeader = generateBasicAuthHeader();
        return HttpRequest
                .newBuilder()
                .uri(this.lakeFSEndpoint)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .header("Accept", "application/json")
                .header("Content-Type", "application/json")
                .header("Authorization", authHeader)
                .header("X-Lakefs-Client", reportClient)
                .build();
    }

    private String prepareRequestBody(Map<String, Object> requestMap) throws JsonProcessingException {
        var statisticsRequest = new HashMap<String, Map<String, Object>[]>() {
            {
                put("events", new Map[]{requestMap});
            }
        };

        var objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(statisticsRequest);
    }

    private String generateBasicAuthHeader() {
        String key = hadoopConfig.get("fs.s3a.access.key");
        String secret = hadoopConfig.get("fs.s3a.secret.key");
        String keySecret = key + ":" + secret;
        return "Basic " + Base64.getUrlEncoder().encodeToString(keySecret.getBytes());
    }
}
