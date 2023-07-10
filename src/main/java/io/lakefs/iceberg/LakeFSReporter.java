package io.lakefs.iceberg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
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
    private String authHeader;
    private HttpClient httpClient;
    private final String reportClient = "iceberg-catalog/" + getClass().getPackage().getImplementationVersion();

    private LakeFSReporter(){}
    public LakeFSReporter(Configuration hadoopConfig) {
        String lakeFSServerURL = hadoopConfig.get("fs.s3a.endpoint");
        this.lakeFSEndpoint = URI.create(StringUtils.stripEnd(lakeFSServerURL, "/") + "/api/v1/statistics");
        this.authHeader = generateBasicAuthHeader(hadoopConfig);
        this.httpClient = HttpClient.newHttpClient();
    }

    public void logOp(String op) {
        try {
            Map<String, Object> reportMap = Map.of("class", "integration", "name", op, "count", 1);
            HttpRequest request = generateRequest(reportMap);
            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        } catch (JsonProcessingException ignored) { }
    }

    private HttpRequest generateRequest(Map<String, Object> body) throws JsonProcessingException {
        String requestBody = prepareRequestBody(body);
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

    private String generateBasicAuthHeader(Configuration hadoopConfig) {
        String key = hadoopConfig.get("fs.s3a.access.key");
        String secret = hadoopConfig.get("fs.s3a.secret.key");
        String keySecret = key + ":" + secret;
        return "Basic " + Base64.getUrlEncoder().encodeToString(keySecret.getBytes());
    }
}
