package ru.viktorgezz.analytic_service.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Check {
    private String url;
    private OffsetDateTime timestamp;
    private Boolean success;
    private String error;
    private Float responseTime;
    private Integer statusCode;
    private String contentType;
    private Integer contentLength;
    private Map<String, String> headers;
    private Boolean isHttps;
    private List<String> redirectChain;
    private String sslInfo;
    private Map<String, String> securityHeaders;
    private List<String> technologyStack;
}

