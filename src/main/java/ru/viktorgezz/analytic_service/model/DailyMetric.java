package ru.viktorgezz.analytic_service.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DailyMetric {
    private String url;
    private LocalDate date;
    private Integer totalChecks;
    private Integer successfulChecks;
    private Float uptimePercent;
    private Float avgResponseTime;
    private Float maxResponseTime;
    private Float p95ResponseTime;
    private Float avgContentLength;
    private Map<String, Integer> errorCategoryBreakdown;
}

