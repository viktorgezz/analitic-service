package ru.viktorgezz.analytic_service.model.charts;

import lombok.Data;

import java.util.List;

@Data
public class ResourceMetricsReport {
    private String resourceId;
    private String resourceName;
    private String url;
    private Metrics metrics;
    private Stats stats;

    @Data
    public static class Metrics {
        private double uptime;
        private double avgResponseTime;
        private long incidents;
        private double mttr;
        private double slaCompliance;
    }

    @Data
    public static class Stats {
        private List<TimestampValue> failuresCount;
        private List<TimestampValue> responseTime;
        private FailuresByTypes failuresByTypes;
        private List<HeatmapEntry> heatmap;
        private List<Dependency> dependencies;
    }
}

