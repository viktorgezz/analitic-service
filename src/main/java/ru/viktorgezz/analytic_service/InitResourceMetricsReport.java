package ru.viktorgezz.analytic_service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.viktorgezz.analytic_service.model.charts.Dependency;
import ru.viktorgezz.analytic_service.model.charts.ResourceMetricsReport;
import ru.viktorgezz.analytic_service.service.intrf.ChartsService;
import ru.viktorgezz.analytic_service.service.intrf.CommonMetricsService;
import ru.viktorgezz.analytic_service.service.intrf.SpecificUrlMetrics;

@Component
@RequiredArgsConstructor
public class InitResourceMetricsReport {

    private final CommonMetricsService commonMetricsService;
    private final SpecificUrlMetrics specificUrlMetrics;
    private final ChartsService chartsService;

    public ResourceMetricsReport initComon() {
        ResourceMetricsReport report = new ResourceMetricsReport();

        ResourceMetricsReport.Metrics metrics = new ResourceMetricsReport.Metrics();
        metrics.setUptime(commonMetricsService.calculateUptimePercent());
        metrics.setAvgResponseTime(commonMetricsService.calculateAverageResponseTime());
        metrics.setIncidents(commonMetricsService.calculateGlobalIncidentCount());
        metrics.setMttr(commonMetricsService.calculateAverageIncidentDuration());
        metrics.setSlaCompliance(commonMetricsService.calculateMonthlySlaPercent());

        report.setMetrics(metrics);

        return report;
    }

    public ResourceMetricsReport initSpecific(String url, int interval) {
        ResourceMetricsReport report = new ResourceMetricsReport();
        report.setUrl(url);

        ResourceMetricsReport.Metrics metrics = new ResourceMetricsReport.Metrics();
        metrics.setUptime(specificUrlMetrics.calculateUptimePercent(url, interval));
        metrics.setAvgResponseTime(specificUrlMetrics.calculateAverageResponseTime(url, interval));
        metrics.setIncidents(specificUrlMetrics.calculateGlobalIncidentCount(url, interval));
        metrics.setMttr(specificUrlMetrics.calculateAverageIncidentDuration(url, interval));
        metrics.setSlaCompliance(specificUrlMetrics.calculateMonthlySlaPercent(url, interval));

        report.setMetrics(metrics);

        ResourceMetricsReport.Stats stats = new ResourceMetricsReport.Stats();
        stats.setFailuresCount(chartsService.getFailures(url, interval));
        stats.setResponseTime(chartsService.getResponseTime(url, interval));
        stats.setFailuresByTypes(chartsService.calculateFailuresByTypes(url, interval));
        stats.setHeatmap(chartsService.getHeatmapEntry(url, interval));
        stats.setDependencies(chartsService.getDependency(url, interval));
        report.setStats(stats);


        return report;
    }
}
