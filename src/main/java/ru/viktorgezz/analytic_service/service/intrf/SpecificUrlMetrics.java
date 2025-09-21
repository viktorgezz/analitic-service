package ru.viktorgezz.analytic_service.service.intrf;

public interface SpecificUrlMetrics {

    double calculateUptimePercent(String url, int interval);

    double calculateAverageResponseTime(String url, int interval);

    long calculateGlobalIncidentCount(String url, int interval);

    double calculateAverageIncidentDuration(String url, int interval);

    double calculateMonthlySlaPercent(String url, int interval);
}
