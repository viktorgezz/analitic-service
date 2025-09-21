package ru.viktorgezz.analytic_service.service.intrf;

public interface CommonMetricsService {

    double calculateUptimePercent();

    double calculateAverageResponseTime();

    long calculateGlobalIncidentCount();

    double calculateAverageIncidentDuration();

    double calculateMonthlySlaPercent();

}
