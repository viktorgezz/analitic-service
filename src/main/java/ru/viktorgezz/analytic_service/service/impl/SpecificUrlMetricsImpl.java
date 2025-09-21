package ru.viktorgezz.analytic_service.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.viktorgezz.analytic_service.dao.SpecificUrlMetricsDao;
import ru.viktorgezz.analytic_service.service.intrf.SpecificUrlMetrics;

@Slf4j
@Service
@RequiredArgsConstructor
public class SpecificUrlMetricsImpl implements SpecificUrlMetrics {

    private final SpecificUrlMetricsDao specificUrlMetricsDao;

    /// Общий Uptime %
    @Override
    public double calculateUptimePercent(String url, int interval) {
        return specificUrlMetricsDao
                .calculateUptimePercent(url, interval)
                .orElseGet(() -> {
                    log.warn("Значения для uptime не могут быть вычислены");
                    return 0.0;
                });
    }

    /// Среднее время отклика
    @Override
    public double calculateAverageResponseTime(String url, int interval) {
        return specificUrlMetricsDao
                .calculateAverageResponseTime(url, interval)
                .orElseGet(() -> {
                    log.warn("Значения для Среднее время отклика не могут быть вычислены");
                    return 0.0;
                });
    }

    /// Общая частота сбоев
    @Override
    public long calculateGlobalIncidentCount(String url, int interval) {
        return specificUrlMetricsDao
                .calculateGlobalIncidentCount(url, interval)
                .orElseGet(() -> {
                    log.warn("Значения для Общая частота сбоев не могут быть вычислены");
                    return 0L;
                });
    }

    /// Общая средняя длительность инцидента
    @Override
    public double calculateAverageIncidentDuration(String url, int interval) {
        return specificUrlMetricsDao
                .calculateAverageIncidentDuration(url, interval)
                .orElseGet(() -> {
                    log.warn("Значения для Общая средняя длительность инцидента не могут быть вычислены");
                    return 0.0;
                });
    }

    /// Общий SLA за месяц (он будет равен 0-_-)
    @Override
    public double calculateMonthlySlaPercent(String url, int interval) {
        return 0.0;
    }
}
