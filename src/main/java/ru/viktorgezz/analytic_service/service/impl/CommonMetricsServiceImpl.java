package ru.viktorgezz.analytic_service.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.viktorgezz.analytic_service.dao.CommonMetricsDao;
import ru.viktorgezz.analytic_service.service.intrf.CommonMetricsService;

@Slf4j
@Service
@RequiredArgsConstructor
public class CommonMetricsServiceImpl implements CommonMetricsService {

    private final CommonMetricsDao commonMetricsDao;

    /// Общий Uptime % (по всем URL)
    @Override
    public double calculateUptimePercent() {
        return commonMetricsDao.calculateUptimePercent().orElseGet(() -> {
                    log.warn("Значения для uptime не могут быть вычислены");
                    return 0.0;
                }
        );
    }

    /// Среднее время отклика (по всем URL)
    @Override
    public double calculateAverageResponseTime() {
        return commonMetricsDao.calculateAverageResponseTime().orElseGet(() -> {
                    log.warn("Значения для Среднее время отклика не могут быть вычислены");
                    return 0.0;
                }
        );
    }

    /// Общая частота сбоев
    @Override
    public long calculateGlobalIncidentCount() {
        return commonMetricsDao.calculateGlobalIncidentCount().orElseGet(() -> {
            log.warn("Значения для Общая частота сбоев не могут быть вычислены");
            return 0L;
        });
    }

    /// Общая средняя длительность инцидента
    @Override
    public double calculateAverageIncidentDuration() {
        return commonMetricsDao.calculateAverageIncidentDuration().orElseGet(() -> {
                    log.warn("Значения для Общая средняя длительность инцидента не могут быть вычислены");
                    return 0.0;
                }
        );
    }

    /// Общий SLA за месяц
    @Override
    public double calculateMonthlySlaPercent() {
        return commonMetricsDao.calculateMonthlySlaPercent().orElseGet(() -> {
            log.warn("Значения для Общий SLA за месяц не могут быть вычислены");
            return 0.0;
        });
    }
}
