package ru.viktorgezz.analytic_service.service.impl;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.viktorgezz.analytic_service.dao.ChartsDao;
import ru.viktorgezz.analytic_service.model.charts.Dependency;
import ru.viktorgezz.analytic_service.model.charts.FailuresByTypes;
import ru.viktorgezz.analytic_service.model.charts.HeatmapEntry;
import ru.viktorgezz.analytic_service.model.charts.TimestampValue;
import ru.viktorgezz.analytic_service.service.intrf.ChartsService;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ChartsServiceImpl implements ChartsService {

    private final ChartsDao chartsDao;

    @Override
    public List<TimestampValue> getFailures(String url, int interval) {
        return chartsDao.getFailures(url, interval);
    }

    @Override
    public List<TimestampValue> getResponseTime(String url, int interval) {
        return chartsDao.getResponseTime(url, interval);
    }

    @Override
    public FailuresByTypes calculateFailuresByTypes(String url, int interval) {
        return chartsDao.calculateFailuresByTypes(url, interval).orElseGet(() -> {
                    log.warn("Не получилось создать объект FailuresByTypes");
                    return new FailuresByTypes(0, 0, 0);
                }
        );
    }

    @Override
    public List<HeatmapEntry> getHeatmapEntry(String url, int interval) {
        return chartsDao.getHeatmapEntry(url, interval);
    }

    @Override
    public List<Dependency> getDependency(String url, int interval) {
        return chartsDao.getDependency(url);
    }
}
