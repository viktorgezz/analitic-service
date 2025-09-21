package ru.viktorgezz.analytic_service.service.intrf;

import ru.viktorgezz.analytic_service.model.charts.Dependency;
import ru.viktorgezz.analytic_service.model.charts.FailuresByTypes;
import ru.viktorgezz.analytic_service.model.charts.HeatmapEntry;
import ru.viktorgezz.analytic_service.model.charts.TimestampValue;

import java.util.List;

public interface ChartsService {

    List<TimestampValue> getFailures(String url, int interval);

    List<TimestampValue> getResponseTime(String url, int interval);

    FailuresByTypes calculateFailuresByTypes(String url, int interval);

    List<HeatmapEntry> getHeatmapEntry(String url, int interval);

    List<Dependency> getDependency(String url, int interval);

}
