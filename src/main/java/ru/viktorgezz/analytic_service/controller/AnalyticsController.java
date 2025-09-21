package ru.viktorgezz.analytic_service.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.viktorgezz.analytic_service.InitResourceMetricsReport;
import ru.viktorgezz.analytic_service.model.charts.ResourceMetricsReport;

@Slf4j
@RestController
@RequiredArgsConstructor
@Tag(name = "Analytics", description = "API для получения аналитических отчетов")
public class AnalyticsController {

    private final InitResourceMetricsReport initResourceMetricsReport;

    @Operation(
            summary = "Отчет по конкретному URL",
            description = "Возвращает метрики и статистику по конкретному ресурсу за заданный интервал"
    )
    @ApiResponse(responseCode = "200", description = "Успешный ответ с отчетом")
    @GetMapping("/report")
    public ResourceMetricsReport sendSpecificUrl(
            @RequestParam String url,
            @RequestParam int intervalHour
    ) {
        try {
            log.info("Запрос отчета для URL: {}, интервал: {} часов", url, intervalHour);
            // Убираем лишний слеш в конце URL если он есть
            String cleanUrl = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
            log.info("Очищенный URL: {}", cleanUrl);
            return initResourceMetricsReport.initSpecific(cleanUrl, intervalHour);
        } catch (Exception e) {
            log.error("Ошибка при получении отчета для URL: {}, интервал: {} часов", url, intervalHour, e);
            throw e;
        }
    }

    @Operation(
            summary = "Общий отчет",
            description = "Возвращает агрегированные метрики по всем URL"
    )
    @ApiResponse(responseCode = "200", description = "Успешный ответ с отчетом")
    @GetMapping("/report-common")
    public ResourceMetricsReport sendCommonUrl(
    ) {
        return initResourceMetricsReport.initComon();
    }
}
