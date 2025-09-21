package ru.viktorgezz.analytic_service.dao;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
public class SpecificUrlMetricsDao {

    private final JdbcTemplate jdbc;

    public Optional<Double> calculateUptimePercent(String url, int interval) {
        final String sql = """
                SELECT
                    round(SUM(success) * 100.0 / COUNT(*), 2) AS uptime_percent
                FROM checks
                WHERE timestamp >= now() - INTERVAL ? HOUR
                  AND startsWith(url, ?)
                """;
        Double result = jdbc.queryForObject(sql, Double.class, interval, url);
        return (result == null || result.isNaN()) ? Optional.empty() : Optional.of(result);
    }

    public Optional<Double> calculateAverageResponseTime(String url, int interval) {
        final String sql = """
                SELECT
                    avg(response_time) AS avg_response_time
                FROM checks
                WHERE timestamp >= now() - INTERVAL ? HOUR
                  AND startsWith(url, ?)
                """;
        Double result = jdbc.queryForObject(sql, Double.class, interval, url);
        return (result == null || result.isNaN()) ? Optional.empty() : Optional.of(result);
    }

    public Optional<Long> calculateGlobalIncidentCount(String url, int interval) {
        final String sql = """
                WITH failure_groups AS (
                    SELECT
                        url,
                        timestamp,
                        success,
                        if(success = false AND lagInFrame(success) OVER (ORDER BY url, timestamp) != false, 1, 0) AS is_start
                    FROM checks
                    WHERE timestamp >= now() - INTERVAL ? HOUR
                      AND startsWith(url, ?)
                ),
                grouped AS (
                    SELECT
                        url,
                        sum(is_start) OVER (ORDER BY url, timestamp) AS group_id
                    FROM failure_groups
                    WHERE success = false
                )
                SELECT
                    COUNT(DISTINCT group_id) AS incident_count
                FROM grouped
                """;
        return Optional.ofNullable(jdbc.queryForObject(sql, Long.class, interval, url));
    }

    public Optional<Double> calculateAverageIncidentDuration(String url, int interval) {
        final String sql = """
                WITH failure_groups AS (
                    SELECT
                        url,
                        timestamp,
                        success,
                        if(success = false AND lagInFrame(success) OVER (ORDER BY url, timestamp) != false, 1, 0) AS is_start
                    FROM checks
                    WHERE timestamp >= now() - INTERVAL ? HOUR
                      AND startsWith(url, ?)
                ),
                grouped AS (
                    SELECT
                        url,
                        timestamp,
                        sum(is_start) OVER (ORDER BY url, timestamp) AS group_id
                    FROM failure_groups
                    WHERE success = false
                ),
                incident_bounds AS (
                    SELECT
                        group_id,
                        min(timestamp) AS start_time,
                        max(timestamp) AS end_time
                    FROM grouped
                    GROUP BY group_id
                    HAVING count(*) >= 2
                )
                SELECT
                    avg(dateDiff('second', start_time, end_time)) AS avg_duration_seconds
                FROM incident_bounds
                """;

        Double result = jdbc.queryForObject(sql, Double.class, interval, url);
        return (result == null || result.isNaN()) ? Optional.empty() : Optional.of(result);
    }

    public Optional<Double> calculateMonthlySlaPercent(String url, int interval) {
        return Optional.of(0.0);
    }
}
