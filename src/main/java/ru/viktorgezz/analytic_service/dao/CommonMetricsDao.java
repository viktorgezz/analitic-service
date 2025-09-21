package ru.viktorgezz.analytic_service.dao;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
public class CommonMetricsDao {

    private final JdbcTemplate jdbc;

    public Optional<Double> calculateUptimePercent() {
        final String sql = """
                SELECT
                    round(SUM(success) * 100.0 / COUNT(*), 2) AS global_uptime_percent
                FROM checks
                """;

        Double result = jdbc.queryForObject(sql, Double.class);
        return (result == null || result.isNaN()) ? Optional.empty() : Optional.of(result);
    }

    public Optional<Double> calculateAverageResponseTime() {
        final String sql = """
                SELECT
                    avg(response_time) AS avg_response_time
                FROM checks
                """;

        Double result = jdbc.queryForObject(sql, Double.class);
        return (result == null || result.isNaN()) ? Optional.empty() : Optional.of(result);
    }

    public Optional<Long> calculateGlobalIncidentCount() {
        final String sql = """
                WITH failure_groups AS (
                    SELECT
                        url,
                        timestamp,
                        success,
                        if(success = false AND lagInFrame(success) OVER (ORDER BY url, timestamp) != false, 1, 0) AS is_start
                    FROM checks),
                grouped AS (
                    SELECT
                        url,
                        sum(is_start) OVER (ORDER BY url, timestamp) AS group_id
                    FROM failure_groups
                    WHERE success = false
                )
                SELECT
                    COUNT(DISTINCT group_id) AS global_incident_count
                FROM grouped
                """;
        return Optional.ofNullable(jdbc.queryForObject(sql, Long.class));
    }

    public Optional<Double> calculateAverageIncidentDuration() {
        final String sql = """
                WITH failure_groups AS (
                    SELECT
                        url,
                        timestamp,
                        success,
                        if(success = false AND lagInFrame(success) OVER (ORDER BY url, timestamp) != false, 1, 0) AS is_start
                    FROM checks
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
                    avg(dateDiff('second', start_time, end_time)) AS avg_incident_duration_seconds
                FROM incident_bounds
                """;
        Double result = jdbc.queryForObject(sql, Double.class);
        return (result == null || result.isNaN()) ? Optional.empty() : Optional.of(result);
    }

    public Optional<Double> calculateMonthlySlaPercent() {
        final String sql = """
                SELECT
                    round(SUM(success) * 100.0 / COUNT(*), 2) AS month_SLA
                FROM checks
                WHERE timestamp >= now() - INTERVAL 1 MONTH
                """;

        Double result = jdbc.queryForObject(sql, Double.class);
        return (result == null || result.isNaN()) ? Optional.empty() : Optional.of(result);
    }
}
