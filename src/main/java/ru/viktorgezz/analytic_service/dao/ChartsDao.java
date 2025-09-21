package ru.viktorgezz.analytic_service.dao;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import ru.viktorgezz.analytic_service.model.charts.FailuresByTypes;
import ru.viktorgezz.analytic_service.model.charts.HeatmapEntry;
import ru.viktorgezz.analytic_service.model.charts.TimestampValue;

import java.sql.ResultSet;
import java.util.List;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class ChartsDao {

    private final JdbcTemplate jdbc;

    public List<TimestampValue> getFailures(String url, int interval) {
        final String sql = """
                SELECT
                    toStartOfInterval(timestamp, INTERVAL 1 HOUR) AS time_bucket,
                    COUNT(*) AS incident_count
                FROM checks
                WHERE success = false
                AND timestamp >= now() - INTERVAL ? HOUR
                AND startsWith(url, ?)
                GROUP BY time_bucket
                ORDER BY time_bucket
                """;

        return jdbc
                .query(
                        sql,
                        (ResultSet rs, int rowNum)
                                -> new TimestampValue(
                                rs.getString("time_bucket"),
                                rs.getInt("incident_count")
                        ),
                        interval,
                        url
                );
    }

    public List<TimestampValue> getResponseTime(String url, int interval) {
        final String sql = """
                SELECT
                    toStartOfInterval(timestamp, INTERVAL 1 HOUR) AS timestamp,
                    round(avg(response_time), 3) AS value
                FROM checks
                WHERE timestamp >= now() - INTERVAL ? HOUR
                  AND startsWith(url, ?)
                GROUP BY timestamp
                ORDER BY timestamp
                """;

        return jdbc
                .query(
                        sql,
                        (ResultSet rs, int rowNum)
                                -> new TimestampValue(
                                rs.getString("timestamp"),
                                rs.getDouble("value")
                        ),
                        interval,
                        url
                );
    }

    public Optional<FailuresByTypes> calculateFailuresByTypes(String url, int interval) {
        final String sql = """
                SELECT
                countIf(response_time > 1) AS resolved,
                countIf(response_time >= 1 AND response_time <= 3) AS warning,
                countIf(response_time > 3) AS critical
                FROM checks
                WHERE startsWith(url, ?)
                AND timestamp >= now() - INTERVAL ? HOUR
                AND success = False
                """;
        return Optional.ofNullable(jdbc
                .query(
                        sql,
                        (ResultSet rs, int rowNum)
                                -> new FailuresByTypes(
                                rs.getInt("critical"),
                                rs.getInt("warning"),
                                rs.getInt("resolved")
                        ),
                        url,
                        interval
                ).getFirst()
        );
    }

    public List<HeatmapEntry> getHeatmapEntry(String url, int interval) {
        final String sql = """
                WITH all_hours AS (
                    SELECT number AS hour_of_day FROM numbers(24)
                ),
                all_days AS (
                    SELECT
                        number + 1 AS day_of_week_num,
                        ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][number + 1] AS day_of_week
                    FROM numbers(7)
                ),
                cartesian_product AS (
                    SELECT d.day_of_week, h.hour_of_day
                    FROM all_days d
                    CROSS JOIN all_hours h
                ),
                real_failures AS (
                    SELECT
                        ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][
                            toDayOfWeek(timestamp, 1)
                        ] AS day_of_week,
                        toHour(timestamp) AS hour_of_day,
                        COUNT(*) AS value
                    FROM checks
                    WHERE timestamp >= now() - INTERVAL ? HOUR
                      AND startsWith(url, ?)
                      AND success = false
                    GROUP BY day_of_week, hour_of_day
                )
                SELECT
                    cp.day_of_week AS day_of_week,
                    cp.hour_of_day AS hour_of_day,
                    COALESCE(f.value, 0) AS value
                FROM cartesian_product cp
                LEFT JOIN real_failures f
                    ON cp.day_of_week = f.day_of_week
                   AND cp.hour_of_day = f.hour_of_day
                ORDER BY
                    CASE cp.day_of_week
                        WHEN 'Monday'    THEN 1
                        WHEN 'Tuesday'   THEN 2
                        WHEN 'Wednesday' THEN 3
                        WHEN 'Thursday'  THEN 4
                        WHEN 'Friday'    THEN 5
                        WHEN 'Saturday'  THEN 6
                        WHEN 'Sunday'    THEN 7
                    END,
                    cp.hour_of_day
                """;

        return jdbc
                .query(
                        sql,
                        (ResultSet rs, int rowNum)
                                -> new HeatmapEntry(
                                rs.getString("day_of_week"),
                                rs.getInt("hour_of_day"),
                                rs.getInt("value")
                        ),
                        interval,
                        url
                );
    }
}