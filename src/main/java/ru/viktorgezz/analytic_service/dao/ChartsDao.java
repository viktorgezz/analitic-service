package ru.viktorgezz.analytic_service.dao;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import ru.viktorgezz.analytic_service.model.charts.Dependency;
import ru.viktorgezz.analytic_service.model.charts.FailuresByTypes;
import ru.viktorgezz.analytic_service.model.charts.HeatmapEntry;
import ru.viktorgezz.analytic_service.model.charts.TimestampValue;

import java.sql.ResultSet;
import java.util.List;
import java.util.Optional;

@Slf4j
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

    public List<Dependency> getDependency(String url) {
        try {

            final String sql = """
                    WITH time_buckets AS (
                        SELECT DISTINCT
                            toStartOfInterval(timestamp, INTERVAL 1 MINUTE) AS time_bucket
                        FROM checks
                        WHERE startsWith(url, ?) -- вставить нужный URL
                    ),
                    url_list AS (
                        SELECT DISTINCT url
                        FROM checks
                        WHERE startsWith(url, ?) -- вставить нужный URL
                    ),
                    failures AS (
                        SELECT
                            toStartOfInterval(c.timestamp, INTERVAL 1 MINUTE) AS time_bucket,
                            c.url,
                            max(c.success = false) AS is_failure
                        FROM checks c
                        WHERE startsWith(c.url, ?) -- вставить нужный URL
                        GROUP BY time_bucket, c.url
                    ),
                    correlations AS (
                        SELECT
                            u1.url AS from_url,
                            u2.url AS to_url,
                            (
                                avg(f1.is_failure * f2.is_failure) - avg(f1.is_failure) * avg(f2.is_failure)
                            ) / (
                                sqrt(avg(f1.is_failure * f1.is_failure) - pow(avg(f1.is_failure), 2)) *
                                sqrt(avg(f2.is_failure * f2.is_failure) - pow(avg(f2.is_failure), 2))
                            ) AS correlation
                        FROM url_list u1
                        CROSS JOIN url_list u2
                        JOIN failures f1 ON f1.url = u1.url
                        JOIN failures f2 ON f2.url = u2.url AND f2.time_bucket = f1.time_bucket
                        WHERE u1.url != u2.url
                        GROUP BY u1.url, u2.url
                        HAVING
                            sqrt(avg(f1.is_failure * f1.is_failure) - pow(avg(f1.is_failure), 2)) > 0 AND
                            sqrt(avg(f2.is_failure * f2.is_failure) - pow(avg(f2.is_failure), 2)) > 0
                    )
                    SELECT
                        from_url AS "from",
                        to_url AS "to",
                        round(correlation, 2) AS correlation
                    FROM correlations
                    ORDER BY correlation DESC
                    
                    """;
            return jdbc
                    .query(
                            sql,
                            (ResultSet rs, int rowNum)
                                    -> new Dependency(
                                    rs.getString("from"),
                                    rs.getString("to"),
                                    rs.getDouble("correlation")
                            ),
                            url,
                            url,
                            url
                    );
        } catch (Exception e) {
            log.error("Не удалось получить Dependency (correlation)");
            return List.of(new Dependency("http://example", "http://example1", 0.0));
        }
    }
}