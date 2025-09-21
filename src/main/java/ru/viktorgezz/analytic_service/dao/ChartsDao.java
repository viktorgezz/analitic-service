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
                    round(avg(response_time), 1) AS value
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
//        final String sql = """
//                SELECT
//                    multiIf(
//                        response_time <= 3, 'warning',
//                        'critical'
//                    ) AS severity,
//                    COUNT(*) AS count
//                FROM checks
//                WHERE timestamp >= now() - INTERVAL ? HOUR
//                  AND startsWith(url, ?)
//                GROUP BY severity
//                ORDER BY severity
//                """;
//        return Optional.ofNullable(jdbc
//                .query(
//                        sql,
//                        (ResultSet rs, int rowNum)
//                                -> new FailuresByTypes(
//                                0,
//                                rs.getInt("severity"),
//                                rs.getInt("count")
//                        ),
//                        interval,
//                        url
//                ).getFirst()
//        );
        return Optional.of(new FailuresByTypes(0, 0 ,0));

    }

    public List<HeatmapEntry> getHeatmapEntry(String url, int interval) {
        final String sql = """
                SELECT
                    toDayOfWeek(timestamp) AS day_of_week,
                    toHour(timestamp) AS hour_of_day,
                    COUNT(*) AS value
                FROM checks
                WHERE timestamp >= now() - INTERVAL ? HOUR
                  AND startsWith(url, ?)
                GROUP BY day_of_week, hour_of_day
                ORDER BY day_of_week, hour_of_day
                
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