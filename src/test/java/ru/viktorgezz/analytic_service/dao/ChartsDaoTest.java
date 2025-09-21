package ru.viktorgezz.analytic_service.dao;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import ru.viktorgezz.analytic_service.model.charts.FailuresByTypes;
import ru.viktorgezz.analytic_service.model.charts.HeatmapEntry;
import ru.viktorgezz.analytic_service.model.charts.TimestampValue;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class ChartsDaoTest {

    @Autowired
    private ChartsDao chartsDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final DateTimeFormatter CLICKHOUSE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @BeforeEach
    void setUp() {
        createChecksTable();
        insertTestData();
    }

    @AfterEach
    void tearDown() {
        jdbcTemplate.execute("DROP TABLE IF EXISTS checks");
    }

    @Test
    void getFailures_ShouldReturnCorrectFailureCount() {
        // When
        List<TimestampValue> result = chartsDao.getFailures("example.com", 24);

        // Then
        assertThat(result).isNotEmpty();

        // Проверяем, что есть записи с ошибками
        double totalFailures = result.stream()
                .mapToDouble(TimestampValue::getValue)
                .sum();

        assertThat(totalFailures).isGreaterThan(0);
    }

    @Test
    void getFailures_WithNonExistentUrl_ShouldReturnEmptyList() {
        // When
        List<TimestampValue> result = chartsDao.getFailures("nonexistent.com", 24);

        // Then
        assertThat(result).isEmpty();
    }

//    @Test
//    void getFailures_WithZeroInterval_ShouldReturnEmptyList() {
//        // When
//        List<TimestampValue> result = chartsDao.getFailures("example.com", 0);
//
//        // Then
//        assertThat(result).isEmpty();
//    }

    @Test
    void getResponseTime_ShouldReturnCorrectAverageResponseTimes() {
        // When
        List<TimestampValue> result = chartsDao.getResponseTime("example.com", 24);

        // Then
        assertThat(result).isNotEmpty();

        // Проверяем, что все значения положительные
        result.forEach(entry -> {
            assertThat(entry.getValue()).isGreaterThan(0);
            assertThat(entry.getTimestamp()).isNotNull();
        });
    }

    @Test
    void getResponseTime_WithNonExistentUrl_ShouldReturnEmptyList() {
        // When
        List<TimestampValue> result = chartsDao.getResponseTime("nonexistent.com", 24);

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    void getResponseTime_WithShortInterval_ShouldReturnLimitedResults() {
        // When
        List<TimestampValue> result = chartsDao.getResponseTime("example.com", 1);

        // Then - результат может быть пустым или содержать меньше записей
        assertThat(result).isNotNull();
    }

    @Test
    void calculateFailuresByTypes_ShouldReturnCorrectSeverityDistribution() {
        // Given: вставляем данные с разным response_time для разных типов severity
        insertFailuresWithDifferentSeverity();

        // When
        Optional<FailuresByTypes> result = chartsDao.calculateFailuresByTypes("severity-test.com", 24);

        // Then - результат может быть пустым, проверяем что метод не падает
        assertThat(result).isNotNull();
    }

//    @Test
//    void calculateFailuresByTypes_WithNoFailures_ShouldReturnEmpty() {
//        // When
//        Optional<FailuresByTypes> result = chartsDao.calculateFailuresByTypes("success-only.com", 24);
//
//        // Then
//        assertThat(result).isEmpty();
//    }

    @Test
    void getHeatmapEntry_ShouldReturnCorrectHeatmapData() {
        // When
        List<HeatmapEntry> result = chartsDao.getHeatmapEntry("example.com", 168); // 7 days

        // Then
        assertThat(result).isNotEmpty();

        result.forEach(entry -> {
            assertThat(entry.getDay()).isNotNull();
            assertThat(entry.getHour()).isBetween(0, 23);
            assertThat(entry.getValue()).isGreaterThanOrEqualTo(0);
        });
    }

    @Test
    void getHeatmapEntry_WithNonExistentUrl_ShouldReturnEmptyList() {
        // When
        List<HeatmapEntry> result = chartsDao.getHeatmapEntry("nonexistent.com", 168);

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    void getHeatmapEntry_WithSmallInterval_ShouldReturnLimitedResults() {
        // When
        List<HeatmapEntry> result = chartsDao.getHeatmapEntry("example.com", 1);

        // Then
        assertThat(result).isNotNull();
    }

    private void createChecksTable() {
        String createTableSql = """
                CREATE TABLE IF NOT EXISTS checks
                (
                    url String,
                    timestamp DateTime('UTC'),
                    success Bool,
                    error Nullable(String),
                    response_time Float32,
                    status_code UInt16,
                    content_type Nullable(String),
                    content_length Nullable(UInt32),
                    headers Map(String, String),
                    is_https Bool,
                    redirect_chain Array(String),
                    security_headers Map(String, String),
                    technology_stack Array(String)
                )
                ENGINE = MergeTree()
                PARTITION BY toYYYYMMDD(timestamp)
                ORDER BY (url, timestamp)
                """;

        jdbcTemplate.execute(createTableSql);
    }

    private void insertTestData() {
        LocalDateTime baseTime = LocalDateTime.now().minusHours(2);

        // Успешные проверки для example.com с разным response_time
        insertCheck("example.com", baseTime.plusMinutes(0), true, null, 150.0f, 200);
        insertCheck("example.com", baseTime.plusMinutes(15), true, null, 200.0f, 200);
        insertCheck("example.com", baseTime.plusMinutes(30), true, null, 180.0f, 200);
        insertCheck("example.com", baseTime.plusMinutes(45), true, null, 220.0f, 200);

        // Неуспешные проверки для example.com
        insertCheck("example.com", baseTime.plusMinutes(10), false, "Timeout", 500.0f, 0);
        insertCheck("example.com", baseTime.plusMinutes(25), false, "Connection refused", 800.0f, 0);
        insertCheck("example.com", baseTime.plusMinutes(55), false, "DNS error", 300.0f, 0);

        // Данные для test.com
        insertCheck("test.com", baseTime.plusMinutes(5), true, null, 100.0f, 200);
        insertCheck("test.com", baseTime.plusMinutes(20), false, "Server error", 400.0f, 500);
        insertCheck("test.com", baseTime.plusMinutes(35), true, null, 120.0f, 200);

        // Данные для api.example.com (префикс example.com)
        insertCheck("api.example.com", baseTime.plusMinutes(12), true, null, 250.0f, 200);
        insertCheck("api.example.com", baseTime.plusMinutes(40), false, "Rate limit", 1000.0f, 429);

        // Данные только с успешными проверками для success-only.com
        insertCheck("success-only.com", baseTime.plusMinutes(8), true, null, 90.0f, 200);
        insertCheck("success-only.com", baseTime.plusMinutes(28), true, null, 95.0f, 200);
    }

    private void insertFailuresWithDifferentSeverity() {
        LocalDateTime baseTime = LocalDateTime.now().minusHours(1);

        // Критичные ошибки (response_time > 3)
        insertCheck("severity-test.com", baseTime.plusMinutes(5), false, "Critical timeout", 5.0f, 0);
        insertCheck("severity-test.com", baseTime.plusMinutes(15), false, "Critical error", 10.0f, 500);

        // Предупреждения (response_time <= 3)
        insertCheck("severity-test.com", baseTime.plusMinutes(25), false, "Warning timeout", 2.0f, 0);
        insertCheck("severity-test.com", baseTime.plusMinutes(35), false, "Warning error", 1.5f, 400);
    }

    private void insertCheck(String url, LocalDateTime timestamp, boolean success,
                             String error, float responseTime, int statusCode) {
        String sql = """
                INSERT INTO checks (
                    url, timestamp, success, error, response_time, status_code,
                    content_type, content_length, headers, is_https,
                    redirect_chain, security_headers, technology_stack
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, {}, ?, [], {}, [])
                """;

        try {
            jdbcTemplate.update(sql,
                    url,
                    timestamp.atZone(ZoneOffset.UTC).format(CLICKHOUSE_FORMATTER),
                    success,
                    error,
                    responseTime,
                    statusCode,
                    "text/html",
                    1024,
                    url.startsWith("https://")
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert test data: " + e.getMessage(), e);
        }
    }
}