package ru.viktorgezz.analytic_service.dao;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class CommonMetricsDaoTest {

    @Autowired
    private CommonMetricsDao commonMetricsDao;

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
    void calculateUptimePercent_ShouldReturnCorrectPercentage() {
        // Given: тестовые данные уже вставлены в setUp()
        // 7 успешных из 10 общих проверок = 70%

        // When
        Optional<Double> result = commonMetricsDao.calculateUptimePercent();

        // Then
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(70.0);
    }

    @Test
    void calculateUptimePercent_WithEmptyTable_ShouldReturnEmpty() {
        // Given
        jdbcTemplate.execute("TRUNCATE TABLE checks");

        // When
        Optional<Double> result = commonMetricsDao.calculateUptimePercent();

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    void calculateAverageResponseTime_ShouldReturnCorrectAverage() {
        // Given: тестовые данные с response_time от 100 до 1000
        // Средний response_time = (100+150+200+250+300+500+600+700+800+1000) / 10 = 460

        // When
        Optional<Double> result = commonMetricsDao.calculateAverageResponseTime();

        // Then
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(460.0);
    }

    @Test
    void calculateAverageResponseTime_WithEmptyTable_ShouldReturnEmpty() {
        // Given
        jdbcTemplate.execute("TRUNCATE TABLE checks");

        // When
        Optional<Double> result = commonMetricsDao.calculateAverageResponseTime();

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    void calculateGlobalIncidentCount_ShouldReturnCorrectCount() {
        // Given: тестовые данные содержат 2 инцидента:
        // 1. example.com: failure на 10:20 и 10:25
        // 2. test.com: failure на 10:35

        // When
        Optional<Long> result = commonMetricsDao.calculateGlobalIncidentCount();

        // Then
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(2L);
    }

    @Test
    void calculateGlobalIncidentCount_WithNoFailures_ShouldReturnZero() {
        // Given
        jdbcTemplate.execute("TRUNCATE TABLE checks");
        insertOnlySuccessfulChecks();

        // When
        Optional<Long> result = commonMetricsDao.calculateGlobalIncidentCount();

        // Then
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(0L);
    }

    @Test
    void calculateAverageIncidentDuration_ShouldReturnCorrectDuration() {
        // Given: инцидент с example.com длится 5 минут (300 секунд)
        // Один инцидент с test.com не учитывается (только одна запись)

        // When
        Optional<Double> result = commonMetricsDao.calculateAverageIncidentDuration();

        // Then
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(300.0); // 5 минут в секундах
    }

    @Test
    void calculateAverageIncidentDuration_WithNoMultiRecordIncidents_ShouldReturnEmpty() {
        // Given
        jdbcTemplate.execute("TRUNCATE TABLE checks");
        insertSingleFailureChecks();

        // When
        Optional<Double> result = commonMetricsDao.calculateAverageIncidentDuration();

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    void calculateMonthlySlaPercent_ShouldReturnCorrectPercentage() {
        // Given: все тестовые данные в пределах последнего месяца
        // 7 успешных из 10 = 70%

        // When
        Optional<Double> result = commonMetricsDao.calculateMonthlySlaPercent();

        // Then
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(70L);
    }

    @Test
    void calculateMonthlySlaPercent_WithOldData_ShouldReturnEmpty() {
        // Given
        jdbcTemplate.execute("TRUNCATE TABLE checks");
        insertOldTestData();

        // When
        Optional<Double> result = commonMetricsDao.calculateMonthlySlaPercent();

        // Then
        assertThat(result).isEmpty();
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
        LocalDateTime baseTime = LocalDateTime.now().minusDays(1);

        // Успешные проверки для example.com
        insertCheck("example.com", baseTime.plusMinutes(0), true, null, 100.0f, 200);
        insertCheck("example.com", baseTime.plusMinutes(5), true, null, 150.0f, 200);
        insertCheck("example.com", baseTime.plusMinutes(10), true, null, 200.0f, 200);

        // Инцидент с example.com (2 неуспешные проверки подряд)
        insertCheck("example.com", baseTime.plusMinutes(20), false, "Connection timeout", 250.0f, 0);
        insertCheck("example.com", baseTime.plusMinutes(25), false, "Connection timeout", 300.0f, 0);

        // Восстановление example.com
        insertCheck("example.com", baseTime.plusMinutes(30), true, null, 500.0f, 200);

        // Проверки для test.com
        insertCheck("test.com", baseTime.plusMinutes(15), true, null, 600.0f, 200);
        insertCheck("test.com", baseTime.plusMinutes(25), true, null, 700.0f, 200);

        // Единичная неуспешная проверка test.com (отдельный инцидент)
        insertCheck("test.com", baseTime.plusMinutes(35), false, "DNS resolution failed", 800.0f, 0);

        // Еще одна успешная проверка
        insertCheck("api.example.com", baseTime.plusMinutes(40), true, null, 1000.0f, 200);
    }

    private void insertOnlySuccessfulChecks() {
        LocalDateTime baseTime = LocalDateTime.now().minusDays(1);

        insertCheck("example.com", baseTime.plusMinutes(0), true, null, 100.0f, 200);
        insertCheck("example.com", baseTime.plusMinutes(5), true, null, 150.0f, 200);
        insertCheck("test.com", baseTime.plusMinutes(10), true, null, 200.0f, 200);
    }

    private void insertSingleFailureChecks() {
        LocalDateTime baseTime = LocalDateTime.now().minusDays(1);

        insertCheck("example.com", baseTime.plusMinutes(0), true, null, 100.0f, 200);
        insertCheck("example.com", baseTime.plusMinutes(5), false, "Error", 150.0f, 500);
        insertCheck("example.com", baseTime.plusMinutes(10), true, null, 200.0f, 200);
    }

    private void insertOldTestData() {
        // Данные старше месяца
        LocalDateTime oldTime = LocalDateTime.now().minusMonths(2);

        insertCheck("example.com", oldTime, true, null, 100.0f, 200);
        insertCheck("example.com", oldTime.plusMinutes(5), false, "Error", 150.0f, 500);
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