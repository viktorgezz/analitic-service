package ru.viktorgezz.analytic_service.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import ru.viktorgezz.analytic_service.model.Check;
import ru.viktorgezz.analytic_service.service.intrf.CheckService;

import java.util.List;

@Service
@RequiredArgsConstructor
public class CheckServiceImpl implements CheckService {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public void saveChecks(List<Check> checks) {
        final String sql = """
                INSERT INTO checks
                (url, timestamp, success, error, response_time, status_code, content_type,
                 content_length, headers, is_https, redirect_chain, security_headers, technology_stack)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        checks.forEach(check -> {
            jdbcTemplate.update(
                    sql,
                    check.getUrl(),
                    check.getTimestamp(),
                    check.getSuccess(),
                    check.getError(),
                    check.getResponseTime(),
                    check.getStatusCode(),
                    check.getContentType(),
                    check.getContentLength(),
                    check.getHeaders(),
                    check.getIsHttps(),
                    check.getRedirectChain(),
                    check.getSecurityHeaders(),
                    check.getTechnologyStack()
                    );
        });

    }
}
