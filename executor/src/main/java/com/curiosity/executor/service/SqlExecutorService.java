package com.curiosity.executor.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Service
public class SqlExecutorService {

    private final JdbcTemplate jdbcTemplate;
    private static final Pattern DANGEROUS_KEYWORDS = Pattern.compile(
            "(?i)\\b(DROP|ALTER|INSERT|UPDATE|DELETE|TRUNCATE|GRANT|REVOKE)\\b"
    );

    public SqlExecutorService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> executeQuery(String sql) {
        if (isDangerous(sql)) {
            throw new SecurityException("Only READ-ONLY queries are allowed. Dangerous keywords detected.");
        }
        return jdbcTemplate.queryForList(sql);
    }

    private boolean isDangerous(String sql) {
        return DANGEROUS_KEYWORDS.matcher(sql).find();
    }
}
