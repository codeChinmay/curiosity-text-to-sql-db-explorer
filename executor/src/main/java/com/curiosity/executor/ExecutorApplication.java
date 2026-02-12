package com.curiosity.executor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class ExecutorApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExecutorApplication.class, args);
    }
}

@RestController
@org.springframework.web.bind.annotation.RequestMapping("/mcp")
class SqlExecutorController {

    private final org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;
    private final com.curiosity.executor.service.DatabaseInspector databaseInspector;

    public SqlExecutorController(org.springframework.jdbc.core.JdbcTemplate jdbcTemplate, com.curiosity.executor.service.DatabaseInspector databaseInspector) {
        this.jdbcTemplate = jdbcTemplate;
        this.databaseInspector = databaseInspector;
    }

    @GetMapping("/status")
    public String status() {
        return "Data Executor Service is Running";
    }

    @GetMapping("/inspect_schema")
    public java.util.List<java.util.Map<String, Object>> inspectSchema() {
        try {
            return databaseInspector.extractSchemaMetadata();
        } catch (Exception e) {
             java.util.Map<String, Object> error = new java.util.HashMap<>();
             error.put("error", e.getMessage());
             return java.util.Collections.singletonList(error);
        }
    }

    @org.springframework.web.bind.annotation.PostMapping("/execute_sql_query")
    public java.util.List<java.util.Map<String, Object>> executeQuery(@org.springframework.web.bind.annotation.RequestBody SqlRequest request) {
        String sql = request.sql();
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL query cannot be empty");
        }
        
        // Simple sanitization or validation here if needed (e.g. read-only enforcement)
        // For now, we trust the agent as this is an internal tool.
        
        System.out.println("Executing SQL: " + sql);
        try {
            return jdbcTemplate.queryForList(sql);
        } catch (Exception e) {
            java.util.Map<String, Object> error = new java.util.HashMap<>();
            error.put("error", e.getMessage());
            return java.util.Collections.singletonList(error);
        }
    }

    public record SqlRequest(String sql) {}
}
