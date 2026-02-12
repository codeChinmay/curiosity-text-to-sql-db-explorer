package com.curiosity.executor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RequestBody; 
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.curiosity.executor.service.DatabaseInspector;
import com.curiosity.executor.service.SqlExecutorService;

import java.util.List;
import java.util.Map;

@SpringBootApplication
public class ExecutorApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExecutorApplication.class, args);
    }
}

@RestController
@RequestMapping("/mcp")
class SqlExecutorController {

    private final JdbcTemplate jdbcTemplate;
    private final DatabaseInspector databaseInspector;
    private final SqlExecutorService sqlExecutorService;

    public SqlExecutorController(JdbcTemplate jdbcTemplate, DatabaseInspector databaseInspector, SqlExecutorService sqlExecutorService) {
        this.jdbcTemplate = jdbcTemplate;
        this.databaseInspector = databaseInspector;
        this.sqlExecutorService = sqlExecutorService;
    }

    @GetMapping("/status")
    public String status() {
        return "Data Executor Service is Running";
    }

    @GetMapping("/inspect_schema")
    public List<Map<String, Object>> inspectSchema() {
        try {
            return databaseInspector.extractSchemaMetadata();
        } catch (Exception e) {
             Map<String, Object> error = new HashMap<>();
             error.put("error", e.getMessage());
             return Collections.singletonList(error);
        }
    }

    @PostMapping("/execute_sql_query")
    public List<Map<String, Object>> executeQuery(@RequestBody  SqlRequest request) {
        String sql = request.sql();
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL query cannot be empty");
        }
        
        // Simple sanitization or validation here if needed (e.g. read-only enforcement)
        // For now, we trust the agent as this is an internal tool.
        
        System.out.println("Executing SQL: " + sql);
        try {
            return sqlExecutorService.executeQuery(sql);
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", e.getMessage());
            return Collections.singletonList(error);
        }
    }

    public record SqlRequest(String sql) {}
}
