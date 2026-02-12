package com.curiosity.executor.controller;

import com.curiosity.executor.service.DatabaseInspector;
import com.curiosity.executor.service.SqlExecutorService;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/mcp")
public class MCPController {

    private final DatabaseInspector databaseInspector;
    private final SqlExecutorService sqlExecutorService;

    public MCPController(DatabaseInspector databaseInspector, SqlExecutorService sqlExecutorService) {
        this.databaseInspector = databaseInspector;
        this.sqlExecutorService = sqlExecutorService;
    }

    @PostMapping("/execute_sql_query")
    public List<Map<String, Object>> executeSqlQuery(@RequestBody Map<String, String> payload) {
        String sql = payload.get("sql");
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL query is required");
        }
        return sqlExecutorService.executeQuery(sql);
    }

    @PostMapping("/refresh_schema_metadata")
    public List<Map<String, Object>> refreshSchemaMetadata(@RequestBody Map<String, String> payload) throws SQLException {
        // In future, payload would contain sourceId.
        // For now, we refresh the default datasource schema.
        return databaseInspector.extractSchemaMetadata();
    }
}
