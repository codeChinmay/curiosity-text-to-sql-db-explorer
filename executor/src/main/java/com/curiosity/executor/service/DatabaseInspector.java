package com.curiosity.executor.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class DatabaseInspector {

    private final DataSource dataSource;
    private final JdbcTemplate jdbcTemplate;

    private static final int SAMPLE_ROW_LIMIT = 5;

    public DatabaseInspector(DataSource dataSource, JdbcTemplate jdbcTemplate) {
        this.dataSource = dataSource;
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Map<String, Object>> extractSchemaMetadata() throws SQLException {
        List<Map<String, Object>> schemaInfo = new ArrayList<>();
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            // Get all user tables
            ResultSet tables = metaData.getTables(null, "public", "%", new String[]{"TABLE"});

            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                Map<String, Object> tableData = new HashMap<>();
                tableData.put("name", tableName);

                List<Map<String, String>> columns = getColumns(metaData, tableName);
                tableData.put("columns", columns);
                tableData.put("foreign_keys", getForeignKeys(metaData, tableName));

                // Fetch random sample rows for LLM enrichment context
                List<String> columnNames = columns.stream()
                        .map(c -> c.get("name"))
                        .collect(Collectors.toList());
                tableData.put("sample_rows", getSampleRows(tableName, columnNames));

                schemaInfo.add(tableData);
            }
        }
        return schemaInfo;
    }

    private List<Map<String, String>> getColumns(DatabaseMetaData metaData, String tableName) throws SQLException {
        List<Map<String, String>> columns = new ArrayList<>();
        ResultSet rs = metaData.getColumns(null, "public", tableName, "%");
        while (rs.next()) {
            Map<String, String> col = new HashMap<>();
            col.put("name", rs.getString("COLUMN_NAME"));
            col.put("type", rs.getString("TYPE_NAME"));
            columns.add(col);
        }
        return columns;
    }

    private List<Map<String, String>> getForeignKeys(DatabaseMetaData metaData, String tableName) throws SQLException {
        List<Map<String, String>> fks = new ArrayList<>();
        ResultSet rs = metaData.getImportedKeys(null, "public", tableName);
        while (rs.next()) {
            Map<String, String> fk = new HashMap<>();
            fk.put("target_table", rs.getString("PKTABLE_NAME"));
            fk.put("fk_column", rs.getString("FKCOLUMN_NAME"));
            fk.put("pk_column", rs.getString("PKCOLUMN_NAME"));
            fks.add(fk);
        }
        return fks;
    }

    /**
     * Fetches up to {@link #SAMPLE_ROW_LIMIT} randomly-selected rows from the
     * given table. These example rows give the LLM enrichment step in the
     * explorer service concrete data to infer domain semantics from.
     *
     * Returns an empty list if the query fails (e.g. empty table, permissions).
     */
    private List<Map<String, Object>> getSampleRows(String tableName, List<String> columnNames) {
        if (columnNames == null || columnNames.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            // Quote column names to handle reserved words / special characters
            String cols = columnNames.stream()
                    .map(c -> "\"" + c + "\"")
                    .collect(Collectors.joining(", "));

            String sql = String.format(
                    "SELECT %s FROM \"%s\" ORDER BY RANDOM() LIMIT %d",
                    cols, tableName, SAMPLE_ROW_LIMIT
            );

            return jdbcTemplate.queryForList(sql);
        } catch (Exception e) {
            // Non-fatal — sample rows are best-effort enrichment context
            System.err.println("[DatabaseInspector] Could not fetch sample rows for '"
                    + tableName + "': " + e.getMessage());
            return Collections.emptyList();
        }
    }
}
