package com.curiosity.executor.service;

import org.springframework.stereotype.Service;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DatabaseInspector {

    private final DataSource dataSource;

    public DatabaseInspector(DataSource dataSource) {
        this.dataSource = dataSource;
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
                tableData.put("columns", getColumns(metaData, tableName));
                tableData.put("foreign_keys", getForeignKeys(metaData, tableName));
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
}
