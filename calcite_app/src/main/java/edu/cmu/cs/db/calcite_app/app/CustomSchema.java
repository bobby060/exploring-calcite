package edu.cmu.cs.db.calcite_app.app;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import javax.sql.DataSource;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;

public class CustomSchema extends AbstractSchema {
    private final Map<String, Table> tableMap;
    // private final RelDataTypeFactory typeFactory;

    // public CustomSchema(RelDataTypeFactory typeFactory) {
    // this.tableMap = new HashMap<>();
    // this.typeFactory = typeFactory;
    // }

    public CustomSchema() {
        this.tableMap = new HashMap<>();
    }

    @Override
    protected Map<String, Table> getTableMap() {

        return tableMap;
    }

    public void addTable(String name, Table table) {
        tableMap.put(name, table);
    }

    /**
     * Converts a JDBC schema to a Calcite schema
     * 
     * @param jdbcSchema
     * @return CalciteSchema
     */
    private static CalciteSchema convertJdbcSchema(JdbcSchema jdbcSchema) {
        CustomSchema CustomSchema = new CustomSchema();

        // Get all tables from JDBC schema
        Set<String> tableNames = jdbcSchema.getTableNames();

        System.out.println("Table names: " + tableNames);

        // Copy each table to the in-memory schema
        for (String tableName : tableNames) {
            Table jdbcTable = jdbcSchema.getTable(tableName);
            if (jdbcTable != null) {
                CustomTable table = CustomTable.fromJdbcTable(
                        jdbcTable,
                        tableName,
                        jdbcSchema.getDataSource(),
                        new JavaTypeFactoryImpl());
                CustomSchema.addTable(tableName, table);
            }
        }

        CalciteSchema calciteSchema = CalciteSchema.createRootSchema(true, false);
        calciteSchema.add("duckdb", CustomSchema);

        return calciteSchema.getSubSchema("duckdb", false);
    }

    /**
     * Loads the schema from the given database path
     * 
     * @param db_path
     * @return CalciteSchema
     */
    protected static CalciteSchema fromDuckDb(String db_path) throws SQLException {

        String url = "jdbc:duckdb:../data.db";

        String schemaName = "duckdb";

        CalciteSchema jdbcRootSchema = CalciteSchema.createRootSchema(false);

        String driverClassName = "org.duckdb.DuckDBDriver";
        DataSource dataSource = JdbcSchema.dataSource(url, driverClassName, null, null);

        Connection connection = dataSource.getConnection();

        System.out.println(connection.isClosed());

        JdbcSchema jdbcSchema = JdbcSchema.create(jdbcRootSchema.plus(), schemaName, dataSource, null, null);

        CalciteSchema customSchema = CustomSchema.convertJdbcSchema(jdbcSchema);

        System.out.println("Loaded tables: " + customSchema.getTableNames());

        return customSchema;

    }

    protected static void addTables(SchemaPlus rootSchema, String db_path) throws SQLException {

        String url = "jdbc:duckdb:../data.db";

        String schemaName = "duckdb";

        CalciteSchema jdbcRootSchema = CalciteSchema.createRootSchema(false);

        String driverClassName = "org.duckdb.DuckDBDriver";
        DataSource dataSource = JdbcSchema.dataSource(url, driverClassName, null, null);

        JdbcSchema jdbcSchema = JdbcSchema.create(jdbcRootSchema.plus(), schemaName, dataSource, null, null);

        Set<String> tableNames = jdbcSchema.getTableNames();

        System.out.println("Table names: " + tableNames);

        // Copy each table to the in-memory schema
        for (String tableName : tableNames) {
            Table jdbcTable = jdbcSchema.getTable(tableName);
            if (jdbcTable != null) {
                CustomTable table = CustomTable.fromJdbcTable(
                        jdbcTable,
                        tableName,
                        jdbcSchema.getDataSource(),
                        new JavaTypeFactoryImpl());
                rootSchema.add(tableName, table);
            }
        }
    }
}