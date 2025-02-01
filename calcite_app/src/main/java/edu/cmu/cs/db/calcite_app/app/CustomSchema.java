package edu.cmu.cs.db.calcite_app.app;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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

    public static CalciteSchema convertJdbcSchema(JdbcSchema jdbcSchema) {
        CustomSchema CustomSchema = new CustomSchema();

        // Get all tables from JDBC schema
        Set<String> tableNames = jdbcSchema.getTableNames();

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
        calciteSchema.add("duck_db", CustomSchema);

        return calciteSchema;
    }
}