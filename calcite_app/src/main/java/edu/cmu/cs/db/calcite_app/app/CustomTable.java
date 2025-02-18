package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.sql.SQLException;
import javax.sql.DataSource;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;

/**
 * Custom table class for our in-memory tables
 * 
 * Allows for loading data from a JDBC table into memory and using table with
 * Enumerable RelRunner
 * 
 * 
 */
public class CustomTable extends AbstractTable implements ScannableTable {
    private static final int BATCH_SIZE = 5000; // batch size for loading data from jdbc
    private final RelDataType rowType;
    private final ArrayList<Object[]> data;
    private final List<RelDataTypeField> fieldTypes;

    private final Statistic statistic;

    private CustomTable(RelDataType rowType, ArrayList<Object[]> data, Statistic statistic) {
        this.rowType = rowType;
        this.data = data;
        this.statistic = statistic;
        this.fieldTypes = rowType.getFieldList();
    }

    @Override
    public Statistic getStatistic() {
        return this.statistic;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return Linq4j.asEnumerable(data);
        // return new CustomEnumerable(data);

    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return rowType;
    }

    // Only way to build a table
    public static CustomTable fromJdbcTable(Table jdbcTable, String tableName,
            DataSource dataSource,
            RelDataTypeFactory typeFactory) {
        RelDataType rowType = jdbcTable.getRowType(new JavaTypeFactoryImpl());

        // System.out.println("rowType: " + rowType.);

        Statistic statistic = new CustomStatistic(150);
        ArrayList<Object[]> data = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {

            // Set fetch size for batch loading
            stmt.setFetchSize(BATCH_SIZE);
            stmt.setMaxRows(BATCH_SIZE);

            // Create statistic from row count
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                if (rs.next()) {
                    double rowCount = rs.getDouble(1);
                    statistic = new CustomStatistic(rowCount);
                }
            }

            // Load data into memory
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                int columnCount = rs.getMetaData().getColumnCount();
                int rowCount = 0;

                // while (rs.next()) {
                // TODO: remove limit
                // while (rowCount < 5000 && rs.next()) {
                while (rs.next()) {
                    Object[] row = new Object[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        row[i] = rs.getObject(i + 1);
                    }
                    data.add(row);

                    rowCount++;
                }
                rs.close();

                System.out.println("Loaded " + rowCount + " rows from " + tableName);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error loading data from JDBC table: " +
                    e.getMessage(), e);

        } finally {
        }

        return new CustomTable(rowType, data, statistic);
    }

    // Created to use instead of Linq4j.asEnumerable. Yet another example of
    // building something because I hadn't found the right function yet
    private class CustomEnumerable extends AbstractEnumerable<Object[]> {

        private final ResultSet data;

        private Enumerator<Object[]> enumerator = new Enumerator<Object[]>() {
            private int index = -1;

            @Override
            public Object[] current() {
                System.out.println("Current: " + index);
                List<Object> result = new ArrayList<>();
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    try {
                        switch (fieldTypes.get(i).getType().getSqlTypeName()) {
                            case DATE:
                                result.add(data.getDate(i));
                                break;
                            case VARCHAR:
                                result.add(data.getString(i));
                                break;
                            case DOUBLE:
                                result.add(data.getDouble(i));
                                break;
                            case BOOLEAN:
                                result.add(data.getBoolean(i));
                                break;
                            case BIGINT:
                                result.add(data.getLong(i));
                                break;
                            default:
                                result.add(data.getObject(i));
                                break;
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException("Error reading data from JDBC table: " +
                                e.getMessage(), e);
                    }
                }
                return result.toArray();
            }

            @Override
            public boolean moveNext() {
                index++;
                boolean result = false;

                try {
                    result = data.next();
                } catch (SQLException e) {
                    return false;
                }
                return result;
            }

            @Override
            public void reset() {
                try {
                    data.beforeFirst();
                } catch (SQLException e) {
                    throw new RuntimeException("Error resetting JDBC table: " +
                            e.getMessage(), e);
                }
            }

            @Override
            public void close() {
                try {
                    data.close();
                } catch (SQLException e) {
                    throw new RuntimeException("Error closing JDBC table: " +
                            e.getMessage(), e);
                }
            }
        };

        CustomEnumerable(ResultSet data) {
            super();
            this.data = data;
            System.out.println("created enumerator");

        }

        @Override
        public Enumerator<Object[]> enumerator() {
            return this.enumerator;
        }
    };

    /**
     * Adds CustomTables to the given root schema from the given duckdb database
     * path
     * 
     * @param rootSchema Schema to add tables to
     * @param dbPath     path to db
     * @throws SQLException
     */
    protected static void addTables(SchemaPlus rootSchema, String dbPath) throws SQLException {

        String url = "jdbc:duckdb:" + dbPath;

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
