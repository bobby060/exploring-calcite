package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.sql.SQLException;
import javax.sql.DataSource;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;

public class CustomTable extends AbstractTable implements ScannableTable {
    private static final int BATCH_SIZE = 1000;
    private final RelDataType rowType;
    private final List<Object[]> data;

    private final Statistic statistic;

    CustomTable(RelDataType rowType, List<Object[]> data, Statistic statistic) {
        this.rowType = rowType;
        this.data = data;
        this.statistic = statistic;
    }

    @Override
    public Statistic getStatistic() {
        return this.statistic;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return null;
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return rowType;
    }

    public static CustomTable fromJdbcTable(Table jdbcTable, String tableName,
            DataSource dataSource,
            RelDataTypeFactory typeFactory) {
        RelDataType rowType = jdbcTable.getRowType(new JavaTypeFactoryImpl());
        List<Object[]> data = new ArrayList<>();
        Statistic statistic = new CustomStatistic(150);
        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {

            // Set fetch size for batch loading
            stmt.setFetchSize(BATCH_SIZE);

            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                if (rs.next()) {
                    double rowCount = rs.getDouble(1);
                    statistic = new CustomStatistic(rowCount);
                }
            }

            // try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            // int columnCount = rs.getMetaData().getColumnCount();
            // int rowCount = 0;

            // while (rs.next()) {
            // Object[] row = new Object[columnCount];
            // for (int i = 0; i < columnCount; i++) {
            // row[i] = rs.getObject(i + 1);
            // }
            // data.add(row);

            // rowCount++;
            // if (rowCount % BATCH_SIZE == 0) {
            // System.out.println("Loaded " + rowCount + " rows from " + tableName);
            // }
            // }
            // }
        } catch (SQLException e) {
            throw new RuntimeException("Error loading data from JDBC table: " +
                    e.getMessage(), e);
        }

        return new CustomTable(rowType, data, statistic);
    }
}
