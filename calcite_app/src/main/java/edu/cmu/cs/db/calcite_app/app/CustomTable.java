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
import java.sql.SQLException;
import javax.sql.DataSource;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;

// import org.apache.parquet.hadoop.ParquetFileReader;
// import org.apache.parquet.hadoop.ParquetReader;
// import org.apache.parquet.hadoop.api.ReadSupport;
// import org.apache.parquet.hadoop.example.GroupReadSupport;
// import org.apache.parquet.avro.AvroParquetReader;
// import org.apache.parquet.avro.AvroParquetReader.Builder;
// import org.apache.parquet.column.page.PageReadStore;
// import org.apache.parquet.example.data.Group;
// import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
// import org.apache.parquet.format.converter.ParquetMetadataConverter;
// import org.apache.parquet.hadoop.metadata.ParquetMetadata;
// import org.apache.parquet.io.ColumnIOFactory;
// import org.apache.parquet.io.MessageColumnIO;
// import org.apache.parquet.schema.MessageType;
// import org.apache.parquet.io.RecordReader;

// import org.apache.avro.generic.GenericRecord;
// import org.apache.avro.reflect.ReflectData;
import org.apache.calcite.linq4j.Linq4j;

public class CustomTable extends AbstractTable implements ScannableTable {
    private static final int BATCH_SIZE = 1000;
    private final RelDataType rowType;
    private final ArrayList<Object[]> data;
    private final List<RelDataTypeField> fieldTypes;

    private final Statistic statistic;

    CustomTable(RelDataType rowType, ArrayList data, Statistic statistic) {
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
            stmt.setFetchSize(5000);
            stmt.setMaxRows(5000);

            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                if (rs.next()) {
                    double rowCount = rs.getDouble(1);
                    statistic = new CustomStatistic(rowCount);
                }
            }

            // int columnCount = stmt
            // .executeQuery("SELECT count(*) as No_of_Column FROM
            // information_schema.columns WHERE table_name ='"
            // + tableName + "'")
            // .getInt("No_of_Column");

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

    // Unused for now
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
    // // https://www.jofre.de/?p=1459
    // private static List<Object[]> fromParquetFile(String parquetPath,
    // RelDataTypeFactory typeFactory) {

    // List<Object[]> data = new ArrayList<>();

    // try {

    // Path path = new Path(parquetPath);

    // Configuration conf = new Configuration();

    // ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path,
    // ParquetMetadataConverter.NO_FILTER);
    // MessageType schema = readFooter.getFileMetaData().getSchema();
    // ParquetFileReader r = new ParquetFileReader(conf, path, readFooter);

    // // builder(path)
    // // .withDataModel(new ReflectData(Row.class.getClassLoader()))
    // // .withConf(conf)
    // // .build();

    // PageReadStore pages = null;
    // try {
    // while (null != (pages = r.readNextRowGroup())) {
    // final long rows = pages.getRowCount();
    // System.out.println("Number of rows: " + rows);

    // final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    // final RecordReader recordReader = columnIO.getRecordReader(pages, new
    // GroupRecordConverter(schema));
    // for (int i = 0; i < rows; i++) {
    // final Group g = recordReader.read().;
    // // data.add(g.().toArray());
    // // TODO Compare to System.out.println(g);
    // }
    // }
    // } finally {
    // r.close();
    // }
    // } catch (Exception e) {
    // throw new RuntimeException("Error reading Parquet file: " + e.getMessage(),
    // e);
    // }
    // return null;
    // }
}
