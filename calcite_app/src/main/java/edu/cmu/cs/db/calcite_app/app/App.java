package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import javax.sql.DataSource;
import java.nio.file.Files;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.linq4j.tree.DefaultExpression;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.jdbc.CalciteSchema;
import java.sql.Connection;
import java.sql.DriverManager;

public class App {
    private static void SerializePlan(RelNode relNode, File outputPath) throws IOException {
        Files.writeString(outputPath.toPath(),
                RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
    }

    private static void SerializeResultSet(ResultSet resultSet, File outputPath) throws SQLException, IOException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder resultSetString = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1) {
                resultSetString.append(", ");
            }
            resultSetString.append(metaData.getColumnName(i));
        }
        resultSetString.append("\n");
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    resultSetString.append(", ");
                }
                resultSetString.append(resultSet.getString(i));
            }
            resultSetString.append("\n");
        }
        Files.writeString(outputPath.toPath(), resultSetString.toString());
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage: java -jar App.jar <arg1> <arg2>");
            return;
        }

        // Feel free to modify this to take as many or as few arguments as you want.
        System.out.println("Running the app!");
        String arg1 = args[0];
        System.out.println("\tArg1: " + arg1);
        String arg2 = args[1];
        System.out.println("\tArg2: " + arg2);

        // Note: in practice, you would probably use
        // org.apache.calcite.tools.Frameworks.
        // That package provides simple defaults that make it easier to configure
        // Calcite.
        // But there's a lot of magic happening there; since this is an educational
        // project,
        // we guide you towards the explicit method in the writeup.

        // Connect to DuckDB
        String url = "jdbc:duckdb:../data.db";

        String schemaName = "duckdb";

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);

        String driverClassName = "org.duckdb.DuckDBDriver";
        DataSource dataSource = JdbcSchema.dataSource(url, driverClassName, null, null);

        Connection connection = dataSource.getConnection();

        System.out.println(connection.isClosed());

        JdbcSchema jdbcSchema = JdbcSchema.create(rootSchema.plus(), schemaName, dataSource, null, null);

        System.out.println("Loaded tables: " + jdbcSchema.getTableNames());

        rootSchema.add(schemaName, jdbcSchema);

        Optimizer optimizer = new Optimizer(rootSchema);
        // Iterate over target queries
        File queryDir = new File(args[0]);
        for (File file : queryDir.listFiles()) {
            System.out.println("Optimizing query: " + file.getName());
            String optimizedQuery = optimizer.optimize(file.getPath(), args[1]);
            System.out.println(optimizedQuery);
        }
    }
}
