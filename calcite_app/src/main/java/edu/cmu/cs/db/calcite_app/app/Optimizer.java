package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.plan.RelTraitSet;

public class Optimizer {
    private Context context;

    /**
     * Initializes the optimizer with the given root schema
     * 
     * @param rootSchema
     */
    public Optimizer(CalciteSchema rootSchema) {
        this.context = new Context(rootSchema);
    }

    /**
     * Optimizes the given SQL query
     * 
     * @param filepath
     * @param out_path
     * @return
     * @throws Exception
     */
    public String optimize(String filepath, String out_path) throws Exception {

        String file_name = filepath.substring(filepath.lastIndexOf("/") + 1);

        String sql = Files.readString(new File(filepath).toPath());

        // Write the original SQL to a file
        Files.writeString(new File(out_path + "/" + file_name).toPath(), sql);

        // Step 1: Use SqlParser to convert SQL string to SQLNode
        SqlParser parser = SqlParser.create(sql, SqlParser.config()
                .withCaseSensitive(false));
        SqlNode sqlNode;

        try {
            sqlNode = parser.parseQuery();
        } catch (SqlParseException e) {
            throw new Exception("Error parsing SQL: " + e.getMessage());
        }

        // Step 2: Use SQL validator to validate tree
        SqlNode validatedNode = context.validator.validate(sqlNode);

        // Step 3: convert to relnode

        // Convert SQL to Rel
        RelRoot relRoot = context.sql2relConverter.convertQuery(validatedNode, false, true);
        RelNode relNode = relRoot.rel;

        SerializePlan(relNode, new File(out_path + '/' + file_name.substring(0, file_name.length() - 4) + ".txt"));
        // For debugging, print the relational algebra tree
        System.out.println("Relational algebra plan:");
        System.out.println(
                RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));

        // Step 4: Implement rule based optimizations
        RelOptCluster cluster = context.cluster;
        System.out.println(cluster.traitSet());
        RelTraitSet desiredTrait = cluster.traitSet().plus(EnumerableConvention.INSTANCE);

        System.out.println(desiredTrait.toString());

        context.planner.setRoot(relNode);

        RelNode relNode2 = context.planner.changeTraits(relNode, desiredTrait);

        context.planner.setRoot(relNode2);

        RelNode bestExp = context.planner.findBestExp();

        // Step 5: Improve with statistics

        // Step 6: Use RelToSqlConverter to convert sql for running in DuckDB

        // Write the optimized plan to a file
        SerializePlan(bestExp.stripped(),
                new File(out_path + '/' + file_name.substring(0, file_name.length() - 4) + "_optimized.txt"));

        SqlNode optimizedSqlNode = this.context.rel2sqlConverter
                .visitRoot(bestExp.stripped())
                .asStatement();

        String final_sql = optimizedSqlNode.toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();

        Files.writeString(
                new File(out_path + '/' + file_name.substring(0, file_name.length() - 4) + "_optimized.sql").toPath(),
                final_sql);

        return final_sql;
    }

    private static void SerializePlan(RelNode relNode, File outputPath) throws IOException {
        Files.writeString(outputPath.toPath(),
                RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
    }

    /**
     * Loads the schema from the given database path
     * 
     * @param db_path
     * @return CalciteSchema
     */
    protected static CalciteSchema loadJdbcSchema(String db_path) throws SQLException {

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

        CalciteSchema customSchema = CustomSchema.convertJdbcSchema(jdbcSchema);

        return customSchema;

    }

}
