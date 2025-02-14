package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.logical.ToLogicalConverter;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelRunners;

public class Optimizer {
    private Context context;

    /**
     * Initializes the optimizer with the given root schema
     * 
     * @param rootSchema
     */
    public Optimizer(String inputPath) throws Exception {
        this.context = new Context(inputPath);
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

        String fileName = filepath.substring(filepath.lastIndexOf("/") + 1);
        String originalSql = Files.readString(new File(filepath).toPath());

        // Step 1: Use SqlParser to convert SQL string to SQLNode
        SqlNode sqlNode;
        try {
            sqlNode = SqlParser.create(originalSql, SqlParser.config()
                    .withCaseSensitive(false)).parseQuery();
        } catch (SqlParseException e) {
            throw new Exception("Error parsing SQL: " + e.getMessage());
        }

        // Step 2: Use SQL validator to validate tree
        SqlNode validatedNode = context.validator.validate(sqlNode);

        // Step 3: convert to relnode and add trait

        // Convert SQL to Rel
        RelRoot relRoot = context.sql2relConverter.convertQuery(validatedNode, false, true);
        RelNode originalPlan = relRoot.rel;

        context.planner.setRoot(originalPlan);

        RelNode planWithTrait = context.planner.changeTraits(originalPlan,
                context.cluster.traitSet().replace(EnumerableConvention.INSTANCE));

        context.planner.setRoot(planWithTrait);

        // Step 4: Optimize with default planner
        RelNode bestExp = context.planner.findBestExp();

        // Step 5: Run

        // Connection
        // Connection connection = context.connection.unwrap(Connection.class);

        // DataContext dataContext = DataContext.create(connection);

        // PreparedStatement statement = RelRunners.run(bestExp.stripped());

        // ResultSet resultSet = statement.executeQuery();

        // SerializeResultSet(resultSet,
        // new File(out_path + '/' + file_name.substring(0, file_name.length() - 4) +
        // "_results.txt"));

        // Decorrelate bc of bugs in calcite
        RelNode decorrelatedNode = RelDecorrelator.decorrelateQuery(bestExp.stripped(),
                RelBuilder.create(context.frameworkConfig));

        // But save logical version to sql
        SqlNode optimizedSqlNode = this.context.rel2sqlConverter
                .visitRoot(decorrelatedNode.stripped())
                .asStatement();

        String final_sql = optimizedSqlNode.toSqlString(MysqlSqlDialect.DEFAULT).getSql();

        // Write the original SQL, original plan, optimized plan, and optimized SQL to
        Files.writeString(new File(out_path + "/" + fileName).toPath(), originalSql);
        SerializePlan(originalPlan, new File(out_path + '/' + fileName.substring(0, fileName.length() - 4) + ".txt"));
        SerializePlan(bestExp.stripped(),
                new File(out_path + '/' + fileName.substring(0, fileName.length() - 4) + "_optimized.txt"));
        Files.writeString(
                new File(out_path + '/' + fileName.substring(0, fileName.length() - 4) + "_optimized.sql").toPath(),
                final_sql);

        PreparedStatement statement = context.connection.unwrap(RelRunner.class).prepareStatement(bestExp);

        ResultSet resultSet = statement.executeQuery();

        SerializeResultSet(resultSet, new File(out_path + '/' + fileName.substring(0, fileName.length() - 4) +
                "_results.csv"));

        return final_sql;
    }

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

}
