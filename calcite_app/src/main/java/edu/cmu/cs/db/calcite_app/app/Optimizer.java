package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelRunner;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.RedshiftSqlDialect;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.ToLogicalConverter;

import org.apache.calcite.plan.RelOptListener;

public class Optimizer {
    private Context context;

    /**
     * Initializes the optimizer with data from the input path
     * 
     * @param inputPath path to data
     */
    public Optimizer(String inputPath) throws Exception {
        this.context = new Context(inputPath);
    }

    /**
     * Optimizes the given SQL query
     * 
     * @param filepath
     * @param outPath
     * @return
     * @throws Exception
     */
    public void optimize(String filepath, String outPath, boolean execute) throws Exception {

        String fileName = filepath.substring(filepath.lastIndexOf("/") + 1);
        String originalSql = Files.readString(new File(filepath).toPath());

        // Reinitalize planner - must do before each optimization otherwise it doesn't
        // actually optimize
        ProgramBuilder.resetPlanner(context.planner, false);

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

        // context.planner.addListener(listener);

        // Step 3: convert to relnode and add trait
        RelRoot relRoot = context.sql2relConverter.convertQuery(validatedNode, false, true);
        RelNode originalPlan = relRoot.rel;

        context.planner.setRoot(originalPlan);

        RelNode planWithTrait = context.planner.changeTraits(originalPlan,
                context.cluster.traitSet().replace(EnumerableConvention.INSTANCE));

        context.planner.setRoot(planWithTrait);

        // Step 4: Optimize with default planner
        long start = System.currentTimeMillis();

        RelNode bestExp = context.planner.findBestExp();
        long end = System.currentTimeMillis();
        System.out.println("Optimization time: " + (end - start) + "ms");

        // Step 5: save plans

        // Convert to logical also bc of bugs in calcite
        ToLogicalConverter toLogicalConverter = new ToLogicalConverter(
                RelBuilder.create(context.frameworkConfig));

        RelNode logicalNode = toLogicalConverter.visit(bestExp.stripped());

        // Decorrelate bc of bugs in calcite
        RelNode decorrelatedOutput = RelDecorrelator.decorrelateQuery(logicalNode,
                RelBuilder.create(context.frameworkConfig));

        // Save logical version to sql
        SqlNode optimizedSqlNode = this.context.rel2sqlConverter
                .visitRoot(decorrelatedOutput)
                .asStatement();

        String finalSql = optimizedSqlNode.toSqlString(RedshiftSqlDialect.DEFAULT).getSql();

        // Write the original SQL, original plan, optimized plan, and optimized SQL to
        Files.writeString(new File(outPath + "/" + fileName).toPath(), originalSql);
        SerializePlan(originalPlan, new File(outPath + '/' + fileName.substring(0, fileName.length() - 4) + ".txt"));
        SerializePlan(bestExp.stripped(),
                new File(outPath + '/' + fileName.substring(0, fileName.length() - 4) + "_optimized.txt"));
        Files.writeString(
                new File(outPath + '/' + fileName.substring(0, fileName.length() - 4) + "_optimized.sql").toPath(),
                finalSql);

        // Execute if not one of the blacklisted queries
        if (execute) {

            System.out.println("prepping statement");

            // Reset planner. The planner maintains some internal state that causes problems
            // if we try to reoptimize the same node
            ProgramBuilder.resetPlanner(context.planner, true);

            // To avoid any carryover from previous optimizations, convert from SQL again
            RelRoot relRoot2 = context.sql2relConverter.convertQuery(validatedNode, false, true);
            RelNode originalPlan2 = relRoot2.rel;

            context.planner.setRoot(originalPlan2);

            // For running inside calcite, we get better performance if we optimize the
            // decorrelated query (but not in duckdb, which is weird)
            RelNode decorrelatedNode = RelDecorrelator.decorrelateQuery(originalPlan2,
                    RelBuilder.create(context.frameworkConfig));

            RelNode planWithTrait2 = context.planner.changeTraits(decorrelatedNode,
                    context.cluster.traitSet().replace(EnumerableConvention.INSTANCE));

            // Reoptimize for execution

            start = System.currentTimeMillis();
            context.planner.setRoot(planWithTrait2);

            RelRunner runner = context.connection.unwrap(RelRunner.class);

            // Optimization happens again while statement is prepared
            PreparedStatement statement = runner.prepareStatement(planWithTrait2);

            try {
                System.out.println("executing statement");

                ResultSet resultSet = statement.executeQuery();

                SerializeResultSet(resultSet, new File(outPath + '/' + fileName.substring(0,
                        fileName.length() - 4) +
                        "_results.csv"));

                end = System.currentTimeMillis();
                System.out.println("Time taken in calcite: " + (end - start) + "ms");

            } catch (SQLException e) {
                System.out.println("Error executing statement: " + e.getMessage());
                Files.writeString(new File(outPath + '/' + fileName.substring(0,
                        fileName.length() - 4) +
                        "_results.csv").toPath(), e.getMessage());
            }
        } else {
            System.out.println("Skipping execution, on blacklist");
        }

        return;
    }

    /**
     * Serializes the plan to a file
     * 
     * @param relNode
     * @param outputPath
     * @throws IOException
     */
    private static void SerializePlan(RelNode relNode, File outputPath) throws IOException {
        Files.writeString(outputPath.toPath(),
                RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));
    }

    /**
     * Serializes the result set to a file
     * 
     * @param resultSet
     * @param outputPath
     * @throws SQLException
     * @throws IOException
     */
    private static void SerializeResultSet(ResultSet resultSet, File outputPath) throws SQLException, IOException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        StringBuilder resultSetString = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1) {
                resultSetString.append(",");
            }
            resultSetString.append(metaData.getColumnName(i));
        }
        resultSetString.append("\n");
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1) {
                    resultSetString.append(",");
                }
                String s = resultSet.getString(i);
                s = s.replace("\n", "\\n");
                s = s.replace("\r", "\\r");
                s = s.replace("\"", "\"\"");
                resultSetString.append("\"");
                resultSetString.append(s);
                resultSetString.append("\"");
            }
            resultSetString.append("\n");
        }
        Files.writeString(outputPath.toPath(), resultSetString.toString());
    }

}
