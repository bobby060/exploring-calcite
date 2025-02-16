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

import org.apache.calcite.plan.RelOptRule;

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
    public String optimize(String filepath, String out_path, boolean execute) throws Exception {

        String fileName = filepath.substring(filepath.lastIndexOf("/") + 1);
        String originalSql = Files.readString(new File(filepath).toPath());

        // Reinitalize planner - must do before each optimization otherwise it doesn't
        // actually optimize
        ProgramBuilder.resetPlanner(context.planner);

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

        RelOptListener listener = new RelOptListener() {
            @Override
            public void relEquivalenceFound(RelEquivalenceEvent event) {
                // System.out
                // .println(
                // "relEquivalenceFound: " + (event.getRel() != null ? event.getRel().explain()
                // : "null"));
            }

            @Override
            public void relDiscarded(RelDiscardedEvent event) {
                System.out.println("relDiscarded: ");
            }

            @Override
            public void ruleAttempted(RuleAttemptedEvent event) {
                // System.out.println("ruleAttempted: " +
                // event.getRuleCall().getRule().getClass().getSimpleName());
            }

            @Override
            public void ruleProductionSucceeded(RuleProductionEvent event) {
                System.out.println(
                        "ruleProductionSucceeded: " + event.getRuleCall().getRule().getClass().getSimpleName());
                System.out.println(
                        "transformed to : " + (event.getRel() != null ? event.getRel().explain() : "null"));
                System.out.println(
                        "from: " + (event.getRuleCall().rels[0] != null ? event.getRuleCall().rels[0].explain()
                                : "null"));
            }

            @Override
            public void relChosen(RelChosenEvent event) {
                // System.out.println("relChosen: " + (event.getRel() != null ?
                // event.getRel().explain() : "null"));
            }
        };

        // context.planner.addListener(listener);

        // Step 3: convert to relnode and add trait

        // Convert SQL to Rel
        RelRoot relRoot = context.sql2relConverter.convertQuery(validatedNode, false, true);
        RelNode originalPlan = relRoot.rel;

        RelNode decorrelatedNode = RelDecorrelator.decorrelateQuery(originalPlan,
                RelBuilder.create(context.frameworkConfig));

        // System.out.println("decorrelatedNode: " + decorrelatedNode.explain());

        context.planner.setRoot(decorrelatedNode);

        RelNode planWithTrait = context.planner.changeTraits(decorrelatedNode,
                context.cluster.traitSet().replace(EnumerableConvention.INSTANCE));

        // ProgramBuilder.resetPlanner(context.planner);

        context.planner.setRoot(planWithTrait);

        // Step 4: Optimize with default planner
        long start = System.currentTimeMillis();
        RelNode bestExp = context.planner.findBestExp();
        long end = System.currentTimeMillis();
        System.out.println("Optimization time: " + (end - start) + "ms");

        // Step 5: save plans

        // Decorrelate bc of bugs in calcite

        // Convert to logical also bc of bugs in calcite
        ToLogicalConverter toLogicalConverter = new ToLogicalConverter(
                RelBuilder.create(context.frameworkConfig));

        RelNode logicalNode = toLogicalConverter.visit(bestExp.stripped());

        // But save logical version to sql
        SqlNode optimizedSqlNode = this.context.rel2sqlConverter
                .visitRoot(logicalNode)
                .asStatement();

        String final_sql = optimizedSqlNode.toSqlString(RedshiftSqlDialect.DEFAULT).getSql();

        // Write the original SQL, original plan, optimized plan, and optimized SQL to
        Files.writeString(new File(out_path + "/" + fileName).toPath(), originalSql);
        SerializePlan(originalPlan, new File(out_path + '/' + fileName.substring(0, fileName.length() - 4) + ".txt"));
        SerializePlan(bestExp.stripped(),
                new File(out_path + '/' + fileName.substring(0, fileName.length() - 4) + "_optimized.txt"));
        Files.writeString(
                new File(out_path + '/' + fileName.substring(0, fileName.length() - 4) + "_optimized.sql").toPath(),
                final_sql);

        if (execute) {

            System.out.println("prepping statement");

            ProgramBuilder.resetPlanner(context.planner);

            // Reoptimize for execution

            start = System.currentTimeMillis();
            context.planner.setRoot(planWithTrait);

            RelRunner runner = context.connection.unwrap(RelRunner.class);

            PreparedStatement statement = runner.prepareStatement(planWithTrait);

            try {
                System.out.println("executing statement");

                ResultSet resultSet = statement.executeQuery();

                SerializeResultSet(resultSet, new File(out_path + '/' + fileName.substring(0,
                        fileName.length() - 4) +
                        "_results.csv"));

                end = System.currentTimeMillis();
                System.out.println("Time taken in calcite: " + (end - start) + "ms");

            } catch (SQLException e) {
                System.out.println("Error executing statement: " + e.getMessage());
                Files.writeString(new File(out_path + '/' + fileName.substring(0,
                        fileName.length() - 4) +
                        "_results.csv").toPath(), e.getStackTrace().toString());
            }
        } else {
            System.out.println("Skipping execution, on blacklist");
        }

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
