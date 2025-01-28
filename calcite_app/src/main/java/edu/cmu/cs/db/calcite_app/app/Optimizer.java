package edu.cmu.cs.db.calcite_app.app;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.plan.hep.HepProgram;

public class Optimizer {

    private CalciteSchema rootSchema;
    private Context context;
    private RelOptPlanner planner;

    public Optimizer(CalciteSchema rootSchema) {
        this.rootSchema = rootSchema;
        this.context = new Context(rootSchema);
        this.buildHeuristicOptimizer();
    }

    public String optimize(String filepath, String out_path) throws Exception {

        String file_name = filepath.substring(filepath.lastIndexOf("/") + 1);

        String sql = Files.readString(new File(filepath).toPath());

        System.out.println(filepath);

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
        SqlNode validatedNode = this.context.validator.validate(sqlNode);

        // System.out.println(validatedNode);

        // Step 3: convert to relnode

        // Convert SQL to Rel
        RelRoot relRoot = this.context.sql2relConverter.convertQuery(validatedNode, false, true);
        RelNode relNode = relRoot.rel;

        Files.writeString(
                new File(out_path + '/' + file_name.substring(0, out_path.length() - 4) + ".txt").toPath(),
                RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));

        // For debugging, print the relational algebra tree
        System.out.println("Relational algebra plan:");
        System.out.println(RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));

        // Step 4: Implement rule based optimizations

        System.out.println(relNode.getTraitSet());

        this.planner.setRoot(relNode);

        RelNode relNodeWithTraits = planner.changeTraits(
                relNode,
                this.context.cluster.traitSetOf(EnumerableConvention.INSTANCE));

        this.planner.setRoot(relNodeWithTraits);

        this.planner.findBestExp();
        // this.planner.changeTraits(relNode, relNode.getTraitSet());

        // Step 5: Improve with statistics

        // Step 6: Use RelToSqlConverter to convert sql for running in DuckDB

        // Write the optimized plan to a file
        Files.writeString(
                new File(out_path + '/' + file_name.substring(0, out_path.length() - 4) + "_optimized.txt").toPath(),
                RelOptUtil.dumpPlan("", relNode, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES));

        SqlNode optimizedSqlNode = this.context.rel2sqlConverter.visitRoot(this.planner.getRoot()).asStatement();

        String final_sql = optimizedSqlNode.toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();

        Files.writeString(
                new File(out_path + '/' + file_name.substring(0, out_path.length() - 4) + "_optimized.sql").toPath(),
                final_sql);

        return final_sql;
    }

    private void buildHeuristicOptimizer() {

        HepProgramBuilder programBuilder = new HepProgramBuilder();

        programBuilder.addRuleInstance(CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS);
        programBuilder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
        programBuilder.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);
        programBuilder.addRuleInstance(CoreRules.PROJECT_FILTER_TRANSPOSE);
        programBuilder.addRuleInstance(CoreRules.SORT_REMOVE);
        programBuilder.addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY);

        HepProgram program = programBuilder.build();

        HepPlanner planner = new HepPlanner(program);

        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        this.planner = planner;

    }

    private class Context {
        SqlParser parser;
        CalciteSchema rootSchema;
        CalciteCatalogReader catalogReader;
        CalciteConnectionConfig connectionConfig;
        RelDataTypeFactory typeFactory;
        SqlToRelConverter sql2relConverter;
        HepProgramBuilder programBuilder;
        RelToSqlConverter rel2sqlConverter;
        SqlValidator validator;
        RelOptPlanner planner;
        RelOptCluster cluster;

        public Context(CalciteSchema rootSchema) {
            RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
            this.typeFactory = typeFactory;

            CalciteConnectionConfig connectionConfig = CalciteConnectionConfig.DEFAULT.set(
                    CalciteConnectionProperty.CASE_SENSITIVE,
                    "false");

            this.connectionConfig = connectionConfig;

            CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                    rootSchema,
                    List.of("duckdb"), // List of schema paths to search
                    typeFactory,
                    connectionConfig);

            this.catalogReader = catalogReader;

            // Create the validator
            SqlValidator validator = SqlValidatorUtil.newValidator(
                    SqlStdOperatorTable.instance(),
                    catalogReader,
                    typeFactory,
                    SqlValidator.Config.DEFAULT);

            RexBuilder rexBuilder = new RexBuilder(typeFactory);

            HepProgramBuilder programBuilder = new HepProgramBuilder();
            RelOptPlanner planner = new HepPlanner(programBuilder.build());

            RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

            this.cluster = cluster;

            SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                    .withTrimUnusedFields(true)
                    .withExpand(false);

            SqlToRelConverter converter = new SqlToRelConverter(
                    null, // ViewExpander - null for now
                    validator,
                    catalogReader,
                    cluster,
                    StandardConvertletTable.INSTANCE,
                    converterConfig);

            this.sql2relConverter = converter;

            RelToSqlConverter rel2sqlconverter = new RelToSqlConverter(
                    PostgresqlSqlDialect.DEFAULT);

            this.rel2sqlConverter = rel2sqlconverter;

            this.validator = validator;
            this.rootSchema = rootSchema;
        }
    }

}
