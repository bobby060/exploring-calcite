package edu.cmu.cs.db.calcite_app.app;

import java.util.List;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.jdbc.CalciteSchema;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.adapter.jdbc.JdbcConvention;

/**
 * /**
 * Context class for the optimizer
 */
public class Context {
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
    JdbcConvention jdbcConvention;

    /**
     * Initializes the context with the given root schema
     * 
     * @param rootSchema
     */
    public Context(CalciteSchema rootSchema) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        this.typeFactory = typeFactory;

        CalciteConnectionConfig connectionConfig = CalciteConnectionConfig.DEFAULT.set(
                CalciteConnectionProperty.CASE_SENSITIVE,
                "false");

        this.connectionConfig = connectionConfig;

        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema,
                List.of("duck_db"), // List of schema paths to search
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

        RelOptPlanner planner = ProgramBuilder.buildVolcanoPlanner();

        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        this.cluster = cluster;
        this.planner = planner;

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(true) // Enable this deprecated feature bc it is the only thing that works.
                .withDecorrelationEnabled(true);

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

        this.jdbcConvention = JdbcConvention.of(PostgresqlSqlDialect.DEFAULT, null, "1");

    }
}
