package edu.cmu.cs.db.calcite_app.app;

import java.util.List;
import java.util.Properties;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.dialect.RedshiftSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.jdbc.CalciteConnection;

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
    FrameworkConfig frameworkConfig;
    CalciteConnection connection;
    RelOptCluster hepCluster;
    RelOptPlanner hepPlanner;

    /**
     * Initializes the context with the given root schema
     * /**
     * Initializes the context with the given root schema
     * 
     * @param rootSchema
     */
    public Context(String inputPath) throws Exception {

        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        this.typeFactory = typeFactory;

        Class.forName("org.apache.calcite.jdbc.Driver");

        // Create properties for the connection
        Properties props = new Properties();
        props.setProperty("caseSensitive", "false");

        // Create the connection using DriverManager
        Connection conn = DriverManager.getConnection("jdbc:calcite:", props);

        // Unwrap to CalciteConnection
        CalciteConnection calciteConnection = conn.unwrap(CalciteConnection.class);

        // Set the root schema
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        // CalciteSchema childSchema = CustomSchema.fromDuckDb(inputPath);

        // rootSchema.add("duckdb", childSchema.schema);
        CustomSchema.addTables(rootSchema, inputPath);

        System.out.println("Table Names in root: " + rootSchema.getTableNames());

        CalciteConnectionConfig connectionConfig = CalciteConnectionConfig.DEFAULT.set(
                CalciteConnectionProperty.CASE_SENSITIVE,
                "false");

        this.connectionConfig = connectionConfig;

        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                List.of(""), // List of schema paths to search
                typeFactory,
                connectionConfig);

        this.connection = calciteConnection;

        // this.connection.setSchema("duckdb");

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

        this.hepPlanner = ProgramBuilder.buildHeuristicOptimizer();

        this.hepCluster = RelOptCluster.create(hepPlanner, rexBuilder);

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
                RedshiftSqlDialect.DEFAULT);

        this.rel2sqlConverter = rel2sqlconverter;

        this.frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.config().withCaseSensitive(false))
                .build();

        this.validator = validator;
        this.rootSchema = CalciteSchema.from(rootSchema);

        this.jdbcConvention = JdbcConvention.of(RedshiftSqlDialect.DEFAULT, null, "1");

    }
}
