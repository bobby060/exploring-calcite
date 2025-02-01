package edu.cmu.cs.db.calcite_app.app.rules;

import org.apache.calcite.adapter.jdbc.JdbcTableScan;

import javax.annotation.Nullable;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.enumerable.EnumerableTableScanRule;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.Convention;

/**
 * Rule to convert JdbcTableScan to LogicalTableScan.
 */
public class JdbcToLogicalTableScanRule extends ConverterRule {
    // public static final JdbcToLogicalTableScanRule INSTANCE = new
    // JdbcToLogicalTableScanRule(DEFAULT_CONFIG);

    /** Default configuration. */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(JdbcTableScan.class,
                    r -> true,
                    JdbcConvention.of(PostgresqlSqlDialect.DEFAULT, null, "1"), EnumerableConvention.INSTANCE,
                    "JdbcToLogicalTableScanRule")
            .withRuleFactory(JdbcToLogicalTableScanRule::new);

    protected JdbcToLogicalTableScanRule(Config config) {
        super(config);
    }

    // public static final Config DEFAULT_CONFIG;

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        System.out.println("JdbcToLogicalTableScanRule");
        // JdbcTableScan scan = (JdbcTableScan) rel;
        JdbcTableScan scan = (JdbcTableScan) rel;
        return LogicalTableScan.create(
                scan.getCluster(),
                scan.getTable(),
                scan.getHints());

    }

    // public static class Config extends ConverterRule.Config {
    // public static final Config INSTANCE = Config.INSTANCE = new Config();

    // private Config() {
    // super(
    // JdbcTableScan.class,
    // (Convention) JdbcConvention.INSTANCE,
    // Convention.NONE,
    // "JdbcToLogicalTableScanRule");
    // }
    // }
}