package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcToEnumerableConverterRule;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import edu.cmu.cs.db.calcite_app.app.rules.JdbcToLogicalTableScanRule;
import org.apache.calcite.plan.volcano.VolcanoPlanner;

/**
 * ProgramBuilder class for building a HepProgram
 * 
 * The purpose of pulling this class out is to provide an easy place to modify
 * the rule set
 */
public class ProgramBuilder {

    /**
     * Builds a HepProgram
     * 
     * @return HepProgram
     */
    protected static HepProgram buildHep() {
        HepProgramBuilder programBuilder = new HepProgramBuilder();

        // programBuilder.addRuleInstance(JdbcToLogicalTableScanRule.DEFAULT_CONFIG.toRule());

        // programBuilder.addRuleInstance(JdbcToEnumerableConverterRule.create(jdbcConvention));

        // programBuilder.addRuleClass(SubQueryRemoveRule.class);

        // programBuilder.addRuleInstance(CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS);
        // programBuilder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);
        // programBuilder.addRuleInstance(CoreRules.JOIN_CONDITION_PUSH);
        // programBuilder.addRuleInstance(CoreRules.PROJECT_FILTER_TRANSPOSE);
        // programBuilder.addRuleInstance(CoreRules.SORT_REMOVE);
        // programBuilder.addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY);

        // // Filter push downs
        // programBuilder.addRuleInstance(CoreRules.FILTER_CORRELATE);
        // programBuilder.addRuleInstance(CoreRules.FILTER_INTO_JOIN);

        programBuilder.addRuleInstance(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        programBuilder.addRuleInstance(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
        programBuilder.addRuleInstance(EnumerableRules.ENUMERABLE_CORRELATE_RULE);
        programBuilder.addRuleInstance(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);

        // programBuilder.addRuleCollection(EnumerableRules.ENUMERABLE_RULES);
        // planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
        programBuilder.addRuleInstance(EnumerableRules.ENUMERABLE_SORT_RULE);
        // planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
        programBuilder.addRuleInstance(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        programBuilder.addRuleInstance(EnumerableRules.ENUMERABLE_FILTER_RULE);

        programBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);

        return programBuilder.build();

    }

    /**
     * Builds a heuristic optimizer
     */
    private RelOptPlanner buildHeuristicOptimizer() {

        HepProgram program = ProgramBuilder.buildHep();

        HepPlanner planner = new HepPlanner(program);

        // planner.changeTraits
        // planner.addRelTraitDef(EnumerableConvention.INSTANCE.getTraitDef());
        // planner.addRelTraitDef(this.jdbcConvention.getTraitDef());
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        // planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        // // programBuilder.addRuleCollection(EnumerableRules.ENUMERABLE_RULES);
        // planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
        // planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
        // planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
        // planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        // planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);

        // planner.addRelTraitDef(CallingConvention.;

        return planner;

    }

    protected static VolcanoPlanner buildTransFormmToEnumerablePlanner() {

        VolcanoPlanner planner = new VolcanoPlanner();

        for (RelOptRule rule : EnumerableRules.ENUMERABLE_RULES) {
            // if (rule != EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE) {
            planner.addRule(rule);
            // }
        }

        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        return planner;

    }

    protected static VolcanoPlanner buildVolcanoPlanner() {
        VolcanoPlanner planner = new VolcanoPlanner();
        // planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        // // planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
        // planner.addRule(EnumerableRules.ENUMERABLE_CORRELATE_RULE);
        // planner.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        // planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
        // planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        // planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);

        planner.addRule(CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS);
        planner.addRule(CoreRules.FILTER_INTO_JOIN);
        planner.addRule(CoreRules.JOIN_CONDITION_PUSH);
        planner.addRule(CoreRules.PROJECT_FILTER_TRANSPOSE);
        planner.addRule(CoreRules.SORT_REMOVE);
        planner.addRule(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY);

        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        return planner;
    }
}
