package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;

import java.util.ArrayList;

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
     * Builds a heuristic optimizer for things we always want to do.
     */
    protected static RelOptPlanner buildHeuristicOptimizer() {

        HepProgram program = ProgramBuilder.buildHep();

        HepPlanner planner = new HepPlanner(program);

        // planner.changeTraits
        // planner.addRelTraitDef(EnumerableConvention.INSTANCE.getTraitDef());
        // planner.addRelTraitDef(this.jdbcConvention.getTraitDef());
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        for (RelOptRule rule : enumerableRules()) {
            planner.addRule(rule);
        }

        planner.addRule(AggregateReduceFunctionsRule.Config.DEFAULT.toRule());

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

    /**
     * List of conversion rules for the enumerable convention
     * 
     * @return list of rules
     */
    public static ArrayList<RelOptRule> enumerableRules() {

        ArrayList<RelOptRule> rules = new ArrayList<>();
        rules.add(EnumerableRules.ENUMERABLE_SORT_RULE);

        rules.add(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
        rules.add(EnumerableRules.ENUMERABLE_JOIN_RULE);
        rules.add(EnumerableRules.ENUMERABLE_CORRELATE_RULE);
        rules.add(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
        rules.add(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        rules.add(EnumerableRules.ENUMERABLE_FILTER_RULE);
        rules.add(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
        // rules.add(EnumerableRules.ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE);

        // rules.add(EnumerableRules.ENUMERABLE_ASOFJOIN_RULE);

        rules.add(EnumerableRules.ENUMERABLE_LIMIT_RULE);

        // rules.add(EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE);
        rules.add(EnumerableRules.ENUMERABLE_CALC_RULE);

        return rules;
    }

    /**
     * List of conversion rules used for transformation
     * 
     * @return list of rules
     */
    public static ArrayList<RelOptRule> coreRules() {

        ArrayList<RelOptRule> rules = new ArrayList<>();

        AggregateReduceFunctionsRule.Config myconfig = AggregateReduceFunctionsRule.Config.DEFAULT;

        myconfig = myconfig.withFunctionsToReduce(AggregateReduceFunctionsRule.Config.DEFAULT_FUNCTIONS_TO_REDUCE);

        rules.add(myconfig.toRule());
        rules.add(AggregateExpandDistinctAggregatesRule.Config.DEFAULT.toRule());

        // Let sorts get pushed into join, should allow mergejoin
        rules.add(CoreRules.SORT_JOIN_COPY);
        rules.add(CoreRules.SORT_PROJECT_TRANSPOSE);
        rules.add(CoreRules.SORT_JOIN_TRANSPOSE);
        rules.add(CoreRules.FILTER_CORRELATE);
        // rules.add(CoreRules.PROJECT_FILTER_VALUES_MERGE);
        rules.add(CoreRules.PROJECT_FILTER_TRANSPOSE);
        // rules.add(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
        // planner.addRule(CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE);
        rules.add(CoreRules.PROJECT_JOIN_TRANSPOSE);
        rules.add(CoreRules.FILTER_INTO_JOIN);
        rules.add(CoreRules.JOIN_PUSH_EXPRESSIONS);
        rules.add(CoreRules.JOIN_CONDITION_PUSH);

        // rules.add(CoreRules.JOIN_ASSOCIATE);
        rules.add(CoreRules.JOIN_COMMUTE);
        rules.add(CoreRules.JOIN_PUSH_EXPRESSIONS);
        rules.add(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES);

        rules.add(CoreRules.PROJECT_REMOVE);
        // rules.add(CoreRules.AGGREGATE_PROJECT_MERGE);
        rules.add(CoreRules.PROJECT_TO_SEMI_JOIN);
        rules.add(CoreRules.SEMI_JOIN_FILTER_TRANSPOSE);
        rules.add(CoreRules.SEMI_JOIN_PROJECT_TRANSPOSE);
        // rules.add(CoreRules.SORT_REMOVE);
        rules.add(CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY);
        rules.add(CoreRules.SORT_REMOVE_REDUNDANT);
        rules.add(CoreRules.AGGREGATE_REMOVE);
        rules.add(CoreRules.PROJECT_REMOVE);
        // rules.add(CoreRules.FILTER_MERGE);

        rules.add(CoreRules.PROJECT_FILTER_TRANSPOSE_WHOLE_EXPRESSIONS);

        // rules.add(AggregateReduceFunctionsRule.Config.DEFAULT.toRule());

        // rules.add(AbstractConverter.ExpandConversionRule.INSTANCE);

        return rules;
    }

    public static void resetPlanner(RelOptPlanner planner) {
        planner.clear();

        for (RelOptRule rule : enumerableRules()) {
            planner.addRule(rule);
        }

        for (RelOptRule rule : coreRules()) {
            planner.addRule(rule);
        }

    }

    protected static VolcanoPlanner buildVolcanoPlanner() {
        VolcanoPlanner planner = new VolcanoPlanner();

        planner.setTopDownOpt(true);

        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        return planner;
    }
}
