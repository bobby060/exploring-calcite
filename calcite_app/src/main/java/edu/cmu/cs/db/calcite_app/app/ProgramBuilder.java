package edu.cmu.cs.db.calcite_app.app;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import java.util.ArrayList;

import org.apache.calcite.plan.RelOptListener;

import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.plan.RelOptRule;

// Rules
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.adapter.enumerable.EnumerableRules;

import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;

/**
 * ProgramBuilder class for building a program and rule sets
 * 
 * The purpose of pulling this class out is to provide an easy place to modify
 * the rule sets
 */
public class ProgramBuilder {

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
        rules.add(EnumerableRules.ENUMERABLE_VALUES_RULE);

        rules.add(EnumerableRules.ENUMERABLE_LIMIT_RULE);

        rules.add(EnumerableRules.ENUMERABLE_CALC_RULE);

        // Wrapper conversion rule. Might not actually need this
        rules.add(AbstractConverter.ExpandConversionRule.INSTANCE);

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

        // These rules make some timeout rn
        // rules.add(JoinPushThroughJoinRule.LEFT);
        // rules.add(JoinPushThroughJoinRule.RIGHT);

        // rules.add(CoreRules.SORT_JOIN_TRANSPOSE);
        rules.add(CoreRules.FILTER_PROJECT_TRANSPOSE);
        rules.add(CoreRules.FILTER_CORRELATE);
        // rules.add(CoreRules.PROJECT_FILTER_TRANSPOSE);

        // rules.add(CoreRules.JOIN_TO_CORRELATE);
        // rules.add(CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
        // rules.add(CoreRules.FILTER_INTO_JOIN_DUMB);

        // Potential adds
        // Join push down predicates to children

        rules.add(CoreRules.PROJECT_JOIN_TRANSPOSE); // maybe this
        rules.add(CoreRules.FILTER_INTO_JOIN);

        // rules.add(CoreRules.PROJECT_JOIN_TRANSPOSE);
        rules.add(CoreRules.FILTER_REDUCE_EXPRESSIONS);
        rules.add(CoreRules.JOIN_REDUCE_EXPRESSIONS);
        rules.add(CoreRules.PROJECT_REDUCE_EXPRESSIONS);
        // rules.add(CoreRules.FILTER_MERGE);
        rules.add(CoreRules.PROJECT_FILTER_TRANSPOSE);
        // rules.add(CoreRules.PROJECT_FILTER_TRANSPOSE_WHOLE_PROJECT_EXPRESSIONS);

        // rules.add(CoreRules.PROJECT_FILTER_TRANSPOSE_WHOLE_EXPRESSIONS);

        rules.add(CoreRules.JOIN_PUSH_EXPRESSIONS);
        rules.add(CoreRules.JOIN_CONDITION_PUSH);

        rules.add(CoreRules.JOIN_ASSOCIATE);
        rules.add(CoreRules.JOIN_COMMUTE);
        rules.add(CoreRules.JOIN_PUSH_EXPRESSIONS);
        rules.add(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES);

        rules.add(CoreRules.PROJECT_REMOVE);
        rules.add(CoreRules.PROJECT_MERGE);
        // rules.add(CoreRules.AGGREGATE_PROJECT_MERGE);
        // rules.add(CoreRules.PROJECT_TO_SEMI_JOIN); // This rule not good. Broke
        // capybara4
        // rules.add(CoreRules.JOIN)
        rules.add(CoreRules.SEMI_JOIN_FILTER_TRANSPOSE);
        // rules.add(CoreRules.SEMI_JOIN_PROJECT_TRANSPOSE);
        // rules.add(CoreRules.SORT_REMOVE);
        // rules.add(CoreRules.SORT_REMOVE_REDUNDANT);
        rules.add(CoreRules.AGGREGATE_REMOVE);
        rules.add(CoreRules.PROJECT_REMOVE);

        return rules;
    }

    /**
     * Builds a volcano planner
     * 
     * with Convention and RelCollation traits defined. Doesn't add rules, need to
     * reset or manually add rules
     * 
     * @return volcano planner
     */
    protected static VolcanoPlanner buildVolcanoPlanner() {
        VolcanoPlanner planner = new VolcanoPlanner();

        planner.setTopDownOpt(true);

        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        return planner;
    }

    /**
     * Resets the planner to the initial state
     * 
     * @param planner      planner to reset
     * @param isRelRunnner if we are resetting in preparation for running in-memory
     *                     optimization
     */
    public static void resetPlanner(RelOptPlanner planner, boolean isRelRunnner) {
        planner.clear();

        for (RelOptRule rule : ProgramBuilder.enumerableRules()) {
            planner.addRule(rule);
        }

        // Rules only forfor in-memory execution
        if (isRelRunnner) {
            planner.addRule(CoreRules.PROJECT_TO_SEMI_JOIN);
        }

        for (RelOptRule rule : ProgramBuilder.coreRules()) {
            planner.addRule(rule);
        }
    }

    // TODO make our own statistics counter for what rules applied

    /**
     * Builds a listener that we can customize to print out information about query
     * planning
     * 
     * @return listener
     */
    public static RelOptListener buildListener() {
        return new RelOptListener() {
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
    }
}
