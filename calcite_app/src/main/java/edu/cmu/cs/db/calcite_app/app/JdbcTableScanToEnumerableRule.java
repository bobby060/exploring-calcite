// // package edu.cmu.cs.db.calcite_app.app;

// import org.apache.calcite.plan.*;
// import org.apache.calcite.rel.convert.ConverterRule;
// import org.apache.calcite.adapter.jdbc.JdbcTableScan;
// import org.apache.calcite.rel.core.TableScan;
// import org.apache.calcite.rel.type.RelDataType;
// import org.apache.calcite.rex.RexBuilder;
// import org.apache.calcite.adapter.enumerable.EnumerableConvention;

// import java.util.List;

// public class JdbcTableScanToEnumerableRule extends ConverterRule {

// @Override
// public Convention getOutConvention() {
// return EnumerableConvention.INSTANCE;
// }

// @Override
// public RelNode convert(RelNode rel) {
// final JdbcTableScan scan = (JdbcTableScan) rel;
// final RelDataType rowType = scan.getRowType();
// final RexBuilder rexBuilder = scan.getCluster().getRexBuilder();

// // Create an EnumerableTableScan
// final TableScan enumerableScan = new TableScan(
// scan.getCluster(),
// scan.getTable(),
// rowType) {
// @Override
// public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
// return new TableScan(scan.getCluster(), scan.getTable(), rowType);
// }
// };

// // Set the traits to Enumerable
// return RelTraitSet.createEmpty()
// .replace(ConventionTraitDef.INSTANCE,
// ConventionTraitDef.INSTANCE.enumerable())
// .reduce(enumerableScan);
// }

// /** Rule configuration. */
// public interface Config extends RelRule.Config {
// Config DEFAULT = RelRule.Config.EMPTY.withOperandSupplier(b ->
// b.operand(JdbcTableScan.class).any());

// default Config withOperandSupplier(OperandQuery<RelNode> operandSupplier) {
// return withOperandSupplier(operandSupplier);
// }
// }
// }