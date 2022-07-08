package ch.epfl.dias.cs422.rel.early.volcano.late.qo

import ch.epfl.dias.cs422.helpers.builder.skeleton.logical.{LogicalFetch, LogicalStitch}
import ch.epfl.dias.cs422.helpers.qo.rules.skeleton.LazyFetchFilterRuleSkeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilLateTuple, Tuple}
import ch.epfl.dias.cs422.helpers.store.late.LateStandaloneColumnStore
import ch.epfl.dias.cs422.helpers.store.late.rel.late.volcano.LateColumnScan
import org.apache.calcite.plan.{RelOptRuleCall, RelOptUtil, RelRule}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rex.RexNode

import java.util

/**
  * RelRule (optimization rule) that finds a reconstruct operator that
  * stitches a filtered column (scan then filter) with the late materialized
  * tuple and transforms stitching into a fetch operator followed by a filter.
  *
  * To use this rule: LazyFetchProjectRule.Config.DEFAULT.toRule()
  *
  * @param config configuration parameters of the optimization rule
  */
/*
class MyColumn(column:LateColumnScan, filter:LogicalFilter) extends LateColumnScan(
  column.getCluster,
  column.getTraitSet,
  column.getTable,
  column.tableToStore,
  column.colName){

  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(filter.getCondition, column.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }
  private var current = 0;
  private lazy val col = scannable.getColumn
  override def next(): Option[LateTuple] = {
    if (current < scannable.getRowCount) {
      val v = col(current)
      val vid = current
      current += 1
      predicate(IndexedSeq(v)) match {
        case true => Option(LateTuple(vid, IndexedSeq(v)))
        case false => next()
      }
    } else {
      NilLateTuple
    }
  }
}*/
/*
class MyColumnStore(column:LateColumnScan, filter:LogicalFilter) extends
  LateStandaloneColumnStore(
  column.getColumn.getColumn.asInstanceOf[RelOperator.Column],
  column.getColumn.getRowCount.asInstanceOf[Long],
  column.getColumn.getColumnIndex.asInstanceOf[Int]){

}*/
class LazyFetchFilterRule protected (config: RelRule.Config)
  extends LazyFetchFilterRuleSkeleton(
    config
  ) {

  override def onMatchHelper(call: RelOptRuleCall): RelNode = {
    println("enter filter")
    println(call.getRelList)
    val stitch_1 : LogicalStitch = call.rel(0)
    val stitch_2: LogicalStitch = call.rel(1)
    val filter: LogicalFilter = call.rel(2)
    val columnScan: LateColumnScan = call.rel(3)
    //call.rels.last.asInstanceOf[LateColumnScan]
    //println("----------------2")
    /*
    //var projects : java.util.List[_<:RexNode] = null
    //var c : Class[_ <: LogicalFetch] = null
    //val fetch = new LogicalFetch(stitch, stitch.getRowType, columnScan.getColumn, None)
    //val c: Class[_ <: LogicalFetch] = Fetch.getClass*/
    //val new_column = new MyColumn(columnScan, filter)
    /*
    var project = new util.LinkedList[RexNode]()
    project.add(filter.getCondition)*/
    //val project:java.util.List[org.apache.calcite.rex.RexNode] = IndexedSeq(filter.getCondition)
    //project +:= filter.getCondition
    val fetch = new LogicalFetch(stitch_2, columnScan.getRowType, columnScan.getColumn, None)
    println("leave filter")
    /*
    val column = new LateStandaloneColumnStore(
      columnScan.getColumn.getColumn.asInstanceOf[RelOperator.Column],
      columnScan.getColumn.getRowCount.asInstanceOf[Long],
      columnScan.getColumn.getColumnIndex.asInstanceOf[Int])
      */
    //val fetch_2 = new LogicalFetch(stitch_1, columnScan.getRowType, columnScan.getColumn, None)

    val condition = filter.getCondition.accept(new RelOptUtil.RexInputConverter(
      filter.getCluster.getRexBuilder,
      columnScan.getRowType.getFieldList,
      fetch.getRowType.getFieldList,
      Array.fill[Int](columnScan.getRowType.getFieldList.size)(fetch.getRowType.getFieldList.size() - 1)))
    //LogicalFetch.create(fetch, condition)
    //filter.getCondition.a
    //println("filter original condition ")
    //println(filter.getCondition)
    //println("new condition")
    //println(condition)
    LogicalFilter.create(fetch, condition)
    //call.rel(0)
  }
}

object LazyFetchFilterRule {

  /**
    * Instance for a [[LazyFetchFilterRule]]
    */
  val INSTANCE = new LazyFetchFilterRule(
    // By default, get an empty configuration
    RelRule.Config.EMPTY
      // and match:
      .withOperandSupplier((b: RelRule.OperandBuilder) =>
        // A node of class classOf[LogicalStitch]
        b.operand(classOf[LogicalStitch])
          // that has inputs:
          .inputs(
            b1 =>
              // A node that is a LateColumnScan
              b1.operand(classOf[RelNode])
                // of any inputs
                .anyInputs(),
            b2 =>
              // A node that is a LateColumnScan
              b2.operand(classOf[LogicalFilter])
                // of any inputs
                .oneInput(
                  b3 =>
                    b3.operand(classOf[LateColumnScan])
                      .anyInputs()
                )
          )
      )
  )
}
