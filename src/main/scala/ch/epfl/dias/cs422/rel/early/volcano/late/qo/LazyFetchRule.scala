package ch.epfl.dias.cs422.rel.early.volcano.late.qo

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.builder.skeleton.logical.{LogicalFetch, LogicalStitch}
import ch.epfl.dias.cs422.helpers.qo.rules.skeleton.LazyFetchRuleSkeleton
import ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
import ch.epfl.dias.cs422.helpers.store.late.rel.late.volcano.LateColumnScan
import ch.epfl.dias.cs422.rel.early.volcano.{Filter, late}
import ch.epfl.dias.cs422.rel.early.volcano.late.Fetch
import ch.epfl.dias.cs422.rel.early.volcano.late.Stitch
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRuleCall, RelRule}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalFilter
import org.apache.calcite.rex.RexNode

import scala.collection.View


/**
  * RelRule (optimization rule) that finds an operator that stitches a new column
  * to the late materialized tuple and transforms stitching into a fetch operator.
  *
  * To use this rule: LazyFetchRule.Config.DEFAULT.toRule()
  *
  * @param config configuration parameters of the optimization rule
  */
class LazyFetchRule protected (config: RelRule.Config)
  extends LazyFetchRuleSkeleton(
    config
  ) {
  override def onMatchHelper(call: RelOptRuleCall): RelNode = {
    //println("------original----------1")
    //println(call.getRelList)
    //println("------original----------2")

    val stitch : LogicalStitch = call.rel(0)
    val left = stitch.getLeft.asInstanceOf[HepRelVertex].getCurrentRel//call.rel(1)
    val right = stitch.getRight.asInstanceOf[HepRelVertex].getCurrentRel
    //val columnScan: LateColumnScan = stitch.getRight.asInstanceOf[LateColumnScan]//call.rel(2)
    //call.rels.last.asInstanceOf[LateColumnScan]
    //println("----------------2")
    /*
    //var projects : java.util.List[_<:RexNode] = null
    //var c : Class[_ <: LogicalFetch] = null
    //val fetch = new LogicalFetch(stitch, stitch.getRowType, columnScan.getColumn, None)
    //val c: Class[_ <: LogicalFetch] = Fetch.getClass*/
    val fetch = new LogicalFetch(left, right.getRowType, right.asInstanceOf[LateColumnScan].getColumn, None)
    //val fetch = new LogicalFetch(filter, columnScan.getRowType, columnScan.getColumn, None)
    /*
    println(call.getRelList)
    val copy = stitch.copy(filter, columnScan)
    println(copy.getRowType)*/
    /*
    filter.copy(filter.getTraitSet(),columnScan, filter.getCondition())
    call.transformTo(stitch.copy(filter.copy(filter.getTraitSet(),filter, filter.getCondition()), columnScan))
    println(call.getRelList)*/
    fetch
  }
}

object LazyFetchRule {

  /**
    * Instance for a [[LazyFetchRule]]
    */
  val INSTANCE = new LazyFetchRule(
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
              b2.operand(classOf[LateColumnScan])
              // of any inputs
              .anyInputs()
          )
      )
  )
}
