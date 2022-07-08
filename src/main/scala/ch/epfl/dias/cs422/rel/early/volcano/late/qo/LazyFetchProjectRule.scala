package ch.epfl.dias.cs422.rel.early.volcano.late.qo

import ch.epfl.dias.cs422.helpers.builder.skeleton.logical.{LogicalFetch, LogicalStitch}
import ch.epfl.dias.cs422.helpers.qo.rules.skeleton.LazyFetchProjectRuleSkeleton
import ch.epfl.dias.cs422.helpers.store.late.rel.late.volcano.LateColumnScan
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRuleCall, RelRule}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalProject

/**
  * RelRule (optimization rule) that finds a reconstruct operator that stitches
  * a new expression (projection over one column) to the late materialized tuple
  * and transforms stitching into a fetch operator with projections.
  *
  * To use this rule: LazyFetchProjectRule.Config.DEFAULT.toRule()
  *
  * @param config configuration parameters of the optimization rule
  */
class LazyFetchProjectRule protected (config: RelRule.Config)
  extends LazyFetchProjectRuleSkeleton(
    config
  ) {

  override def onMatchHelper(call: RelOptRuleCall): RelNode = {
    //println("----project------------1")
    //println(call.getRelList)
    val stitch : LogicalStitch = call.rel(0)
    val left = stitch.getLeft.asInstanceOf[HepRelVertex].getCurrentRel//call.rel(1)
    val right = stitch.getRight.asInstanceOf[HepRelVertex].getCurrentRel
    val fetch = new LogicalFetch(left, call.rels.last.getRowType, call.rels.last.asInstanceOf[LateColumnScan].getColumn, Some(right.asInstanceOf[LogicalProject].getProjects))
    //val projects = right.asInstanceOf[LogicalProject].getProjects
    //println("get_projects")
    //println(projects)
    //val real_project:LogicalProject = call.rel(2)
    //val real_projects = real_project.getProjects
    //println("real_projects")
    //println(real_projects)
    //println("----project------------2")
    //call.rels(0)
    fetch
  }
}

object LazyFetchProjectRule {

  /**
    * Instance for a [[LazyFetchProjectRule]]
    */
  val INSTANCE = new LazyFetchProjectRule(
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
              b2.operand(classOf[LogicalProject])
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