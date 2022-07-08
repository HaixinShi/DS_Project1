package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Scan protected(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {

  protected val scannable: ColumnStore = tableToStore(
    table.unwrap(classOf[ScannableTable])
  ).asInstanceOf[ColumnStore]

  /**
   * @inheritdoc
   */
  def execute(): IndexedSeq[Column] = {
    val cols = (0 until getRowType.getFieldCount).map(i => scannable.getColumn(i))
    /*
    println("enter scan-1")
    println(cols.map(e => e.toIterable.toIndexedSeq))
    println("enter scan-2")
    println(cols.map(e => e.toIterable.toIndexedSeq) :+ (0 until cols.head.size).map(_ => true))
    println("enter scan-3")*/
    cols.map(e => e.toIterable.toIndexedSeq) :+ (0 until cols.head.size).map(_ => true)
    /*
    println("leav scan")
    ret*/
  }
}
