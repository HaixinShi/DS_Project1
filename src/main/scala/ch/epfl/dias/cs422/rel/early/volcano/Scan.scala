package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.store.{RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */

/*
* In the context of this project you will store all data in-memory,
* thus removing the requirement for a buffer manager.
* In addition, you can assume that the records will consist of objects (Any class in Scala).
*
* Implement the operators based on the prototypes given in the skeleton code.
* */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )

  //private var prog = getRowType.getFieldList.asScala.map(_ => 0)
  private var row_cnt = -1;
  private var row_num = table.getRowCount;
  private val row_store= scannable.asInstanceOf[RowStore];
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    //we do nothing at open
    row_cnt = -1//necessary
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    //here we return a tuple if applicable
    if(row_cnt == row_num - 1){
      NilTuple
    }
    else{
      row_cnt = row_cnt + 1
      Option(row_store.getRow(row_cnt))
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    //do nothing here
  }
}