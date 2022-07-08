package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import org.apache.calcite.rel.RelCollation

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {
  /**
    * Hint: See superclass documentation for info on collation i.e.
    * sort keys and direction
    */

  /**
   * @inheritdoc
   */
  def comparator (tuple1: Tuple, tuple2: Tuple): Boolean = {
    // Get the list of collations
    val collation_list = collation.getFieldCollations

    // Iterate over all collations
    for (i <- 0 until collation_list.size()) {
      // Get the ith collation from the list
      val coll = collation_list.get(i)
      // Convert the tuple elements at sorting key to comparables
      val t1_comparable = tuple1(coll.getFieldIndex).asInstanceOf[Comparable[Any]]
      val t2_comparable = tuple2(coll.getFieldIndex).asInstanceOf[Comparable[Any]]

      if (t1_comparable.compareTo(t2_comparable) < 0) {
        // This is the case where tuple1 < tuple2
        if (coll.direction.isDescending) {
          // Return false for descending collation
          return false
        } else {
          // Return true for ascending collation
          return true
        }
      } else if (t1_comparable.compareTo(t2_comparable) > 0) {
        // This is the case where tuple1 > tuple 2
        if (coll.direction.isDescending) {
          // Return true for descending collation
          return true
        } else {
          // Return false for ascending collation
          return false
        }
      }
    } // End of for loop

    // The program will reach this point only if all the entries are equal
    // There is no need to sort in this case, hence, return false
    false
  }
  override def execute(): IndexedSeq[Column] = {
    val cols = input.execute()
    val tuples = cols.transpose.map(row => (0 until(row.size - 1)).map(i => row(i)))
    val sorted = tuples.sortWith(comparator)
    var end_count = 0
    var start_count = 0

    (offset.isEmpty, fetch.isEmpty) match {
      case (false,false) => {
        start_count = offset.get
        end_count = offset.get + fetch.get
      }
      case (true,false) => end_count = fetch.get
      case _ => end_count = sorted.length
    }
    if (end_count > sorted.size){
      end_count = sorted.size
    }
    val results = (start_count until(end_count)) map(i => sorted(i))
    results.transpose :+ (0 until results.transpose.head.size).map(_ => true)
  }
}
