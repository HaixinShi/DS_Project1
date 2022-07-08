package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.Map

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[Column] = {
    val lkeys = getLeftKeys
    val rkeys = getRightKeys
    val lcols = left.execute()
    val rcols = right.execute()
    //val l_tuples = lcols.transpose.map(row => (0 until(row.size - 1)).map(i => row(i)))
    //val r_tuples = rcols.transpose.map(row => (0 until(row.size - 1)).map(i => row(i)))
    val l_tuples = (lcols.transpose filter { tuple:Tuple => tuple(lcols.size - 1) match { case b: Boolean => b } }).map(row => (0 until(row.size - 1)).map(i => row(i)))
    val r_tuples = (rcols.transpose filter { tuple:Tuple => tuple(rcols.size - 1) match { case b: Boolean => b } }).map(row => (0 until(row.size - 1)).map(i => row(i)))
    var results = IndexedSeq.empty[IndexedSeq[RelOperator.Elem]]
    def hashKeys(tuple: Tuple, keys: IndexedSeq[Int]) = {for(k <- keys)yield tuple(k)}.hashCode()
    val m = Map[Int, IndexedSeq[Tuple]]()
    for(l_tuple <- l_tuples){
      val key = hashKeys(l_tuple,lkeys)
      if(m.contains(key)){
        m(key) :+= l_tuple
      }
      else{
        m += (key -> IndexedSeq[Tuple](l_tuple))
      }
    }

    for(r_tuple <- r_tuples) {
      val key = hashKeys(r_tuple,rkeys)
      if (m.contains(key)) {
        //stitch it
        val seq = m(key)
        for (item <- seq) {
          results = results :+ (item ++ r_tuple)
        }
      }
    }
    results = results.distinct
    /*
    println("l_tuples")
    println(l_tuples)
    println("r_tuples")
    println(r_tuples)*/
    (l_tuples.isEmpty, r_tuples.isEmpty) match {
      case (false, false)=> results.transpose :+ (0 until results.transpose.head.size).map(_ => true)
      case _=>  IndexedSeq[Column]()
    }
  }
}
