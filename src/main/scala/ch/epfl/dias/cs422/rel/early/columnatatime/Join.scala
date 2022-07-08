package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.Map
/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Join(
            left: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
            right: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
            condition: RexNode
          ) extends skeleton.Join[
  ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
](left, right, condition)
  with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {

  /**
    * @inheritdoc
    */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    // Get the left and right row sequences of homogeneous columns
    val lcols = left.execute()
    val rcols = right.execute()
    val lkeys = getLeftKeys
    val rkeys = getRightKeys

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

    (l_tuples.isEmpty, r_tuples.isEmpty) match {
      case (false, false)=> {
        val data = results.transpose.map(i => toHomogeneousColumn(i))
        val flags = toHomogeneousColumn((0 until data.head.size).toArray.map(_ => true))
        data :+ flags
      }
      case _=> IndexedSeq[HomogeneousColumn]()
    }
  }
}