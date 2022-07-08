package ch.epfl.dias.cs422.rel.early.columnatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator._
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.columnatatime.Operator {
  /**
    * Hint 1: See superclass documentation for semantics of groupSet and aggCalls
    * Hint 2: You do not need to implement each aggregate function yourself.
    * You can use reduce method of AggregateCall
    * Hint 3: In case you prefer a functional solution, you can use
    * groupMapReduce
    */

  /**
   * @inheritdoc
   */
  override def execute(): IndexedSeq[HomogeneousColumn] = {
    //println("haixin-1")
    val cols = input.execute()
    val tuples = cols.transpose filter { tuple:Tuple => tuple(cols.size - 1) match { case b: Boolean => b } }
    //val tuples = cols.transpose
    //println("haixin-2")
    val group_map = tuples.groupBy(tuple => groupSet.asScala.toIndexedSeq.map(i => tuple(i)))
    var results = IndexedSeq.empty[IndexedSeq[RelOperator.Elem]]
    if(tuples.isEmpty){
      val empty_result = aggCalls.map(agg => aggEmptyValue(agg)).toIndexedSeq
      results = results :+ empty_result
    }
    else{
      //iterate the map
      for(group <- group_map){
        var result = group._1//key
        for(aggCall <- aggCalls) {
          var cur = aggCall.getArgument(group._2.head)
          var tuple_cnt = 1;
          while ( tuple_cnt < group._2.size) {
            cur = aggReduce(cur, aggCall.getArgument(group._2(tuple_cnt)), aggCall)
            tuple_cnt = tuple_cnt + 1
          }
          //finish one aggregation
          result = result :+ cur
        }
        results = results :+ result
      }
    }
    results = results.distinct//why we would have duplication here?
    //println("haixin-3")
    //println(results.transpose :+ (0 until results.transpose.head.size).map(_ => true))

    //results.transpose.map(i => toHomogeneousColumn(i)) :+ (0 until results.transpose.head.size).map(_ => true)
    results.transpose.map(i => toHomogeneousColumn(i)) :+ toHomogeneousColumn((0 until results.transpose.head.size).toArray.map(_ => true))
  }
}
