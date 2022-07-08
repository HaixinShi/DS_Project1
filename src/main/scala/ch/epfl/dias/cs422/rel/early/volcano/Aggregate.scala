
package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {
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
  private var results = List.empty[List[RelOperator.Elem]]
  private var cnt = -1
  //override def open() = {}

  override def open(): Unit = {
    cnt = -1
    //println("haxin_aggregate_open_start")
    //input.open()
    val tuples = input.toList
    //println(tuples)
    val group_map = tuples.groupBy(tuple => groupSet.asScala.toList.map(i => tuple(i)))
    //println("Haixin---- before aggregate")
    if(tuples.isEmpty){
      val empty_result = aggCalls.map(agg => aggEmptyValue((agg))).toList
      results = results :+ empty_result
    }
    else{
      //iterate the map
      for(group <- group_map){
        var result = group._1
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
    //println("Haixin---- after aggregate")
    //println("without distinct-1")
    //println(results.length)
    results = results.distinct//why we would have duplication here?
    //if(results.length<=5){
      //println(results)
    //}
    //println("with distinct-2")
    //println(results.length)
    //at this results contain a lot of tuples(one tuple for one group)
    //println("haxin_aggregate_open_finish")
  }

  /**
    * @inheritdoc
    */

  override def next(): Option[Tuple] = {
    //println("haxin_aggregate_next")
    if(cnt == results.size - 1){
      NilTuple
    }
    else{
      cnt = cnt + 1
      Option(results(cnt).toIndexedSeq)
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    //println("haxin_aggregate_close")
    input.close()
  }
}