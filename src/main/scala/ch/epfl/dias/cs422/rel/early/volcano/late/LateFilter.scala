package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilLateTuple, NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class LateFilter protected (
                            input: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
                            condition: RexNode
                          ) extends skeleton.Filter[
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](input, condition)
  with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }
  //private var cnt = -1
  //private var input_list = List[LateTuple]()
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    //println("late filter")
    //println(input.getRowType())
    //cnt = -1
    //input_list = input_list.toList
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] = {
    /*
    while(cnt < input_list.length - 1){
      cnt = cnt + 1
      /*
      println("haixinshi filter-------------")
      println(input_list(cnt))
      */
      if(predicate(input_list(cnt).value)){

        return Option(input_list(cnt))
      }
    }*/
    var tuple = input.next()
    while(tuple != NilLateTuple){
      if(predicate(tuple.get.value)){
        //println("haixin filter-get")
        //println(tuple)
        return tuple
      }
      //println("haixin filter-discard")
      //println(tuple)
      tuple = input.next()
    }
    NilLateTuple
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}
