package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Filter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }

  /**
    * @inheritdoc
    */
    val input_list = input.toList
    var cnt = -1;
  override def open(): Unit = {
    cnt = -1
    input.open()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    /*
    it would lead to class cast errors
    input.next() match {
      case NilTuple => NilTuple
      case tuple=>{
        predicate(tuple.get) match {
          case true => tuple
          case false => next()
        }
      }
    }*/
    /*
    var tuple = input.next()
    if(predicate(tuple.get)){
      Option(tuple)
    }
      tuple = input.next()*/

    while(cnt < input_list.length - 1){
      cnt = cnt + 1
      if(predicate(input_list(cnt))){

        return Option(input_list(cnt))
      }
    }
    NilTuple
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}