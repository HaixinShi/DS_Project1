package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilLateTuple, NilTuple, Tuple}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Drop]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class Drop protected(
                         input: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
                       ) extends skeleton.Drop[
  ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](input)
  with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private var currTuple: Option[LateTuple] = NilLateTuple
  private var input_list = List[LateTuple]()
  private var output_list = List[Tuple]()
  private var cnt = -1
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    /*
    input_list = input.toList
    //println("haixinshi drop-------enter------")
    //println(input_list)
    for(ltuple <- input_list){
      output_list = output_list :+ ltuple.value
    }
    output_list= output_list.distinct
    cnt = -1
    //println("haixinshi drop------leave-------")
    //println(input_list)*/
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    //println("haixinshi drop------next-------")
    val tuple = input.next()
    if(tuple == NilLateTuple){
      NilLateTuple
    }
    else{
      Option(tuple.get.value)
    }
    /*
    if(cnt == output_list.length - 1){
      NilTuple
    }
    else{
      cnt = cnt + 1
      Option(output_list(cnt))
    }*/
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}
