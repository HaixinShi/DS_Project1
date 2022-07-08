package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilLateTuple, Tuple}
import org.apache.calcite.rex.RexNode

import scala.collection.mutable.Map
import scala.util.control.Breaks.{break, breakable}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class LateJoin(
               left: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
               right: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
               condition: RexNode
             ) extends skeleton.Join[
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](left, right, condition)
  with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  /**
    * @inheritdoc
    */
  var results = List[LateTuple]()
  var cnt = -1
  override def open(): Unit = {
    //println("hi! this is late join")
    left.open()
    right.open()
    cnt = -1
    val lkeys = getLeftKeys
    val rkeys = getRightKeys
    val l_tuples = left.toList
    val r_tuples = right.toList
    /*
    for(ltuple <- ltuples){
      for(rtuple <- rtuples){
        var index = 0
        breakable{
          while(index < lkeys.length){
            //compare keys of left tuple and right tuple
            val t1_key = ltuple.value(lkeys(index)).asInstanceOf[Comparable[Any]]
            val t2_key = rtuple.value(rkeys(index)).asInstanceOf[Comparable[Any]]
            if(t1_key.compareTo(t2_key) != 0){
              break
            }
            index = index + 1
          }
        }
        if(index == lkeys.length){

          results = results :+ LateTuple(ltuple.vid, ltuple.value ++ rtuple.value)
        }
      }
    }
    results = results.distinct*/
    def hashKeys(tuple: LateTuple, keys: IndexedSeq[Int]) = {for(k <- keys)yield tuple.value(k)}.hashCode()
    results = List[LateTuple]()
    var m = Map[Int, List[LateTuple]]()
    for(l_tuple <- l_tuples){
      var key = hashKeys(l_tuple,lkeys)
      if(m.contains(key)){
        m(key) :+= l_tuple
      }
      else{
        m += (key -> List[LateTuple](l_tuple))
      }
    }

    for(r_tuple <- r_tuples) {
      var key = hashKeys(r_tuple,rkeys)
      if (m.contains(key)) {
        //stitch it
        val list = m(key)
        for (item <- list) {
          results = results :+ LateTuple(item.vid, item.value ++ r_tuple.value)
        }
      }
    }
    //results = results.distinct
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] = {
    if(cnt == results.length - 1){
      NilLateTuple
    }
    else{
      cnt = cnt + 1
      Option(results(cnt))
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    right.close()
  }
}
