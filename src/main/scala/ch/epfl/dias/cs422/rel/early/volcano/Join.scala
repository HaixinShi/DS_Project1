package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{LateTuple, NilTuple, Tuple}
import org.apache.calcite.rex.RexNode
import scala.collection.mutable.Map
import scala.util.control.Breaks.{break, breakable}



/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {
  /**
    * Hint: you need to use methods getLeftKeys and getRightKeys
    * to implement joins
    */

  /**
    * @inheritdoc
    */
    var results = List[Tuple]()
    var cnt = -1
  override def open(): Unit = {
    left.open()
    right.open()
    cnt = -1
    val lkeys = getLeftKeys
    val rkeys = getRightKeys
    val l_tuples = left.toList
    val r_tuples = right.toList

    /*
    for(ltuple <- l_tuples){
      for(rtuple <- r_tuples){
        var index = 0
        breakable{
          while(index < lkeys.length){
            //compare keys of left tuple and right tuple
            val t1_key = ltuple(lkeys(index)).asInstanceOf[Comparable[Any]]
            val t2_key = rtuple(rkeys(index)).asInstanceOf[Comparable[Any]]
            if(t1_key.compareTo(t2_key) != 0){
              break
            }
            index = index + 1
          }
        }
        if(index == lkeys.length){
          results = results :+(ltuple ++ rtuple)
        }
      }
    }
    results = results.distinct

    //def hashKeys(tuple: Tuple, keys: IndexedSeq[Int]) = {for(k <- keys)yield tuple(k)}.hashCode()

    println("normal join results-1:")
    println(results)
    */
    //
    /*
    results = List[Tuple]()
    var m = Map[String, List[Tuple]]()
    for(l_tuple <- l_tuples){
      var key = ""
      for(lkey <- lkeys){
        key = key + l_tuple(lkey).hashCode().toString
      }
      if(m.contains(key)){
        m(key) :+= l_tuple
      }
      else{
        m += (key -> List[Tuple](l_tuple))
      }
    }

    for(r_tuple <- r_tuples) {

      var key = ""
      for (rkey <- rkeys) {
        key = key + r_tuple(rkey).hashCode().toString
      }
      if (m.contains(key)) {
        //stitch it
        val list = m(key)
        for (item <- list) {
          results = results :+ (item ++ r_tuple)
        }
      }
    }
    results = results.distinct
    //println("normal join results-2:")
    //println(results)*/
    def hashKeys(tuple: Tuple, keys: IndexedSeq[Int]) = {for(k <- keys)yield tuple(k)}.hashCode()
    results = List[Tuple]()
    var m = Map[Int, List[Tuple]]()
    for(l_tuple <- l_tuples){
      var key = hashKeys(l_tuple,lkeys)
      if(m.contains(key)){
        m(key) :+= l_tuple
      }
      else{
        m += (key -> List[Tuple](l_tuple))
      }
    }

    for(r_tuple <- r_tuples) {
      var key = hashKeys(r_tuple,rkeys)
      if (m.contains(key)) {
        //stitch it
        val list = m(key)
        for (item <- list) {
          results = results :+ (item ++ r_tuple)
        }
      }
    }
    //results = results.distinct
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if(cnt == results.length - 1){
      NilTuple
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
