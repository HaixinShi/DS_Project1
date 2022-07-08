package ch.epfl.dias.cs422.rel.early.volcano.late

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{
  LateTuple,
  NilLateTuple,
  NilTuple,
  Tuple
}

/**
  * @inheritdoc
  *
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Stitch]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator]]
  */
class Stitch protected (
                         left: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator,
                         right: ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
                       ) extends skeleton.Stitch[
  ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator
](left, right)
  with ch.epfl.dias.cs422.helpers.rel.late.volcano.naive.Operator {

  private var left_map = Map[Comparable[Any], Tuple]()

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    //left.open()
    left_map = left.toIndexedSeq.map { lt: LateTuple =>(lt.vid.asInstanceOf[Comparable[Any]], lt.value)}.toMap
    right.open()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[LateTuple] = {
    right.next() match {
      case NilLateTuple => NilLateTuple
      case Some(r_tuple) => {
          left_map.get(r_tuple.vid.asInstanceOf[Comparable[Any]]) match {
          case None => this.next()
          case Some(l_tuple) =>
            Some(LateTuple(r_tuple.vid, l_tuple ++ r_tuple.value))
        }
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    //left.close()
    right.close()
  }
}
