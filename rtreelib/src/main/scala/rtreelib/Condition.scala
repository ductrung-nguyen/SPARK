package rtreelib

import rtreelib._

class Condition(val splitPoint : SplitPoint, val positive: Boolean = true) extends Serializable{
    def check(value : Any) ={
        splitPoint.point match {
            case s: Set[String] => if (positive) s.contains(value.asInstanceOf[String]) else !s.contains(value.asInstanceOf[String])
            case d: Double => if (positive) (value.asInstanceOf[Double] < d) else !(value.asInstanceOf[Double] < d)
        }
    }
}