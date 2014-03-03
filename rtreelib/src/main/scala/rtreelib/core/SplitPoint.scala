package rtreelib.core

/**
 * Representative of a split point
 * 
 * @param index		index of the feature
 * @param point		the split point of this feature (it can be a Set, or a Double)
 * @param weight	the weight we get if we apply this splitting (often be a value of  an impurity function)
 */
class SplitPoint(val index: Int, var point: Any, val weight: Double) extends Serializable {
    override def toString = index.toString + "," + point.toString + "," + weight.toString // for debugging
}