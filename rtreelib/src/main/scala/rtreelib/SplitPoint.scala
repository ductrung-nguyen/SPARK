package rtreelib

// index : index of the feature
// point: the split point of this feature (it can be a Set, or a Double)
// weight: the weight we get if we apply this splitting
class SplitPoint(val index: Int, var point: Any, val weight: Double) extends Serializable {
    override def toString = index.toString + "," + point.toString + "," + weight.toString // for debugging
}