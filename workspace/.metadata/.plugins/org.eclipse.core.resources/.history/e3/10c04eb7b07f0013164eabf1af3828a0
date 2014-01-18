package machinelearning

trait Node {
    def condition: Any
    var feature: FeatureInfo
    def left: Node
    def right: Node
    def isEmpty: Boolean
    def toStringWithLevel(level: Int): String
    override def toString: String = "\n" + toStringWithLevel(1)
}

case class Empty(value: String = "Empty") extends Node {
    def this() = this("Empty")
    def isEmpty = true
    def condition: Nothing = throw new NoSuchElementException("empty.condition")
    def left: Nothing = throw new NoSuchElementException("empty.left")
    def right: Nothing = throw new NoSuchElementException("empty.right")
    var feature: FeatureInfo = FeatureInfo("Empty", "0", 0)
    def toStringWithLevel(level: Int) = value
}

case class NonEmpty(xFeature: FeatureInfo, xCondition: Any, xLeft: Node, xRight: Node) extends Node {
    def isEmpty = false
    def condition = xCondition match { case s: Set[String] => s; case d: Double => d }
    def left = xLeft
    def right = xRight
    var feature: FeatureInfo = xFeature
    val (conditionLeft, conditionRight) = xCondition match {
        case s: Set[String] => (s.toString, "Not in " + s.toString )
        case d : Double => ("%s < %f".format(xFeature.Name, d), "%s >= %f".format(xFeature.Name, d))
    }

    def toStringWithLevel(level: Int) =
        feature.Name + "\n" +
            ("".padTo(level, "|")).mkString("    ") + "-(" + conditionLeft + ")" + ("".padTo(level, "-")).mkString("") + left.toStringWithLevel(level + 1) + "\n" +
            ("".padTo(level, "|")).mkString("    ") + "-(" + conditionRight + ")" + ("".padTo(level, "-")).mkString("") + right.toStringWithLevel(level + 1)
}