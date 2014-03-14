package rtreelib.core

import rtreelib.core.Feature._

/**
 * An interface of node in tree
 */
trait Node extends Serializable {
    
    /**
     * The split point which this node contains
     */
    def splitpoint: SplitPoint
    
    /**
     * The associated feature of this node
     */
    var feature: Feature
    
    /**
     * The predicted value of this node
     */
    var value : Any
    
    /**
     * The metric to consider the error of prediction of this node
     */
    var errorMetric : Double
    
    /**
     * The left child
     */
    def left: Node
    
    /**
     * The right child
     */
    def right: Node
    
    /**
     * Is this node empty?
     */
    def isEmpty: Boolean
    
    /**
     * Set the left child
     * @param node the desired left node 
     */
    def setLeft(node : Node): Unit
    
    /**
     * Set the right child
     * @param node the desired right node
     */
    def setRight(node: Node) : Unit
    
    /**
     * A function support to convert an object of this class to string
     * @param level The level of this node. Root node has level 1
     */
    def toStringWithLevel(level: Int): String
    
    override def toString: String = "\n" + toStringWithLevel(1)
}

/**
 * A leaf node
 */
class Empty(xValue: String = "Empty") extends Node {
    /**
     * Constructor of node. The default value of this node is 'Empty'
     */
    def this() = this("Empty")
    
    /**
     * Is this node empty ?
     */
    def isEmpty = true
    
    /**
     * Value of this leaf node
     */
    var value : Any = xValue
    
    var errorMetric : Double = 0
    
    /**
     * The split point of this node
     */
    def splitpoint: Nothing = throw new NoSuchElementException("empty.splitpoint")
    
    /**
     * Get the left child of this node
     */
    def left: Nothing = throw new NoSuchElementException("empty.left")
    
    /**
     * Get the right child of this node
     */
    def right: Nothing = throw new NoSuchElementException("empty.right")
    
    /**
     * Set the left child
     * @param node The desired left child
     */
    def setLeft(node : Node)= {}
    
    /**
     * Set the right child
     * @param node The desired right child
     */
    def setRight(node: Node)= {}
    
    /**
     * The feature which is associated to this node
     */
    var feature: Feature = _ //FeatureInfo("Empty", "d", 0)
    
    def toStringWithLevel(level: Int) = xValue
}

/**
 * An internal node (the node contains children)
 * 
 * @param xFeature		the associated feature
 * @param xSplitpoint	the split point of feature which associated to this node
 * @param xLeft			the left child
 * @param xRight		the right child
 */
class NonEmpty(xFeature: Feature, xSplitpoint: SplitPoint, var xLeft: Node, var xRight: Node) extends Node{
    
    /**
     * Value of this leaf node. 
     * Because this is non-leaf node, so the value is the name of feature which it's associated
     */
    var value : Any = 0.0
    
    var errorMetric : Double = 0
    
    /**
     * Is this node empty ?
     */
    def isEmpty = false
    
    /**
     * The split point of feature which associated to this node
     */
    def splitpoint = xSplitpoint
    
    /**
     * Get the left child
     */
    def left = xLeft
    
    /**
     * Get the right child
     */
    def right = xRight
    
    /**
     * Set the left child
     * @param node The desired left node
     */
    def setLeft(node: Node) = {xLeft = node;}
    
    /**
     * Set the right child
     * @param node The desired right node
     */
    def setRight(node: Node) = {xRight = node;}
    
    /**
     * The feature which is associated to this node
     */
    var feature: Feature = xFeature
    
    /**
     * Get the conditions to go to the left and right child
     */
    /*
    val (conditionLeft, conditionRight) = feature.Type match {
        case FeatureType.Categorical => (this.splitpoint.point , "Not in %s".format(this.splitpoint.point.toString))
        case FeatureType.Numerical => ("%s < %f".format(feature.Name, splitpoint.point), "%s >= %f".format(feature.Name, splitpoint.point))
    }*/

    def toStringWithLevel(level: Int) =
        "%s(%s)\n%s-(yes)%s%s\n%s-(no)-%s%s".format(
            feature.Name,
            (feature.Type match {
                case FeatureType.Categorical => Utility.setToString(splitpoint.point.asInstanceOf[Set[String]])
                case FeatureType.Numerical => " < %f".format(splitpoint.point.asInstanceOf[Double])
            }) + " || (" + value + "," + errorMetric + ")",
            ("".padTo(level, "|")).mkString("    "),
            ("".padTo(level, "-")).mkString(""),
            left.toStringWithLevel(level + 1),
            ("".padTo(level, "|")).mkString("    "),
            ("".padTo(level, "-")).mkString(""),
            right.toStringWithLevel(level + 1)
            )
}