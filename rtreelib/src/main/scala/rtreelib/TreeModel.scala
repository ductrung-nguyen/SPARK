package rtreelib

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

class TreeModel extends Serializable {
	var yIndex : Int = -1;
	var xIndexes : Set[Int] = Set[Int]()
	var featureSet : FeatureSet = new FeatureSet(Array[String]())
	
	var tree : Node = new Empty("Nil")
	
	def isEmpty() = tree.isEmpty
	
	/**
     * Predict Y base on input features
     * @record: an array, which its each element is a value of each input feature
     */
    def predict(record: Array[String]): String = {
        def predictIter(root: Node): String = {
            if (root.isEmpty) root.value.toString
            else root.condition match {
                case s: Set[String] => {
                    if (s.contains(record(root.feature.index))) predictIter(root.left)
                    else predictIter(root.right)
                }
                case d: Double => {
                    if (record(root.feature.index).toDouble < d) predictIter(root.left)
                    else predictIter(root.right)
                }
            }

        }
        if (tree.isEmpty) "Please build tree first"
        else predictIter(tree)
    }
	
	/**
     * Evaluate the accuracy of regression tree
     * @input: an input record (uses the same delimiter with trained data set)
     */
    def evaluate(input: RDD[String], delimiter : Char = ',') {
        if (!tree.isEmpty){
            val numTest = input.count
            val inputdata = input.map(x => x.split(delimiter))
            val diff = inputdata.map(x => (predict(x).toDouble, x(yIndex).toDouble)).map(x => (x._2 - x._1, (x._2 - x._1)*(x._2-x._1)))

            val sums = diff.reduce((x,y) => (x._1 + y._1, x._2 + y._2))

            val meanDiff = sums._1/numTest
            val meanDiffPower2 = sums._2/numTest
            val deviation = math.sqrt(meanDiffPower2 - meanDiff*meanDiff)
            val SE = deviation/numTest
            
            println("Mean of different:%f\nDeviation of different:%f\nSE of different:%f".format(meanDiff, deviation, SE) )
        }else {
            "Please build tree first"
        }
    }
    
    override def toString() = {
      (
    		  "FeatureSet:" + featureSet.toString + "\n"
    		  + "xIndexes:" + xIndexes + "\n"
    		  + "yIndexes" + yIndex + "\n"
    		  + "Tree:\n" + "\n"
    		  + tree
      )
    }
}