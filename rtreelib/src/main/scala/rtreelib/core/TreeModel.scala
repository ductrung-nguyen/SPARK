package rtreelib.core

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import java.io._
import scala.actors.remote.JavaSerializer
import java.io.DataOutputStream
import java.io.FileOutputStream

/***
 * This class is representative for a tree model. 
 * It contains all related information: index of target feature, indexes of the other features which were used to build tree
 */
class TreeModel extends Serializable {
    /**
     *  The index of target feature
     */ 
	var yIndex : Int = -1;
	
	/**
	 *  The indexes of the features which were used to predict target feature
	 */ 
	var xIndexes : Set[Int] = Set[Int]()
	
	/**
	 *  all features information
	 */ 
	var featureSet : FeatureSet = new FeatureSet(Array[String]())
	
	/**
	 *  the root node of the tree
	 */ 
	var tree : Node = new Empty("root.empty")
	
	/***
	 * Is the tree empty ?
	 */
	def isEmpty() = tree.isEmpty
	
	/**
	 *  Is the tree is build completely
	 */ 
	var isComplete = false
	
	/**
     * Predict Y base on input features
     * 
     * @param record	an array, which its each element is a value of each input feature
     * @return a predicted value
     * @throw Exception if the tree is empty
     */
    def predict(record: Array[String]): String = {
	    
        def predictIter(currentNode: Node): String = {
            if (currentNode.isEmpty) currentNode.value.toString
            else currentNode.splitpoint match {
                case s: Set[String] => {
                    if (s.contains(record(currentNode.feature.index))) predictIter(currentNode.left)
                    else predictIter(currentNode.right)
                }
                case d: Double => {
                    if (record(currentNode.feature.index).toDouble < d) predictIter(currentNode.left)
                    else predictIter(currentNode.right)
                }
            }

        }
        
        if (tree.isEmpty) throw new Exception("ERROR: The tree is empty.Please build tree first")
        else predictIter(tree)
    }
	
	/**
     * Evaluate the accuracy of regression tree
     * @param input	an input record (uses the same delimiter with trained data set)
     * @param delimiter the delimiter of training data
     */
    def evaluate(input: RDD[String], delimiter : Char = ',') =  {
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
            throw new Exception("Please build tree first")
        }
    }
    
    /**
     * Write the current tree model to file
     * 
     * @param path where we want to write to
     */
    def writeToFile(path: String) = {

        val js = new JavaSerializer(null, null)
        val os = new DataOutputStream(new FileOutputStream(path))

        js.writeObject(os, this)
        os.close
    }
    
    /**
     * convert instance of this class into string
     */
    override def toString() = {
      (
    		  "* FeatureSet:\n" + featureSet.toString + "\n"
    		  + "* xIndexes:" + xIndexes + "\n"
    		  + "* yIndex (target index):" + yIndex + "\n"
    		  + "* Is complete:" + isComplete + "\n"
    		  + "* Tree:\n"
    		  + tree
      )
    }
}