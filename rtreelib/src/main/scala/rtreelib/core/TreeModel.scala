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
	var featureSet : FeatureSet = new FeatureSet(List[Feature]())

	/**
	 * The features which were used to build the tree, they were re-indexed from featureSet
	 */
	var usefulFeatureSet : FeatureSet = new FeatureSet(List[Feature]())
	
	/**
	 *  the root node of the tree
	 */ 
	var tree : Node = new LeafNode("root.empty")
	
	var minsplit : Int = 10
	var threshold : Double = 0.01
	var maximumComplexity : Double = 0.01
	var yFeature: String = ""
	var xFeatures : Set[Any] = Set[Any]()
	
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
     * @param record			an array, which its each element is a value of each input feature
     * @param ignoreBranchSet	a set of branch ID, which won't be used to predict (only use it's root node)
     * @return a predicted value
     * @throw Exception if the tree is empty
     */
    def predict(record: Array[String], ignoreBranchSet: Set[BigInt] = Set[BigInt]()): String = {

        def predictIter(currentNode: Node, currentID: BigInt): String = {
            if (currentNode.isEmpty || ignoreBranchSet.contains(currentID)) {
                currentNode.value.toString
            } else {
                currentNode.feature.Type match {
                    case FeatureType.Categorical => {
                        if (currentNode.splitpoint.point.asInstanceOf[Set[String]].contains(record(currentNode.feature.index)))
                            predictIter(currentNode.left, currentID << 1)
                        else predictIter(currentNode.right, (currentID << 1) + 1)
                    }
                    case FeatureType.Numerical => {
                        if (record(currentNode.feature.index).toDouble < currentNode.splitpoint.point.asInstanceOf[Double])
                            predictIter(currentNode.left, currentID << 1)
                        else predictIter(currentNode.right, (currentID << 1) + 1)
                    }
                }
            }
        }

        if (tree.isEmpty) throw new Exception("ERROR: The tree is empty.Please build tree first")
        else predictIter(tree , 1)
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
    		  + "* xIndexes:" + xIndexes.map(index => featureSet.getIndex(usefulFeatureSet.data(index).Name))  + "\n"
    		  + "* yIndex (target index):" + featureSet.getIndex(usefulFeatureSet.data(yIndex).Name) + "\n"
    		  + "* Is complete:" + isComplete + "\n"
    		  + "* Tree:\n"
    		  + tree
      )
    }
}