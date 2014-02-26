package rtreelib

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import scala.util.Marshal
import scala.io.Source
import java.io._
import scala.actors.remote.JavaSerializer
import java.io.DataOutputStream
import java.io.FileOutputStream
import java.io.DataInputStream
import java.io.FileInputStream

abstract class BaseRegressionTree(metadata : Array[String]) extends Serializable {
	
     // delimiter of fields in data set
    // pm: this is very generic. You could instead assume your input data is always
    // a CSV file. If it is not, then you have a pre-proccessing job to make it so
    var delimiter = ','
    
    // set of feature in dataset
    // pm: this is very generic, and indeed it depends on the input data
    // however, in "production", you wouldn't do this, as you specialize for a particular kind of data
    // this variable is a part of instance of RegessionTree class, which will be dispatched to 
    // every worker also. So, should we broadcast something always transfered to the workers ?
    var featureSet = new FeatureSet(metadata)
    
    // coefficient of variation
    var threshold : Double = 0.1

    // Default index of Y feature
    println("Num F:" + featureSet.numberOfFeature)
    var yIndex = featureSet.numberOfFeature - 1	// = number_of_feature - 1
    println("yIndex:" + yIndex)
    // Default indices/indexes of X feature
    // this variable can be infered from featureSet and yIndex
    // but because it will be used in functions processLine, and buidingTree
    // so we don't want to calculate it multiple time
    var xIndexs = featureSet.data.map(x => x.index).filter(x => (x != yIndex)).toSet[Int]

    // Tree model
    protected var tree: Node = new Empty("Nil")

    // Minimum records to do a splitting
    var minsplit = 10

    // user partitioner or not
    //var usePartitioner = true
    //val partitioner = new HashPartitioner(contextBroadcast.value.defaultParallelism)

    def setDelimiter(c: Char) = { delimiter = c }

    /**
     * Set the minimum records of splitting
     * It's mean if a node have the number of records <= minsplit, it can't be splitted anymore
     * @xMinSplit: new minimum records for splitting
     */
    def setMinSplit(xMinSlit: Int) = { this.minsplit = xMinSlit }

    /**
     * Set threshold for stopping criterion. This threshold is coefficient of variation
     * A node will stop expand if Dev(Y)/E(Y) < threshold
     * In which:
     * Dev(Y) is standard deviation
     * E(Y) is medium of Y
     * @xThreshold: new threshold
     */
    def setThreshold(xThreshlod: Double) = { threshold = xThreshlod }
    
        /**
     * Process a line of data set
     * For each value of each feature, encapsulate it into a FeatureAgregateInfo(fetureIndex, xValue, yValue, frequency)
     * @line: array of value of each feature in a "record"
     * @numbeFeatures: the TOTAL number of feature in data set (include features which may be not processed)
     * @fTypes: type of each feature in each line (in ordered)
     * @return: an array of FeatureAggregateInfo, each element is a value of each feature on this line
     */
    protected def processLine(line: Array[String], numberFeatures: Int, featureSet : FeatureSet): Array[FeatureValueAggregate] = {
        val length = numberFeatures
        var i = -1;
        parseDouble(line(yIndex)) match {
            case Some(yValue) => { // check type of Y : if isn't continuous type, return nothing
                line.map(f => { // this map is not parallel, it is executed by each worker on their part of the input RDD
                    i = (i + 1) % length
                    if (xIndexs.contains(i)) {
                        featureSet.data(i) match {
                            case nFeature : NumericalFeature => { // If this is a numerical feature => parse value from string to double
                                val v = parseDouble(f);
                                v match {
                                    case Some(d) => new FeatureValueAggregate(i, d, yValue, 1)
                                    case None => new FeatureValueAggregate(-1, f, 0, 0)
                                }
                            }
                            // if this is a categorical feature => return a FeatureAggregateInfo
                            case cFeature : CategoricalFeature => new FeatureValueAggregate(i, f, yValue, 1)
                        } // end match fType(i)
                    } // end if
                    else new FeatureValueAggregate(-1, f, 0, 0)
                }) // end map
            } // end case Some(yvalue)
            case None => { println("Y value is invalid:(%s)".format(line(yIndex))); Array[FeatureValueAggregate]() }
        } // end match Y value
    }
    
     /**
     * Check a sub data set has meet stop criterion or not
     * @data: data set
     * @return: true/false and average of Y
     */
    def checkStopCriterion(data: RDD[FeatureValueAggregate]): (Boolean, Double) = { //PM: since it operates on RDD it is parallel
        val yFeature = data.filter(x => x.index == yIndex)

        //yFeature.collect.foreach(println)
        
        val numTotalRecs = yFeature.reduce(_ + _).frequency
        
        
        val yValues = yFeature.groupBy(_.yValue)

        val yAggregate = yFeature.map(x => (x.yValue, x.yValue * x.yValue))

        val ySumValue = yAggregate.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
        
        
        
        val EY = ySumValue._1 / numTotalRecs
        val EY2 = ySumValue._2 / numTotalRecs

        val standardDeviation = math.sqrt(EY2 - EY * EY)

        (		(	// the first component of tuple
                (numTotalRecs <= this.minsplit) // or the number of records is less than minimum
                || (((standardDeviation < this.threshold) && (EY == 0)) || (standardDeviation / EY < threshold)) // or standard devariance of values of Y feature is small enough
                ),
                EY	// the second component of tuple
        )

    }
    
     /**
     * Building tree bases on:
     * @yFeature: predicted feature
     * @xFeature: input features
     * @return: root of tree
     */
    def buildTree(trainingData: RDD[String], yFeature: String = featureSet.data(yIndex).Name, xFeatures: Set[String] = Set[String]()): Node;

    
        /**
     * Parse a string to double
     */
    private def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ => None }
    
    
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
   def writeTreeToFile(path: String) = {
        
        val js = new JavaSerializer(null, null)
        val os = new DataOutputStream(new FileOutputStream(path))
        
        //js.writeObject(os, featureSet.data)
        //js.writeObject(os, tree)
        //js.writeObject(os, yIndex.value : Integer)
        js.writeObject(os, this)
        os.close
    }
     
}