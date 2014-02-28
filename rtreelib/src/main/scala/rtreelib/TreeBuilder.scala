package rtreelib

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.immutable.Queue
import java.util.{ Timer, TimerTask }
import java.util.Date
import scala.concurrent._
import scala.concurrent.duration.Duration
import rtreelib._

abstract class TreeBuilder(val featureSet: FeatureSet) extends Serializable {
	
	// queue of waiting jobs
	var expandingJobs: Queue[JobInfo] = Queue[JobInfo]();
	
	// queue of finished jobs
  	var finishedJobs: Queue[JobInfo] = Queue[JobInfo]();
  	
  	// queue of error jobs
  	var errorJobs : Queue[JobInfo] = Queue[JobInfo]();
  
  	// the number of currently running jobs
  	var numberOfRunningJobs = 0
  	
  	// tree model
  	var treeModel = new TreeModel()
  	
  	//var root : Node = new Empty("Nil")
  	
  	// Minimum records to do a splitting
    var minsplit = 10
  	
  	 // delimiter of fields in data set
    var delimiter = ','
    
    // coefficient of variation
    var threshold : Double = 0.1

    // Default index of Y feature
    var yIndex = featureSet.numberOfFeature - 1	// = number_of_feature - 1

    // Default indices/indexes of X feature
    // this variable can be infered from featureSet and yIndex
    // but because it will be used in functions processLine, and buidingTree
    // so we don't want to calculate it multiple time
    var xIndexes = featureSet.data.map(x => x.index).filter(x => (x != yIndex)).toSet[Int]

  	
  
  	protected val ERROR_SPLITPOINT_VALUE = ",,,@,,," 
  	protected var MAXIMUM_PARALLEL_JOBS = 9999
  	
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
    
  	
  	def setMaximumParallelJobs(value : Int) = { MAXIMUM_PARALLEL_JOBS = value }

  

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
         Utility.parseDouble(line(yIndex)) match {
            case Some(yValue) => { // check type of Y : if isn't continuous type, return nothing
              try{
                line.map(f => { // this map is not parallel, it is executed by each worker on their part of the input RDD
                    i = (i + 1) % length
                    if (xIndexes.contains(i)) {
                        featureSet.data(i) match {
                            case nFeature : NumericalFeature => { // If this is a numerical feature => parse value from string to double
                                val v = Utility.parseDouble(f);
                                v match {
                                    case Some(d) => new FeatureValueAggregate(i, d, yValue, 1)
                                    case None =>  throw new Exception("Value of feature " + i + " is not double. Require DOUBLE")//new FeatureValueAggregate(-9, f, 0, 0)
                                }
                            }
                            // if this is a categorical feature => return a FeatureAggregateInfo
                            case cFeature : CategoricalFeature => new FeatureValueAggregate(i, f, yValue, 1)
                        } // end match fType(i)
                    } // end if
                    else new FeatureValueAggregate(-9, f, 0, -1)	// with frequency = -1 and value 0, we will remove unused features
                }) // end map
              }
              catch {
                case e : Exception => { println("Record has some invalid values");  Array[FeatureValueAggregate]() }
              }
            } // end case Some(yvalue)
            case None => { println("Y value is invalid:(%s)".format(line(yIndex))); Array[FeatureValueAggregate]() }
        } // end match Y value
    }
    
    /**
   * Building tree bases on:
   * @yFeature: predicted feature
   * @xFeature: input features
   * @return: root of tree
   */
  def buildTree(trainingData: RDD[String],
    yFeature: String = featureSet.data(yIndex).Name,
    xFeatures: Set[String] = Set[String]()): TreeModel;
  	
}
