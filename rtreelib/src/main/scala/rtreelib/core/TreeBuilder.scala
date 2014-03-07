package rtreelib.core

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.concurrent._

/**
 * Abstract class of tree builder
 */
abstract class TreeBuilder(var featureSet: FeatureSet) extends Serializable {

    /**
     *  tree model
     */ 
    var treeModel = new TreeModel()
    
    /**
     *  minimum records to do a splitting
     */ 
    var minsplit = 10

    /**
     *  delimiter of fields in data set
     */ 
    var delimiter = ','

    /**
     *  coefficient of variation
     */ 
    var threshold: Double = 0.1

    /**
     *  index of target feature, 
     *  default value is the index of the last feature in dataset
     */ 
    var yIndex = featureSet.numberOfFeature - 1

    /**
     *  the indices/indexes of X features, which will be used to predict the target feature
     *  this variable can be infered from featureSet and yIndex
     *  but because it will be used in functions processLine, and buidingTree
     *  so we don't want to calculate it multiple time
     *  The default value is the index of all features, except the last one
     */ 
    var xIndexes = featureSet.data.map(x => x.index).filter(x => (x != yIndex)).toSet[Int]

    /**
     * A value , which is used to marked a split point is invalid
     */
    protected val ERROR_SPLITPOINT_VALUE = ",,,@,,,"
        
    protected var MAXIMUM_PARALLEL_JOBS = 9999

    def setDelimiter(c: Char) = { delimiter = c }

    /**
     * Set the minimum records of splitting
     * It's mean if a node have the number of records <= minsplit, it can't be splitted anymore
     * 
     * @param xMinSplit	new minimum records for splitting
     */
    def setMinSplit(xMinSlit: Int) = { this.minsplit = xMinSlit }

    /**
     * Set threshold for stopping criterion. This threshold is coefficient of variation
     * A node will stop expand if Dev(Y)/E(Y) < threshold
     * In which:
     * Dev(Y) is standard deviation
     * E(Y) is medium of Y
     * 
     * @param xThreshold	new threshold
     */
    def setThreshold(xThreshlod: Double) = { threshold = xThreshlod }

    /**
     * Set the maximum of parallel jobs
     * 
     * @param value	the maximum of parallel jobs
     */
    def setMaximumParallelJobs(value: Int) = { MAXIMUM_PARALLEL_JOBS = value }

    /**
     * Process a line of data set
     * For each value of each feature, encapsulate it into a FeatureAgregateInfo(fetureIndex, xValue, yValue, frequency)
     * 
     * @param line			array of value of each feature in a "record"
     * @param numbeFeatures	the TOTAL number of feature in data set (include features which may be not processed)
     * @param fTypes		type of each feature in each line (in ordered)
     * @return an array of FeatureAggregateInfo, each element is a value of each feature on this line
     */
    protected def processLine(line: Array[String], numberFeatures: Int, featureSet: FeatureSet): Array[FeatureValueAggregate] = {
        val length = numberFeatures
        var i = -1;
        Utility.parseDouble(line(yIndex)) match {
            case Some(yValue) => { // check type of Y : if isn't continuous type, return nothing
                try {
                    line.map(f => { // this map is not parallel, it is executed by each worker on their part of the input RDD
                        i = (i + 1) % length
                        if (xIndexes.contains(i)) {
                            featureSet.data(i) match {
                                case nFeature: NumericalFeature => { // If this is a numerical feature => parse value from string to double
                                    val v = Utility.parseDouble(f);
                                    v match {
                                        case Some(d) => new FeatureValueAggregate(i, d, yValue, yValue*yValue, 1)
                                        case None => throw new Exception("Value of feature " + i + " is not double. Require DOUBLE") //new FeatureValueAggregate(-9, f, 0, 0)
                                    }
                                }
                                // if this is a categorical feature => return a FeatureAggregateInfo
                                case cFeature: CategoricalFeature => new FeatureValueAggregate(i, f, yValue, yValue*yValue, 1)
                            } // end match fType(i)
                        } // end if
                        else new FeatureValueAggregate(-9, f, 0, 0, -1) // with frequency = -1 and value 0, we will remove unused features
                    }) // end map
                } catch {
                    case e: Exception => { println("Record has some invalid values"); Array[FeatureValueAggregate]() }
                }
            } // end case Some(yvalue)
            case None => { println("Y value is invalid:(%s)".format(line(yIndex))); Array[FeatureValueAggregate]() }
        } // end match Y value
    }

    /**
     * Building tree, bases on:
     *
     * @param trainingData	the data which is used to build tree
     * @parm yFeature 		predicted feature
     * @param xFeature		input features
     *
     * @return: <code>TreeModel</code> : root of the tree
     */
    def buildTree(trainingData: RDD[String],
        xIndexes: Set[Int],
        yIndex: Int): TreeModel = {
        
        initBuildingTree(trainingData, xIndexes, yIndex)
        startBuildTree(trainingData)
        treeModel
    }
    
    /**
     * Init state before start algorithm
     * This function will try to map features' name to its index
     * 
     * @param trainingData	the data which is used to build tree
     * @parm yFeature 		predicted feature
     * @param xFeature		input features
     * 
     * @throw Exception if can not map the name of target feature into index (invalid name)
     */
    protected def initBuildingTree(trainingData: RDD[String],
        xIndexes : Set[Int],
        yIndex : Int) = {
        
        this.xIndexes = xIndexes
        this.yIndex = yIndex
        
    	/* Save necessary information for recovery in failure cases */
        treeModel.tree = new Empty("None")
        treeModel.featureSet = featureSet
        treeModel.xIndexes = xIndexes
        treeModel.yIndex = yIndex
    }
    
    /**
     * Start the main part of algorithm
     * 
     * @param trainingData	the input data
     * @return TreeModel the root node of tree
     * @see TreeModel
     */
    protected def startBuildTree(trainingData: RDD[String])
    
    /**
     * Recover, repair and continue build tree from the last state
     */
    def continueFromIncompleteModel(trainingData: RDD[String])
    
    def createNewInstance(featureSet: FeatureSet) : TreeBuilder

}
