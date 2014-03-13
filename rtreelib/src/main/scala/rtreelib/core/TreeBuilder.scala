package rtreelib.core

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.concurrent._

/**
 * Abstract class of tree builder
 * 
 * @param featureSet		all features in the training data
 * @param usefulFeatureSet	the features which we used to build the tree (included the target feature) 
 */
abstract class TreeBuilder(var featureSet: FeatureSet, var usefulFeatureSet : FeatureSet) extends Serializable {

    /**
     *  tree model
     */ 
    var treeModel = new TreeModel()
    
    /**
     *  minimum records to do a splitting, default value is 10
     */ 
    var minsplit = 10

    /**
     *  delimiter of fields in data set, default value is ","
     */ 
    var delimiter = ','

    /**
     *  coefficient of variation, default value is 0.1
     */ 
    var threshold: Double = 0.1
    
    /**
     * Max depth of the tree, default value if 30
     */
    protected var maxDepth : Int = 30

    /**
     *  index of target feature, 
     *  default value is the index of the last feature in dataset
     */ 
    protected var yIndex = usefulFeatureSet.numberOfFeature - 1

    /**
     *  the indices/indexes of X features, which will be used to predict the target feature
     *  this variable can be infered from featureSet and yIndex
     *  but because it will be used in functions processLine, and buidingTree
     *  so we don't want to calculate it multiple time
     *  The default value is the index of all features, except the last one
     */ 
    protected var xIndexes = usefulFeatureSet.data.map(x => x.index).filter(x => (x != yIndex)).toSet[Int]

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
     * Set the maximum of depth of tree
     */
    def setMaxDepth(value: Int) = { this.maxDepth = value }

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
        treeModel.usefulFeatureSet = usefulFeatureSet
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
    
    def createNewInstance(featureSet: FeatureSet, usefulFeatureSet : FeatureSet) : TreeBuilder

}
