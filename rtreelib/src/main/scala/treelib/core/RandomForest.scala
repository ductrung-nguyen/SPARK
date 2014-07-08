package treelib.core


import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.immutable.HashMap
import java.io._
import scala.actors.remote.JavaSerializer
import java.io.DataOutputStream
import java.io.FileOutputStream
import java.io.DataInputStream
import java.io.FileInputStream
import treelib.cart.CARTTreeModel
import treelib.cart.ClassificationTree


class Forest (var numberOfTrees : Int) extends Serializable {
    /**
	 * Forest of trees
	 */
	private var trees = new Array[TreeModel](numberOfTrees)
	
	/**
	 * Set index-th tree
	 */
	def setTree(index : Int, treeModel: TreeModel) = {
	    if (index >=0 && index < numberOfTrees)
	    	trees.update(index, treeModel)
	}
	
	def predictOne(input: String, delimeter: String = ",") : Any = {
	    var prediction_vote = new HashMap[String, Int]()

	    var maxVote = Int.MinValue
        var finalPrediction: Any = null
        var values = input.split(delimeter)

        for (i <- 0 until numberOfTrees) {
            val prediction = trees(i).predict(values)
            var numVote = prediction_vote.getOrElse(prediction, 0) + 1

            prediction_vote = prediction_vote.updated(prediction, numVote)

            if (numVote > maxVote) {
                maxVote = numVote
                finalPrediction = prediction
                
            }
	    	
	    }	
	    
	    finalPrediction
	}
	
	/**
     * Predict value of the target feature base on the values of input features
     * 
     * @param testingData	the RDD of testing data
     * @return a RDD contain predicted values
     */
    def predict(testingData: RDD[String], 
            delimiter : String = ",", 
            ignoreBranchIDs : Set[BigInt] = Set[BigInt]()
            ) : RDD[String] = {
        
        testingData.map(line => this.predictOne(line).toString)
        //testingData.map(line => this.predict(line.split(delimiter))) 
    }
    
    override def toString : String = {
        "number of trees:%d\n%s".format(numberOfTrees, trees.mkString("\n"))
    }
	
	/***********************************************/
    /*    REGION WRITING AND LOADING MODEL    */
    /***********************************************/
    /**
     * Write the current tree model to file
     * 
     * @param path where we want to write to
     */
    def writeToFile(path: String) = {
        val ois = new ObjectOutputStream(new FileOutputStream(path))
        ois.writeObject(trees)
        ois.close()
    }

    /**
     * Load tree model from file
     *
     * @param path the location of file which contains tree model
     */
    def loadModelFromFile(path: String) = {
        val js = new JavaSerializer(null, null)

        val ois = new ObjectInputStream(new FileInputStream(path)) {
            override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
                try { Class.forName(desc.getName, false, getClass.getClassLoader) }
                catch { case ex: ClassNotFoundException => super.resolveClass(desc) }
            }
        }

        var rt = ois.readObject().asInstanceOf[Array[TreeModel]]
        //treeModel = rt
        //this.featureSet = treeModel.featureSet
        //this.usefulFeatureSet = treeModel.usefulFeatureSet
        trees = rt
        this.numberOfTrees = trees.length
        
        ois.close()
    }
}



class RandomForest {
    /**
     * The number of tree
     */
	private var numberOfTrees : Int = 100
	
	def setNumberOfTree(nTrees : Int) = {
	    numberOfTrees = nTrees;
	    forest = new Forest(numberOfTrees)
	}
	
	/**
	 * The number of random features, which will be use in each feature split
	 */
	var numberOfRandomFeatures : Int = 0
	
	private var minSplit : Int = 0
	
	/**
	 * Forest of trees
	 */
	var forest : Forest = new Forest(numberOfTrees)
	
	private var featureNames : Array[String] = null
	
	private var trainingData : RDD[String] = _

	/**
	 * Set training data, which will be used to build the forest
	 * @param	trainingData	the training data (without header)
	 */
	def setData(trainingData: RDD[String]) {
	    this.trainingData = trainingData
	}
	
	/**
	 * Because we didn't support included header in csv file, we use this function to set the features' name
	 * @param	fNames	the names of features
	 */
	def setFeatureName(fNames : Array[String]) = {
	    this.featureNames = fNames
	}
	
	
	def setMinSplit(m : Int) = {
		this.minSplit = m
	}
	
	/**
	 * Build the forest
     * @param yFeature 	name of target feature, the feature which we want to predict.
     * 					Default value is the name of the last feature
     * @param xFeatures set of names of features which will be used to predict the target feature
     * 					Default value is all features names, except target feature
	 */
	def buildForest[T <: TreeBuilder : ClassManifest](yFeature: String = "",
        xFeatures: Set[Any] = Set[Any]()) : Forest = {
	    for (i <- 0 until numberOfTrees) {
	        var tree : T = (implicitly[ClassManifest[T]]).erasure.newInstance.asInstanceOf[T]
	        tree.useCache = false
	        val samplingData = trainingData.sample(true, 1.0, System.nanoTime().toInt)
	        val obb = trainingData.subtract(samplingData)
	        tree.setDataset(samplingData)
	        tree.useRandomSubsetFeature = true
	        //tree.setMinSplit(this.minSplit)
	        
	        if (this.featureNames != null)
	        	tree.setFeatureNames(this.featureNames)
	        forest.setTree(i, tree.buildTree(yFeature, xFeatures))
	        samplingData.unpersist(true)
	    }
	    
	    forest
	}
}