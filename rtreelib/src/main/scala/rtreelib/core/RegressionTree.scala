package rtreelib.core

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.concurrent._
import rtreelib._
import java.io._
import scala.actors.remote.JavaSerializer
import java.io.DataOutputStream
import java.io.FileOutputStream
import java.io.DataInputStream
import java.io.FileInputStream


/**
 * This class is used to build a regression tree
 */
class RegressionTree() extends Serializable {

    /**
     * Contains raw information about features, which will be used to construct a feature set
     */
    private var metadata: Array[String] = Array[String]()
    
    /**
     * Contains information of feature
     */
    var featureSet = new FeatureSet(metadata)

    /**
     * The main component to build the tree
     * Default is an instance of ThreadTreeBuilder, which will launch each job in
     * a thread to expand a node in tree
     */ 
    var treeBuilder: TreeBuilder = new ThreadTreeBuilder(featureSet);

    /**
     * Tree model
     */
    var treeModel: TreeModel = new TreeModel()

    /**
     * Default value of the index of the target feature. 
     * Here this value is the index of the last feature
     */
    var yIndexDefault = featureSet.numberOfFeature - 1 // = number_of_feature - 1

    /**
     * The data will be used to build tree
     */
    var trainingData: RDD[String] = null;

    private def getXIndexesAndYIndexByNames(xNames : Set[String], yName : String) : (Set[Int], Int) = {
        var yindex = featureSet.getIndex(yName)
        if (yindex < 0)
            throw new Exception("ERROR: Can not find attribute `" + yName + "` in (" + featureSet.data.map(f => f.Name).mkString(",") + ")")
        // index of features, which will be used to predict the target feature
        var xindexes =
            if (xNames.isEmpty) // if user didn't specify xFeature, we will process on all feature, include Y feature (to check stop criterion)
                featureSet.data.map(x => x.index).toSet[Int]
            else
                xNames.map(x => featureSet.getIndex(x)) + yindex
                
        (xindexes, yindex)
        
    }
    
    /**
     * This function is used to build the tree
     * 
     * @param yFeature 	name of target feature, the feature which we want to predict.
     * @param xFeatures set of names of features which will be used to predict the target feature
     * @return <code>TreeModel</code> the root of tree
     * @see TreeModel
     */
    private def buildTree(trainingData: RDD[String],
        xIndexes: Set[Int],
        yIndex: Int): TreeModel = {

        treeModel = treeBuilder.buildTree(trainingData, xIndexes, yIndex);
        treeModel
    }
    
    /**
     * This function is used to build the tree
     * 
     * @param yFeature 	name of target feature, the feature which we want to predict.
     * 					Default value is the name of the last feature
     * @param xFeatures set of names of features which will be used to predict the target feature
     * 					Default value is all features names, except target feature
     * @return <code>TreeModel</code> the root of tree
     * @see TreeModel
     */
    def buildTree(yFeature: String = "",
        xFeatures: Set[String] = Set[String]()) : TreeModel = {
        if (this.trainingData == null) {
            throw new Exception("ERROR: Dataset can not be null.Set dataset first")
        }
        if (yIndexDefault < 0) {
            throw new Exception("ERROR:Dataset is invalid or invalid feature names")
        }

        val (xIndexes, yIndex) = this.getXIndexesAndYIndexByNames(xFeatures, yFeature)
        this.buildTree(this.trainingData, xIndexes, yIndex)
    }

    /**
     * Predict value of the target feature base on the values of input features
     * 
     * @param record	an array, which its each element is a value of each input feature
     * @return predicted value or '???' if input record is invalid
     */
    def predict(record: Array[String]): String = {
        try{
            treeModel.predict(record)
        }
        catch{
            case e: Exception => "???"
        }
    }

    /**
     * Predict value of the target feature base on the values of input features
     * 
     * @param testingData	the RDD of testing data
     * @return a RDD contain predicted values
     */
    def predict(testingData: RDD[String], delimiter : String = ",") : RDD[String] = {
        /*
        val firstLine = testingData.first()
        try{
            this.predict(firstLine.split(this.treeBuilder.delimiter))
        }
        catch {
            case e:Exception => throw new Exception("Make sure testing data doesn't contain header")
            //case e : Exception => throw e
        }
        */
        testingData.map(line => this.predict(line.split(delimiter)))
    }


    /**
     * Write the current tree model to file
     * 
     * @param path where we want to write to
     */
    def writeModelToFile(path: String) = {

        val js = new JavaSerializer(null, null)
        val os = new DataOutputStream(new FileOutputStream(path))

        js.writeObject(os, treeModel)
        os.close
    }

    /**
     * Load tree model from file
     * 
     * @param path the location of file which contains tree model
     */
    def loadModelFromFile(path: String) = {
        val js = new JavaSerializer(null, null)
        val is = new DataInputStream(new FileInputStream(path))
        val rt = js.readObject(is).asInstanceOf[TreeModel]
        treeModel = rt
    }
    
    /**
     * Recover, repair and continue build tree from the last state
     */
    def continueFromIncompleteModel(trainingData: RDD[String], path_to_model : String) : TreeModel = {
        loadModelFromFile(path_to_model)
        treeBuilder.treeModel = treeModel
        treeBuilder.continueFromIncompleteModel(trainingData)
        treeModel
    }
    
    
    /*  REGION: SET DATASET AND METADATA */
    
    /**
     * Set the training dataset to used for building tree
     * 
     * @param trainingData	the training dataset
     * @throw Exception if the dataset contains less than 2 lines
     */
    def setDataset(trainingData: RDD[String]) {
        this.trainingData = trainingData;
        var headers = this.trainingData.take(2)
        
        // If we can not get 2 first line of dataset, it's invalid dataset
        if (headers.length < 2) {
            throw new Exception("ERROR:Invalid dataset")
        } else {
            this.metadata = Array[String]()
            
            // Get the first line of dataset
            var temp_header = headers(0).split(treeBuilder.delimiter)
            
            // if we can convert one of values in the first line to double, 
            // maybe the dataset doesn't contain header, because the header name usually be string type
            // In this case, create a fake header, named it "Column0", "Column1"...
            if (temp_header.exists(v => {
                Utility.parseDouble(v.trim()) match {
                    case Some(d) => true
                    case None => false
                }
            })) {
                var i = -1;
                this.metadata = this.metadata :+ (temp_header.map(v => { i = i +1; "Column" + i } ).mkString(","))
            } 
            else {	// if this dataset contain header, use it
                this.metadata = this.metadata :+ (temp_header.mkString(","))
            }
            
            // Get the second line of dataset and try to parse each of them to double
            // If we can parse, it's a numerical feature, otherwise, it's a categorical feature
            var temp_types = headers(1).split(treeBuilder.delimiter);
            
            this.metadata = this.metadata :+ (temp_types.map(v => {
                Utility.parseDouble(v.trim()) match {
                    case Some(d) => "c"	// continous feature == numerical feature
                    case None => "d"	// discrete feature == categorical feature
                }
            }).mkString(","))
            
            // update the dataset
            updateFeatureSet()
            
        }
    }
    
    /**
     * Set the metadata for this dataset. Metadata is 2 lines:
     * <ul>
     * <li> First line: Name of the features </li>
     * <li> Second line: Type of the features </li>
     * </ul>
     * Each value is separated together by a delimiter, which you set before (default value is comma ',')
     * 
     * @param mdata metadata
     */
    def setMetadata(mdata : Array[String]) = {
        this.metadata = mdata;
        updateFeatureSet()
    }
    
    /**
     * Set the type of features
     * 
     * @param types	a line contains type of features, 
     * 				separated by by a delimiter, which you set before (default value is comma ',')
     * 				Example: c,d,c,c,c
     * @throw Exception if the training set is never be set before
     */
    def setFeatureTypes(types : String) = {
        if (this.trainingData == null) 
            throw new Exception("Trainingset is null. Set dataset first")
        else {
	        this.metadata.update(1, types.split(treeBuilder.delimiter).map(v => v.trim()).mkString(","))
	        updateFeatureSet()
        }
    }

    /**
     * Set feature names
     * 
     * @param names a line contains names of features, 
     * 				separated by by a delimiter, which you set before (default value is comma ',')
     *     			Example: Temperature,Age,"Type"
     * @throw Exception if the training set is never be set before
     */
    def setFeatureNames(names: String) = {
        if (this.trainingData == null) 
            throw new Exception("Trainingset is null. Set dataset first")
        else {
	        this.metadata.update(0, names.split(treeBuilder.delimiter).map(v => Utility.normalizeString(v)).mkString(","))
	        updateFeatureSet()
        }
    }

    /**
     * Update the feature set based on the information of metadata
     */
    private def updateFeatureSet() = {
        	featureSet = new FeatureSet(this.metadata)
            yIndexDefault = featureSet.numberOfFeature - 1
            //treeBuilder = new ThreadTreeBuilder(featureSet);
            treeBuilder = treeBuilder.createNewInstance(featureSet)
    }
    /* END REGION DATASET AND METADATA */
}