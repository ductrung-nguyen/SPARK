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
    //private var metadata: Array[String] = Array[String]()
    private var headerOfDataset = Array[String]()
    
    /**
     * Contains information of feature
     */
    var featureSet = new FeatureSet()
    var usefulFeatureSet = new FeatureSet()

    /**
     * The main component to build the tree
     * Default is an instance of ThreadTreeBuilder, which will launch each job in
     * a thread to expand a node in tree
     */ 
    var treeBuilder: TreeBuilder =  
        new DataMarkerTreeBuilder(featureSet, usefulFeatureSet)
    	//new ThreadTreeBuilder(featureSet);

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

    private def getXIndexesAndYIndexByNames(xNames : Set[Any], yName : String) : (Set[Int], Int) = {
        
        var yindex = featureSet.getIndex(yName)
        if (yName == "" && yindex < 0)
            yindex = this.yIndexDefault
            
        if (yindex < 0)
            throw new Exception("ERROR: Can not find attribute `" + yName + "` in (" + featureSet.data.map(f => f.Name).mkString(",") + ")")
        // index of features, which will be used to predict the target feature
        var xindexes =
            if (xNames.isEmpty) // if user didn't specify xFeature, we will process on all feature, exclude Y feature (to check stop criterion)
                featureSet.data.filter(_.index != yindex).map(x => x.index).toSet[Int]
            else //xNames.map(x => featureSet.getIndex(x)) //+ yindex
            {
                xNames.map(x => {
                    var index = x match {
                        case Feature(name, ftype, _) => {
                            val idx = featureSet.getIndex(name)
                            featureSet.update(Feature(name, ftype, idx), idx)
                            idx
                        }
                        case s: String => {
                            featureSet.getIndex(s)
                        }
                        case _ => { throw new Exception("Invalid feature. Expect as.String(feature_name) or as.Number(feature_name) or \"feature_name\"") }
                    }
                    if (index < 0)
                        throw new Exception("Could not find feature " + x)
                    else
                        index
                })
            }
                
        (xindexes, yindex)
        
    }
    
    def filterUnusedFeatures(trainingData : RDD[String], xIndexes:Set[Int], yIndex : Int, removeInvalidRecord : Boolean = true) : RDD[String] = {
        var i = 0
        var j = 0
        var temp = trainingData.map(line => {
            var array = line.split(this.treeBuilder.delimiter)

            i = 0
            j = 0
            var newLine = ""
            try {
                array.foreach(element => {
                    if (yIndex == i || xIndexes.contains(i)) {
                        if (newLine.equals(""))
                            newLine = element
                        else {
                            newLine = "%s,%s".format(newLine, element)
                        }
                        if (removeInvalidRecord){
	                        this.usefulFeatureSet.data(j).Type match {
	                            case FeatureType.Categorical => element
	                            case FeatureType.Numerical => element.toDouble
	                        }
                        }

                        j = j + 1
                    }
                    i = i + 1
                })
                newLine
            } catch {
                case _: Throwable => ""
            }
        })
        
        temp.filter(line => !line.equals(""))
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

        treeModel = treeBuilder.buildTree(trainingData.cache, xIndexes, yIndex);
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
        xFeatures: Set[Any] = Set[Any]()) : TreeModel = {
        if (this.trainingData == null) {
            throw new Exception("ERROR: Dataset can not be null.Set dataset first")
        }
        if (yIndexDefault < 0) {
            throw new Exception("ERROR:Dataset is invalid or invalid feature names")
        }
        
        
        // These information will be used in Pruning phase
        treeBuilder.treeModel.xFeatures = xFeatures
        treeBuilder.treeModel.yFeature = yFeature
        
        var (xIndexes, yIndex) = this.getXIndexesAndYIndexByNames(xFeatures, yFeature)
        
        /* SET UP LIST OF USEFUL FEATURES AND ITS INDEXES */
        var usefulFeatureList = List[Feature]()
        var i = -1
        var usefulIndexes = List[Int]()
        var newYIndex = 0
        featureSet.data.foreach(feature => {
            if (xIndexes.contains(feature.index) || feature.index == yIndex){
                i = i + 1
                if (feature.index == yIndex)
                    newYIndex = i
                usefulIndexes = usefulIndexes.:+(i) 
                usefulFeatureList = usefulFeatureList.:+(Feature(feature.Name, feature.Type, i))
            }
        })
        
        this.usefulFeatureSet = new FeatureSet(usefulFeatureList)
        this.treeBuilder.usefulFeatureSet = new FeatureSet(usefulFeatureList)
        
        /* FILTER OUT THE UNUSED FEATURES */
        this.trainingData = filterUnusedFeatures(this.trainingData, xIndexes, yIndex)
        
        
        
        var newXIndexes = usefulIndexes.filter(x => x != newYIndex)
        
        this.treeBuilder.usefulFeatureSet = new FeatureSet(usefulFeatureList)
        
        // build tree
        this.buildTree(this.trainingData, newXIndexes.toSet, newYIndex)
    }

    /**
     * Predict value of the target feature base on the values of input features
     * 
     * @param record	an array, which its each element is a value of each input feature (already remove unused features)
     * @return predicted value or '???' if input record is invalid
     */
    private def predictOnPreciseData(record: Array[String], ignoreBranchIDs : Set[BigInt]): String = {
        try{
            treeModel.predict(record, ignoreBranchIDs)
        }
        catch{
            case e: Exception => "???"
        }
    }
    
    def predictOneInstance(record : Array[String], ignoreBranchIDs : Set[BigInt] = Set[BigInt]()) : String = {
        if (record.length == 0)
            "???"
        else {
            var (xIndexes, yIndex) = mapFromUsefulIndexToOriginalIndex(featureSet, usefulFeatureSet)
            var newRecord : Array[String] = Array[String]()
            var i = 0
            for (field <- record) {
                if (i == yIndex || xIndexes.contains(i)) {
                    newRecord = newRecord.:+(field)
                }
                i = i + 1
            }
            
            predictOnPreciseData(newRecord, ignoreBranchIDs)
        }
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
        var (xIndexes, yIndex) = mapFromUsefulIndexToOriginalIndex(featureSet, usefulFeatureSet)
        var newTestingData = filterUnusedFeatures(testingData, xIndexes, yIndex, false)
        newTestingData.map(line => this.predictOnPreciseData(line.split(delimiter), ignoreBranchIDs))
        //testingData.map(line => this.predict(line.split(delimiter))) 
    }


    /**
     * Write the current tree model to file
     * 
     * @param path where we want to write to
     */
    def writeModelToFile(path: String) = {
        val ois = new ObjectOutputStream(new FileOutputStream(path))
        ois.writeObject(treeModel)
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

        var rt = ois.readObject().asInstanceOf[TreeModel]
        //treeModel = rt
        //this.featureSet = treeModel.featureSet
        //this.usefulFeatureSet = treeModel.usefulFeatureSet
        setTreeModel(rt)
        
        ois.close()
    }
    
    def setTreeModel(tm : TreeModel) = {
        this.treeModel = tm
        this.featureSet = tm.featureSet
        this.usefulFeatureSet = tm.usefulFeatureSet
        updateFeatureSet()
    }
    
    private def mapFromUsefulIndexToOriginalIndex(featureSet : FeatureSet , usefulFeatureSet : FeatureSet) : (Set[Int], Int) = {
        var xIndexes = treeModel.xIndexes.map(index => treeModel.featureSet.getIndex(treeModel.usefulFeatureSet.data(index).Name))
        var yIndex = treeModel.featureSet.getIndex(usefulFeatureSet.data(treeModel.yIndex).Name)
        (xIndexes, yIndex)
    }
    
    /**
     * Recover, repair and continue build tree from the last state
     */
    def continueFromIncompleteModel(trainingData: RDD[String], path_to_model : String) : TreeModel = {
        loadModelFromFile(path_to_model)
        treeBuilder.treeModel = treeModel
        this.featureSet = treeModel.featureSet
        this.usefulFeatureSet = treeModel.usefulFeatureSet
        var (xIndexes, yIndex) = mapFromUsefulIndexToOriginalIndex(featureSet, usefulFeatureSet)
        var newtrainingData = filterUnusedFeatures(trainingData, xIndexes, yIndex)
        treeBuilder.continueFromIncompleteModel(newtrainingData)
        treeModel
    }

    /*  REGION: SET DATASET AND METADATA */

    /**
     * Set the training dataset to used for building tree
     *
     * @param trainingData	the training dataset
     * @throw Exception if the dataset contains less than 2 lines
     */
    def setDataset(trainingData: RDD[String], hasHeader: Boolean = true) {
        
        var twoFirstLines = trainingData.take(2)
        
        // If we can not get 2 first line of dataset, it's invalid dataset
        if (twoFirstLines.length < 2) {
            throw new Exception("ERROR:Invalid dataset")
        } else {
            this.trainingData = trainingData;
            
            var header = twoFirstLines(0)
            var temp_header = header.split(treeBuilder.delimiter)

            if (hasHeader) {
                this.headerOfDataset = temp_header
            } else {
                var i = -1;
                this.headerOfDataset = temp_header.map(v => { i = i + 1; "Column" + i })
            }
            
            // determine types of features automatically
            // Get the second line of dataset and try to parse each of them to double
            // If we can parse, it's a numerical feature, otherwise, it's a categorical feature
            var secondLine = twoFirstLines(1).split(treeBuilder.delimiter);
            
            var i = 0
            var listOfFeatures = List[Feature]()
            
            // if we can parse value of a feature into double, this feature may be a numerical feature
            secondLine.foreach(v => {
                Utility.parseDouble(v.trim()) match {
                    case Some(d) => { listOfFeatures = listOfFeatures.:+(Feature(headerOfDataset(i), FeatureType.Numerical, i)) }
                    case None => { listOfFeatures = listOfFeatures.:+(Feature(headerOfDataset(i), FeatureType.Categorical, i)) }
                }
                i = i + 1
            })
            
            // update the dataset
            featureSet = new FeatureSet(listOfFeatures)
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
    def setFeatureNames(names: Array[String]) = {
        if (this.trainingData == null) 
            throw new Exception("Trainingset is null. Set dataset first")
        else {
            if (names.length != featureSet.data.length){
                throw new Exception("Incorrect names")
            }
            
	        var i = 0
	        names.foreach(name =>{
	           featureSet.data(i).Name = name
	           featureSet.update(featureSet.data(i), i)
	            i = i + 1
	        } )
	        updateFeatureSet()
        }
    }

    /**
     * Update the feature set based on the information of metadata
     */
    private def updateFeatureSet() = {

            yIndexDefault = featureSet.numberOfFeature - 1
            //treeBuilder = new ThreadTreeBuilder(featureSet);
            treeBuilder = treeBuilder.createNewInstance(featureSet, usefulFeatureSet)
    }
    /* END REGION DATASET AND METADATA */
}