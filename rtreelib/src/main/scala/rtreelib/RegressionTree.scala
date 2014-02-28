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
import scala.util.Marshal
import scala.io.Source
import java.io._
import scala.actors.remote.JavaSerializer
import java.io.DataOutputStream
import java.io.FileOutputStream
import java.io.DataInputStream
import java.io.FileInputStream

class RegressionTree() extends Serializable {

    var metadata: Array[String] = Array[String]()
    
    var featureSet = new FeatureSet(metadata)

    // set default tree builder is threadTreeBuilder
    var treeBuilder: TreeBuilder = new ThreadTreeBuilder(featureSet);

    //var tree : Node = new Empty("Nil")
    var treeModel: TreeModel = new TreeModel()

    // Default index of Y feature
    var yIndexDefault = featureSet.numberOfFeature - 1 // = number_of_feature - 1

    var trainingData: RDD[String] = null;

    private def buildTree(trainingData: RDD[String],
        yFeature: String,
        xFeatures: Set[String]): TreeModel = {

        treeModel = treeBuilder.buildTree(trainingData, yFeature, xFeatures);
        treeModel
    }
    
    def buildTree(yFeature: String = "",
        xFeatures: Set[String] = Set[String]()) : TreeModel = {
        if (this.trainingData == null) {
            throw new Exception("ERROR: Dataset can not be null.Set dataset first")
        }
        if (yIndexDefault < 0) {
            throw new Exception("ERROR:Dataset is invalid or invalid column name")
        }

        if (yFeature == "")
            this.buildTree(this.trainingData, featureSet.data(yIndexDefault).Name, xFeatures)
        else
            this.buildTree(this.trainingData, yFeature, xFeatures)
    }

    /**
     * Predict Y base on input features
     * @record: an array, which its each element is a value of each input feature
     */
    def predict(record: Array[String]): String = {
        treeModel.predict(record)
    }

    /**
     * Evaluate the accuracy of regression tree
     * @input: an input record (uses the same delimiter with trained data set)
     */
    def evaluate(input: RDD[String], delimiter: Char = ',') {
        treeModel.evaluate(input, delimiter);
    }

    def writeModelToFile(path: String) = {

        val js = new JavaSerializer(null, null)
        val os = new DataOutputStream(new FileOutputStream(path))

        js.writeObject(os, treeModel)
        os.close
    }

    def loadModelFromFile(path: String) = {
        val js = new JavaSerializer(null, null)
        val is = new DataInputStream(new FileInputStream(path))
        val rt = js.readObject(is).asInstanceOf[TreeModel]
        treeModel = rt
    }
    
    
    /*  REGION: SET DATASET AND METADATA */
    
    def setDataset(trainingData: RDD[String]) {
        this.trainingData = trainingData;
        var headers = this.trainingData.take(2)
        
        if (headers.length < 2) {
            throw new Exception("ERROR:Invalid dataset")
        } else {
            this.metadata = Array[String]()
            
            var temp_header = headers(0).split(treeBuilder.delimiter)
            if (temp_header.exists(v => {
                Utility.parseDouble(v.trim()) match {
                    case Some(d) => true
                    case None => false
                }
            })) {
                var i = -1;
                this.metadata = this.metadata :+ (temp_header.map(v => { i = i +1; "Column" + i } ).mkString(","))
            } else {
                this.metadata = this.metadata :+ (temp_header.mkString(","))
            }
            
            var temp_types = headers(1).split(treeBuilder.delimiter);
            this.metadata = this.metadata :+ (temp_types.map(v => {
                Utility.parseDouble(v.trim()) match {
                    case Some(d) => "c"
                    case None => "d"
                }
            }).mkString(","))
            
            updateFeatureSet()
            
        }
    }
    
    def setMetadata(mdata : Array[String]) = {
        this.metadata = mdata;
        updateFeatureSet()
    }
    
    def setAttributeTypes(types : String) = {
        this.metadata.update(1, types.split(treeBuilder.delimiter).map(v => v.trim()).mkString(","))
        updateFeatureSet()
    }

    def setAttributeNames(names: String) = {
        this.metadata.update(0, names.split(treeBuilder.delimiter).map(v => Utility.normalizeString(v)).mkString(","))
        updateFeatureSet()
    }

    private def updateFeatureSet() = {
        	featureSet = new FeatureSet(this.metadata)
            yIndexDefault = featureSet.numberOfFeature - 1
            treeBuilder = new ThreadTreeBuilder(featureSet);
    }
    /* END REGION DATASET AND METADATA */
}