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

class RegressionTree(metadata: Array[String] = Array[String]()) extends Serializable {

  var featureSet = new FeatureSet(metadata)
  
  // set default tree builder is threadTreeBuilder
  var treeBuilder : TreeBuilder = new ThreadTreeBuilder(featureSet);
  
  //var tree : Node = new Empty("Nil")
  var treeModel : TreeModel = new TreeModel()
  
  // Default index of Y feature
  var yIndexDefault = featureSet.numberOfFeature - 1	// = number_of_feature - 1
    
  def buildTree(trainingData: RDD[String], 
      yFeature: String = featureSet.data(yIndexDefault).Name, 
      xFeatures: Set[String] = Set[String]()): TreeModel ={
    
    treeModel = treeBuilder.buildTree(trainingData, yFeature, xFeatures);
    treeModel
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
    def evaluate(input: RDD[String], delimiter : Char = ',') {
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
}